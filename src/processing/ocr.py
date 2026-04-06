"""Pipeline de OCR: converte imagens de jornais em texto usando Tesseract (via tesserocr)."""

from __future__ import annotations

import json
import logging
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

from PIL import Image, ImageEnhance, ImageFilter
import tesserocr

from src.config import IMAGES_DIR, TEXT_DIR, TESSERACT_LANG, TESSERACT_PSM, TESSDATA_DIR
from src.processing.ocr_quality import (
    OCRComparison,
    OCRResult,
    compare_with_existing,
    limpar_texto as _limpar_texto,
    score_ocr_text as _score_ocr_text,
    select_best_ocr_result as _select_best_ocr_result,
)
from src.processing.ocr_storage import (
    build_ocr_task_list,
    comparison_metadata,
    persist_ocr_outputs,
    run_parallel_ocr,
)

# Número de colunas para segmentação (2 colunas equilibra ganho em multi-coluna
# com perda mínima em coluna única — testado empiricamente)
N_COLUNAS = 2
OCR_VARIANTS = (
    {"name": "balanced_psm6", "threshold": 130, "contrast": 2.0, "sharpness": 2.5, "psm": 6, "n_cols": "auto"},
    {"name": "lighter_psm6", "threshold": 145, "contrast": 2.2, "sharpness": 2.4, "psm": 6, "n_cols": "auto"},
    {"name": "darker_psm4", "threshold": 118, "contrast": 1.8, "sharpness": 2.1, "psm": 4, "n_cols": "auto"},
    {"name": "singlecol_psm4", "threshold": 130, "contrast": 1.9, "sharpness": 2.2, "psm": 4, "n_cols": 1},
)

logger = logging.getLogger(__name__)
Image.MAX_IMAGE_PIXELS = None


def preprocessar_imagem(
    img_path: Path,
    *,
    threshold: int = 130,
    contrast: float = 2.0,
    sharpness: float = 2.5,
    upscale: int | None = None,
) -> Image.Image:
    """
    Pré-processamento da imagem para melhorar OCR em jornais históricos.

    - Upscale 4x com Lanczos (imagens da HDB são ~491x676, muito pequenas para OCR)
    - Converte para escala de cinza
    - Denoise com filtro mediana
    - Aumenta contraste e nitidez
    - Aplica binarização
    """
    img = Image.open(img_path)

    # Converter para escala de cinza primeiro (1 canal = 3x menos dados no resize)
    img = img.convert("L")

    # Upscale adaptativo: páginas hi-res não devem ser ampliadas novamente.
    if upscale is None:
        longest_edge = max(img.width, img.height)
        if longest_edge >= 5000:
            upscale = 1
        elif longest_edge >= 2500:
            upscale = 2
        else:
            upscale = 4

    if upscale > 1:
        img = img.resize((img.width * upscale, img.height * upscale), Image.LANCZOS)

    # Denoise com filtro mediana (remove ruído de papel envelhecido sem borrar texto)
    img = img.filter(ImageFilter.MedianFilter(3))

    # Aumentar contraste (2.0 melhora separação texto/fundo em jornais multi-coluna)
    img = ImageEnhance.Contrast(img).enhance(contrast)

    # Aumentar nitidez (2.5 recupera bordas de caracteres pós-denoise)
    img = ImageEnhance.Sharpness(img).enhance(sharpness)

    # Binarização (limiar 130 melhor para papel amarelado de jornais históricos)
    img = img.point(lambda p: 255 if p > threshold else 0)

    return img


def _detectar_n_colunas(img: Image.Image) -> int:
    """Detecta número aproximado de colunas via projeção vertical."""
    try:
        import numpy as np
    except Exception:
        return N_COLUNAS

    w, h = img.size
    arr = np.array(img)

    header_end = int(h * 0.20)
    footer_start = int(h * 0.95)
    body = arr[header_end:footer_start, :]

    proj = (body < 128).sum(axis=0).astype(float)
    kernel = max(15, w // 100)
    smoothed = np.convolve(proj, np.ones(kernel) / kernel, mode="same")

    margin = int(w * 0.05)
    search = smoothed[margin:w - margin]
    mean_val = search.mean() if len(search) else 0
    threshold = mean_val * 0.25

    gaps = []
    in_gap = False
    gap_start = 0
    for x in range(len(search)):
        if search[x] < threshold:
            if not in_gap:
                gap_start = x
                in_gap = True
        else:
            if in_gap:
                gap_w = x - gap_start
                if gap_w > w * 0.005:
                    gaps.append(margin + (gap_start + x) // 2)
                in_gap = False

    n_cols = len(gaps) + 1
    if n_cols < 1:
        return 1
    if n_cols > 4:
        return 4
    return n_cols


def _ocr_segmentado(img: Image.Image, *, psm: int = 6, n_colunas: int | str = "auto") -> str:
    """
    OCR com segmentação em colunas para jornais multi-coluna.

    Divide a imagem em cabeçalho + N colunas verticais e processa cada
    segmento separadamente com PSM 6 (bloco uniforme). Isso melhora
    drasticamente o OCR de jornais multi-coluna (+64% a +1854% em testes)
    com perda mínima em jornais de coluna única (~3%).
    """
    w, h = img.size
    margin = int(w * 0.03)
    header_end = int(h * 0.18)
    footer_start = int(h * 0.95)

    textos = []
    cols = _detectar_n_colunas(img) if n_colunas == "auto" else int(n_colunas)

    # Cabeçalho (título do jornal, data, etc.)
    header = img.crop((margin, 0, w - margin, header_end))
    textos.append(tesserocr.image_to_text(
        header, lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=psm,
    ))

    # Colunas do corpo
    col_w = (w - 2 * margin) // cols
    for i in range(cols):
        x1 = margin + i * col_w
        x2 = margin + (i + 1) * col_w if i < cols - 1 else w - margin
        col = img.crop((x1, header_end, x2, footer_start))
        textos.append(tesserocr.image_to_text(
            col, lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=psm,
        ))

    return "\n\n".join(textos)


def extrair_texto(img_path: Path, preprocessar: bool = True) -> str:
    """
    Extrai texto de uma imagem usando Tesseract OCR (via tesserocr).

    Usa segmentação em colunas para melhorar OCR de jornais multi-coluna.

    Args:
        img_path: caminho da imagem
        preprocessar: se deve aplicar pré-processamento

    Returns:
        texto extraído
    """
    result = extrair_texto_com_qualidade(img_path, preprocessar=preprocessar)
    return result.text
def _result_from_text(text: str, variant: str = "saved") -> OCRResult:
    metrics = _score_ocr_text(text)
    return OCRResult(text=text, score=metrics["score"], variant=variant, metrics=metrics)


def extrair_texto_com_qualidade(img_path: Path, preprocessar: bool = True) -> OCRResult:
    """Executa OCR adaptativo em múltiplas variantes e escolhe a melhor."""
    try:
        if not preprocessar:
            img = Image.open(img_path)
            texto = _limpar_texto(_ocr_segmentado(img, psm=6, n_colunas="auto"))
            metrics = _score_ocr_text(texto)
            return OCRResult(text=texto, score=metrics["score"], variant="raw_psm6", metrics=metrics)

        candidates: list[OCRResult] = []
        for variant in OCR_VARIANTS:
            img = preprocessar_imagem(
                img_path,
                threshold=variant["threshold"],
                contrast=variant["contrast"],
                sharpness=variant["sharpness"],
            )
            texto = _limpar_texto(
                _ocr_segmentado(
                    img,
                    psm=variant["psm"],
                    n_colunas=variant["n_cols"],
                )
            )
            metrics = _score_ocr_text(texto)
            candidates.append(
                OCRResult(
                    text=texto,
                    score=metrics["score"],
                    variant=variant["name"],
                    metrics=metrics,
                )
            )
        return _select_best_ocr_result(candidates)
    except Exception as e:
        logger.error(f"Erro OCR em {img_path.name}: {e}")
        return OCRResult(text="", score=0.0, variant="error", metrics=_score_ocr_text(""))


def extrair_texto_vs_existente(
    img_path: Path,
    existing_text: str | None,
    preprocessar: bool = True,
) -> OCRComparison:
    new_result = extrair_texto_com_qualidade(img_path, preprocessar=preprocessar)
    if not existing_text or not existing_text.strip():
        return OCRComparison(
            selected_text=new_result.text,
            selected_source="new",
            selected_variant=new_result.variant,
            new_result=new_result,
            existing_result=None,
            reason="no_existing_ocr",
        )
    return compare_with_existing(existing_text, new_result)


def _ocr_single_image(args: tuple) -> tuple:
    """Worker function para multiprocessing. Processa uma única imagem.

    Args:
        args: (img_path_str, txt_path_str, meta_path_str, bib)

    Returns:
        (img_name, success, chars)
    """
    img_path_str, txt_path_str, meta_path_str, bib = args
    img_path = Path(img_path_str)
    txt_path = Path(txt_path_str)
    meta_path = Path(meta_path_str)

    existing_text = txt_path.read_text(encoding="utf-8") if txt_path.exists() else ""

    try:
        comparison = extrair_texto_vs_existente(img_path, existing_text, preprocessar=True)
        texto = comparison.selected_text

        if texto:
            metadata = comparison_metadata(
                comparison,
                bib=bib,
                image_name=img_path.name,
                text_name=txt_path.name,
                texto=texto,
            )
            persist_ocr_outputs(txt_path, meta_path, metadata, texto, comparison)
            return (img_path.name, True, len(texto))
        else:
            return (img_path.name, False, 0)
    except Exception as e:
        return (img_path.name, False, 0)


def processar_acervo(bib: str, force: bool = False) -> int:
    """
    Processa todas as imagens de um acervo, gerando arquivos de texto.

    Args:
        bib: código do acervo
        force: reprocessar mesmo se já existe texto

    Returns:
        número de páginas processadas
    """
    img_dir = IMAGES_DIR / bib
    txt_dir = TEXT_DIR / bib
    txt_dir.mkdir(parents=True, exist_ok=True)

    if not img_dir.exists():
        logger.warning(f"Diretório de imagens não encontrado: {img_dir}")
        return 0

    imagens = sorted(img_dir.glob("*.jpg")) + sorted(img_dir.glob("*.png"))
    processadas = 0

    for img_path in imagens:
        txt_path = txt_dir / f"{img_path.stem}.txt"
        meta_path = txt_dir / f"{img_path.stem}.json"

        if txt_path.exists() and not force:
            continue

        existing_text = txt_path.read_text(encoding="utf-8") if txt_path.exists() else ""
        comparison = extrair_texto_vs_existente(img_path, existing_text)
        texto = comparison.selected_text

        if texto:
            if comparison.selected_source == "new" or not txt_path.exists():
                txt_path.write_text(texto, encoding="utf-8")

            metadata = comparison_metadata(
                comparison,
                bib=bib,
                image_name=img_path.name,
                text_name=txt_path.name,
                texto=texto,
            )
            persist_ocr_outputs(txt_path, meta_path, metadata, texto, comparison)

            processadas += 1
            logger.debug(f"OCR: {img_path.name} -> {len(texto)} chars")
        else:
            logger.warning(f"OCR vazio para: {img_path.name}")

    logger.info(f"Acervo {bib}: {processadas} páginas processadas por OCR")
    return processadas


def processar_todos_acervos(force: bool = False, workers: int = 0) -> dict:
    """Processa OCR de todos os acervos que têm imagens baixadas.

    Args:
        force: reprocessar mesmo se já existe texto
        workers: número de processos paralelos (0 = cpu_count - 2)
    """
    tasks = build_ocr_task_list(force=force)
    return run_parallel_ocr(tasks, _ocr_single_image, workers=workers)
