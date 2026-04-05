"""Pipeline de OCR: converte imagens de jornais em texto usando Tesseract (via tesserocr)."""

import json
import logging
import re
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

from PIL import Image, ImageEnhance, ImageFilter
import tesserocr

from src.config import IMAGES_DIR, TEXT_DIR, TESSERACT_LANG, TESSERACT_PSM, TESSDATA_DIR

# Número de colunas para segmentação (2 colunas equilibra ganho em multi-coluna
# com perda mínima em coluna única — testado empiricamente)
N_COLUNAS = 2

logger = logging.getLogger(__name__)


def preprocessar_imagem(img_path: Path) -> Image.Image:
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

    # Upscale 4x — imagens da HDB são ~491x676 a 96 DPI
    # Tesseract precisa de ~300 DPI para bons resultados
    img = img.resize((img.width * 4, img.height * 4), Image.LANCZOS)

    # Denoise com filtro mediana (remove ruído de papel envelhecido sem borrar texto)
    img = img.filter(ImageFilter.MedianFilter(3))

    # Aumentar contraste (2.0 melhora separação texto/fundo em jornais multi-coluna)
    img = ImageEnhance.Contrast(img).enhance(2.0)

    # Aumentar nitidez (2.5 recupera bordas de caracteres pós-denoise)
    img = ImageEnhance.Sharpness(img).enhance(2.5)

    # Binarização (limiar 130 melhor para papel amarelado de jornais históricos)
    img = img.point(lambda p: 255 if p > 130 else 0)

    return img


def _ocr_segmentado(img: Image.Image) -> str:
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

    # Cabeçalho (título do jornal, data, etc.)
    header = img.crop((margin, 0, w - margin, header_end))
    textos.append(tesserocr.image_to_text(
        header, lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=6,
    ))

    # Colunas do corpo
    col_w = (w - 2 * margin) // N_COLUNAS
    for i in range(N_COLUNAS):
        x1 = margin + i * col_w
        x2 = margin + (i + 1) * col_w
        col = img.crop((x1, header_end, x2, footer_start))
        textos.append(tesserocr.image_to_text(
            col, lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=6,
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
    try:
        if preprocessar:
            img = preprocessar_imagem(img_path)
        else:
            img = Image.open(img_path)

        texto = _ocr_segmentado(img)

        # Limpeza básica
        texto = _limpar_texto(texto)

        return texto

    except Exception as e:
        logger.error(f"Erro OCR em {img_path.name}: {e}")
        return ""


def _limpar_texto(texto: str) -> str:
    """Limpeza básica do texto extraído pelo OCR."""
    # Remover linhas com apenas espaços ou caracteres especiais
    lines = texto.split("\n")
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        # Manter linhas que tenham pelo menos 2 caracteres alfanuméricos
        if len(re.findall(r'[a-zA-ZÀ-ú0-9]', stripped)) >= 2:
            cleaned_lines.append(stripped)

    texto = "\n".join(cleaned_lines)

    # Remover múltiplas quebras de linha consecutivas
    texto = re.sub(r'\n{3,}', '\n\n', texto)

    # Remover espaços múltiplos
    texto = re.sub(r' {3,}', ' ', texto)

    return texto.strip()


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

    try:
        img = preprocessar_imagem(img_path)

        texto = _ocr_segmentado(img)
        texto = _limpar_texto(texto)

        if texto:
            txt_path.write_text(texto, encoding="utf-8")
            metadata = {
                "bib": bib,
                "imagem": img_path.name,
                "arquivo_texto": txt_path.name,
                "caracteres": len(texto),
                "palavras": len(texto.split()),
            }
            meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
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

        texto = extrair_texto(img_path)

        if texto:
            txt_path.write_text(texto, encoding="utf-8")

            # Salvar metadados
            metadata = {
                "bib": bib,
                "imagem": img_path.name,
                "arquivo_texto": txt_path.name,
                "caracteres": len(texto),
                "palavras": len(texto.split()),
            }
            meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

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
    if not IMAGES_DIR.exists():
        logger.warning("Nenhum diretório de imagens encontrado")
        return {}

    if workers <= 0:
        workers = max(2, cpu_count() - 2)

    # Coletar todas as tarefas de OCR
    tasks = []
    for acervo_dir in sorted(IMAGES_DIR.iterdir()):
        if not acervo_dir.is_dir():
            continue
        bib = acervo_dir.name
        txt_dir = TEXT_DIR / bib
        txt_dir.mkdir(parents=True, exist_ok=True)

        imagens = sorted(acervo_dir.glob("*.jpg")) + sorted(acervo_dir.glob("*.png"))
        for img_path in imagens:
            txt_path = txt_dir / f"{img_path.stem}.txt"
            meta_path = txt_dir / f"{img_path.stem}.json"
            if txt_path.exists() and not force:
                continue
            tasks.append((str(img_path), str(txt_path), str(meta_path), bib))

    total = len(tasks)
    if total == 0:
        logger.info("Nenhuma imagem para processar")
        return {}

    logger.info(f"OCR paralelo: {total} imagens com {workers} workers")

    stats = {}
    done = 0
    t0 = time.time()

    with Pool(processes=workers) as pool:
        for img_name, success, chars in pool.imap_unordered(_ocr_single_image, tasks, chunksize=4):
            done += 1
            if success:
                bib = img_name.rsplit("_", 1)[0] if "_" in img_name else "unknown"
                stats[bib] = stats.get(bib, 0) + 1

            if done % 100 == 0:
                elapsed = time.time() - t0
                rate = done / elapsed
                eta = (total - done) / rate if rate > 0 else 0
                logger.info(
                    f"OCR: {done}/{total} ({done*100//total}%) | "
                    f"{rate:.1f} img/s | ETA {eta/60:.0f}min"
                )

    elapsed = time.time() - t0
    total_ok = sum(stats.values())
    logger.info(f"OCR concluído: {total_ok}/{total} em {elapsed/60:.1f}min ({total_ok/elapsed:.1f} img/s)")
    return stats
