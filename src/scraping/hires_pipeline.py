from __future__ import annotations

"""
Pipeline de OCR em alta resolução.

Baixa imagens hi-res do DocReader da BN, faz OCR e deleta a imagem.
Usa apenas espaço temporário (~4MB por imagem), sem acumular no disco.

Melhorias desta versão:
- retry por página
- persistência de páginas falhadas em CACHE_DIR/hires_progress.json
- só marca acervo como done se não houver falhas pendentes
- páginas já salvas em .txt com metadata hires=True são puladas automaticamente
"""

import json
import logging
import re
import ssl
import tempfile
import time
import urllib.request
from pathlib import Path
from threading import Lock

import tesserocr

from src.config import (
    HDB_DOCREADER_URL,
    TEXT_DIR,
    IMAGES_DIR,
    CACHE_DIR,
    TESSDATA_DIR,
    TESSERACT_LANG,
    CLICK_PAUSE,
)
from src.scraping.hires_progress import (
    default_progress as _default_progress_impl,
    load_progress as _load_progress_impl,
    mark_done as _mark_done_impl,
    save_progress as _save_progress_impl,
    set_bib_stats as _set_bib_stats_impl,
    update_failed_page as _update_failed_page_impl,
)
from src.scraping.hires_docreader import (
    captcha_visivel as _captcha_visivel_impl,
    fechar_dialog as _fechar_dialog_impl,
    get_cookie_str as _get_cookie_str_impl,
    get_page_metadata as _get_page_metadata_impl,
    proxima_pagina as _proxima_pagina_impl,
    resolver_captcha as _resolver_captcha_impl,
    setup_acervo as _setup_acervo_impl,
    wait_for_cache_url as _wait_for_cache_url_impl,
)
from src.scraping.hires_orchestrator import (
    get_total_pages as _get_total_pages_impl,
    processar_acervo_paralelo as _processar_acervo_paralelo_impl,
    processar_todos_hires as _processar_todos_hires_impl,
)

logger = logging.getLogger(__name__)

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

HIRES_SIZE = "6464x8940"

PROGRESS_FILE = CACHE_DIR / "hires_progress.json"
_PROGRESS_LOCK = Lock()

# Retry por página
PAGE_RETRIES = 3
PAGE_RETRY_SLEEP = 2.0

# Retry no setup inicial do acervo
ACERVO_SETUP_RETRIES = 3
ACERVO_SETUP_SLEEP = 3.0


# ── OCR ──────────────────────────────────────────────────────────────

def _detectar_n_colunas(img) -> int:
    """Detecta número aproximado de colunas via projeção vertical."""
    import numpy as np

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
    mean_val = search.mean()
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
        n_cols = 1
    elif n_cols > 6:
        n_cols = 4

    return n_cols


def _ocr_hires(img_path: Path) -> str:
    """OCR em imagem hi-res com segmentação automática de colunas."""
    from PIL import Image, ImageEnhance, ImageFilter

    img = Image.open(img_path).convert("L")

    img = img.filter(ImageFilter.MedianFilter(3))
    img = ImageEnhance.Contrast(img).enhance(1.8)
    img = ImageEnhance.Sharpness(img).enhance(2.0)
    img = img.point(lambda p: 255 if p > 130 else 0)

    w, h = img.size
    margin = int(w * 0.02)
    header_end = int(h * 0.18)
    footer_start = int(h * 0.96)

    n_cols = _detectar_n_colunas(img)
    textos = []

    if header_end > 50:
        header = img.crop((margin, 0, w - margin, header_end))
        t = tesserocr.image_to_text(
            header,
            lang=TESSERACT_LANG,
            path=str(TESSDATA_DIR),
            psm=6,
        )
        if t.strip():
            textos.append(t.strip())

    col_w = (w - 2 * margin) // n_cols
    for i in range(n_cols):
        x1 = margin + i * col_w
        x2 = margin + (i + 1) * col_w
        col = img.crop((x1, header_end, x2, footer_start))
        t = tesserocr.image_to_text(
            col,
            lang=TESSERACT_LANG,
            path=str(TESSDATA_DIR),
            psm=6,
        )
        if t.strip():
            textos.append(t.strip())

    texto = "\n\n".join(textos)

    lines = texto.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if len(re.findall(r"[a-zA-ZÀ-ú0-9]", stripped)) >= 2:
            cleaned.append(stripped)

    texto = "\n".join(cleaned)
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    texto = re.sub(r" {3,}", " ", texto)

    return texto.strip()


# ── Estado persistente ───────────────────────────────────────────────

def _default_progress() -> dict:
    return _default_progress_impl()


def _load_progress() -> dict:
    return _load_progress_impl(PROGRESS_FILE)


def _save_progress(data: dict) -> None:
    _save_progress_impl(PROGRESS_FILE, data)


def _mark_failed_page(bib: str, page_num: int) -> None:
    _update_failed_page_impl(PROGRESS_FILE, _PROGRESS_LOCK, bib, page_num, failed=True)


def _clear_failed_page(bib: str, page_num: int) -> None:
    _update_failed_page_impl(PROGRESS_FILE, _PROGRESS_LOCK, bib, page_num, failed=False)


def _set_bib_stats(bib: str, stats: dict) -> None:
    _set_bib_stats_impl(PROGRESS_FILE, _PROGRESS_LOCK, bib, stats)


def _mark_done(bib: str) -> None:
    _mark_done_impl(PROGRESS_FILE, _PROGRESS_LOCK, bib, done=True)


def _unmark_done(bib: str) -> None:
    _mark_done_impl(PROGRESS_FILE, _PROGRESS_LOCK, bib, done=False)


# ── Selenium helpers ─────────────────────────────────────────────────

def _resolver_captcha(driver, max_tentativas=5, timeout_manual=120):
    return _resolver_captcha_impl(
        driver,
        _captcha_visivel,
        max_tentativas=max_tentativas,
        timeout_manual=timeout_manual,
    )


def _captcha_visivel(driver) -> bool:
    return _captcha_visivel_impl(driver)


def _fechar_dialog(driver):
    _fechar_dialog_impl(driver)


def _wait_for_cache_url(driver, old_src=None, timeout=20):
    return _wait_for_cache_url_impl(driver, old_src=old_src, timeout=timeout)


def _proxima_pagina(driver):
    _proxima_pagina_impl(driver, CLICK_PAUSE)


def _get_cookie_str(driver):
    return _get_cookie_str_impl(driver)


def _get_page_metadata(driver, bib: str, nome: str, global_page: int) -> dict:
    return _get_page_metadata_impl(driver, bib, nome, global_page)


# ── Processamento do acervo ──────────────────────────────────────────

def processar_acervo_hires(
    bib: str,
    nome: str,
    headless: bool = True,
    force: bool = False,
    max_pages: int = 0,
    keep_images: bool = False,
) -> int:
    from src.scraping.driver import create_driver

    txt_dir = TEXT_DIR / bib
    txt_dir.mkdir(parents=True, exist_ok=True)

    driver = create_driver(headless=headless)
    try:
        result = _processar_com_driver(
            driver,
            bib,
            nome,
            txt_dir,
            force=force,
            max_pages=max_pages,
            keep_images=keep_images,
        )
        return result["processed"]
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _setup_acervo(driver, bib: str, nome: str, start_page: int = 1) -> tuple[str | None, str | None]:
    return _setup_acervo_impl(
        driver,
        bib=bib,
        nome=nome,
        hires_size=HIRES_SIZE,
        hdb_docreader_url=HDB_DOCREADER_URL,
        start_page=start_page,
        acervo_setup_retries=ACERVO_SETUP_RETRIES,
        acervo_setup_sleep=ACERVO_SETUP_SLEEP,
    )


def _page_already_done(txt_path: Path, meta_path: Path, force: bool) -> bool:
    if not txt_path.exists() or force:
        return False

    if not meta_path.exists():
        return False

    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return bool(meta.get("hires"))
    except Exception:
        return False


def _process_page_with_retry(
    *,
    driver,
    bib: str,
    global_page: int,
    txt_path: Path,
    meta_path: Path,
    img_dir: Path | None,
    cookie_str: str,
    ua: str,
    referer: str,
    prev_src: str | None,
    page_metadata: dict | None,
) -> tuple[bool, str | None, str]:
    """
    Retorna:
      (sucesso, novo_prev_src, motivo)
    """
    for attempt in range(1, PAGE_RETRIES + 1):
        tmp_path = None
        try:
            driver.execute_script(
                f"document.getElementById('HiddenSize').value = '{HIRES_SIZE}';"
            )

            src = _wait_for_cache_url(driver, old_src=prev_src, timeout=15)
            if not src:
                raise RuntimeError("cache_url_nao_carregou")

            req = urllib.request.Request(src)
            req.add_header("Cookie", cookie_str)
            req.add_header("User-Agent", ua)
            req.add_header("Referer", referer)
            req.add_header("Accept", "image/*")

            with urllib.request.urlopen(req, context=_SSL_CTX, timeout=30) as resp:
                data = resp.read()

            if len(data) < 1000:
                raise RuntimeError("imagem_muito_pequena")

            tmp_path = Path(tempfile.mktemp(suffix=".jpg"))
            tmp_path.write_bytes(data)

            if img_dir:
                img_persistent = img_dir / f"{bib}_{global_page:05d}.jpg"
                img_persistent.write_bytes(data)

            texto = _ocr_hires(tmp_path)
            if not texto:
                raise RuntimeError("ocr_vazio")

            txt_path.write_text(texto, encoding="utf-8")
            metadata = {
                "bib": bib,
                "imagem": f"{bib}_{global_page:05d}.jpg",
                "arquivo_texto": txt_path.name,
                "caracteres": len(texto),
                "palavras": len(texto.split()),
                "hires": True,
            }
            if page_metadata:
                metadata.update({k: v for k, v in page_metadata.items() if v not in (None, "")})
            meta_path.write_text(
                json.dumps(metadata, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return True, src, "ok"

        except Exception as e:
            reason = str(e)
            logger.warning(
                f"{bib} p{global_page}: falha na tentativa {attempt}/{PAGE_RETRIES}: {reason}"
            )
            if attempt < PAGE_RETRIES:
                time.sleep(PAGE_RETRY_SLEEP)
        finally:
            try:
                if tmp_path:
                    tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    return False, prev_src, reason


def _processar_com_driver(
    driver,
    bib,
    nome,
    txt_dir,
    force,
    max_pages: int = 0,
    keep_images: bool = False,
    start_page: int = 1,
    end_page: int = 0,
) -> dict:
    low_res_src, first_src = _setup_acervo(driver, bib, nome, start_page=start_page)
    if not low_res_src or not first_src:
        _set_bib_stats(
            bib,
            {
                "processed": 0,
                "skipped": 0,
                "failed_pages": ["setup"],
                "complete": False,
            },
        )
        _unmark_done(bib)
        return {
            "processed": 0,
            "skipped": 0,
            "failed_pages": ["setup"],
            "complete": False,
        }

    cookie_str = _get_cookie_str(driver)
    ua = driver.execute_script("return navigator.userAgent")
    referer = driver.current_url

    processadas = 0
    skipped = 0
    global_page = start_page
    stale_count = 0
    max_stale = 30
    prev_src = None
    t0 = time.time()
    failed_pages_run = set()

    # end_page > 0 = worker com range definido (não usa stale_count)
    total_esperado = end_page

    img_dir = IMAGES_DIR / bib if keep_images else None
    if img_dir:
        img_dir.mkdir(parents=True, exist_ok=True)

    while True:
        if max_pages and (processadas + skipped) >= max_pages:
            logger.info(f"Limite de {max_pages} páginas atingido para {bib}")
            break

        txt_path = txt_dir / f"{bib}_{global_page:05d}.txt"
        meta_path = txt_dir / f"{bib}_{global_page:05d}.json"

        if _page_already_done(txt_path, meta_path, force):
            skipped += 1
            stale_count = 0  # página existente = acervo não acabou
            _clear_failed_page(bib, global_page)
            _proxima_pagina(driver)
            global_page += 1
            prev_src = None
            continue

        success, new_prev_src, reason = _process_page_with_retry(
            driver=driver,
            bib=bib,
            global_page=global_page,
            txt_path=txt_path,
            meta_path=meta_path,
            img_dir=img_dir,
            cookie_str=cookie_str,
            ua=ua,
            referer=referer,
            prev_src=prev_src,
            page_metadata=_get_page_metadata(driver, bib, nome, global_page),
        )

        if success:
            processadas += 1
            prev_src = new_prev_src
            stale_count = 0  # sucesso = acervo não acabou
            _clear_failed_page(bib, global_page)
        else:
            failed_pages_run.add(global_page)
            _mark_failed_page(bib, global_page)
            logger.error(f"{bib} p{global_page}: falhou após {PAGE_RETRIES} tentativas ({reason})")

        if processadas % 50 == 0 and processadas > 0:
            elapsed = time.time() - t0
            rate = processadas / elapsed if elapsed > 0 else 0
            remaining = (end_page - global_page) if end_page else 0
            eta = remaining / rate if rate > 0 and remaining else 0
            logger.info(
                f"Hi-res {bib}: {processadas} feitas "
                f"(p{global_page}/{end_page or '?'}) | "
                f"{rate:.1f} pg/s | ETA {eta/60:.0f}min"
            )
            cookie_str = _get_cookie_str(driver)
            if _captcha_visivel(driver):
                _resolver_captcha(driver)
                cookie_str = _get_cookie_str(driver)

        before_page = global_page
        _proxima_pagina(driver)
        global_page += 1

        if not success and before_page == global_page - 1:
            prev_src = None

        if total_esperado and global_page > total_esperado:
            break

        if not total_esperado:
            # fallback para detectar fim sem total conhecido
            src_probe = _wait_for_cache_url(driver, old_src=prev_src, timeout=5)
            if not src_probe:
                stale_count += 1
                if stale_count >= max_stale:
                    logger.info(f"Fim do acervo {bib} na página {before_page}")
                    break
            else:
                stale_count = 0
                prev_src = src_probe

    elapsed = time.time() - t0

    progress = _load_progress()
    failed_pages_all = sorted(set(progress["failed_pages"].get(bib, [])))

    # completo quando:
    # - não há páginas falhadas pendentes
    # - e houve algum resultado ou havia páginas já puladas
    complete = len(failed_pages_all) == 0 and (processadas > 0 or skipped > 0)

    stats = {
        "processed": processadas,
        "skipped": skipped,
        "failed_pages": failed_pages_all,
        "complete": complete,
        "elapsed_sec": round(elapsed, 1),
        "total_expected": total_esperado,
    }
    _set_bib_stats(bib, stats)

    logger.info(
        f"Hi-res {bib} concluído: {processadas} páginas em {elapsed/60:.1f}min "
        f"(skipped: {skipped}, failed: {len(failed_pages_all)})"
    )

    return stats


# ── Orquestrador ─────────────────────────────────────────────────────

def _get_total_pages(bib: str) -> int:
    return _get_total_pages_impl(bib)


def processar_todos_hires(
    headless: bool = True,
    force: bool = False,
    bib: str = None,
    workers: int = 4,
    max_pages: int = 0,
    keep_images: bool = False,
):
    from src.scraping.driver import create_driver

    return _processar_todos_hires_impl(
        headless=headless,
        force=force,
        bib=bib,
        workers=workers,
        max_pages=max_pages,
        keep_images=keep_images,
        load_progress_fn=_load_progress,
        processar_com_driver_fn=_processar_com_driver,
        processar_acervo_paralelo_fn=_processar_acervo_paralelo_impl,
        mark_done_fn=_mark_done,
        unmark_done_fn=_unmark_done,
        create_driver_fn=create_driver,
    )


def _processar_acervo_paralelo(acervo, workers, headless, force, max_pages, keep_images, done):
    from src.scraping.driver import create_driver
    return _processar_acervo_paralelo_impl(
        acervo,
        workers=workers,
        headless=headless,
        force=force,
        max_pages=max_pages,
        keep_images=keep_images,
        processar_com_driver_fn=_processar_com_driver,
        load_progress_fn=_load_progress,
        mark_done_fn=_mark_done,
        create_driver_fn=create_driver,
    )
