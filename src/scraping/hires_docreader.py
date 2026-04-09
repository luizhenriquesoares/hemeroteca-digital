"""Helpers de navegação no DocReader da BN."""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)


def resolver_captcha(driver, captcha_visible_fn, *, max_tentativas=5, timeout_manual=120):
    from src.scraping.scraper import JornalScraper

    scraper = JornalScraper.__new__(JornalScraper)
    scraper.driver = driver
    scraper._captcha_visivel = lambda: captcha_visible_fn(driver)
    return JornalScraper._resolver_captcha(scraper, max_tentativas, timeout_manual)


def captcha_visivel(driver) -> bool:
    try:
        iframes = driver.find_elements("name", "CaptchaWnd")
        if not iframes:
            return False
        parent = iframes[0].find_element("xpath", "./..")
        return parent.is_displayed()
    except Exception:
        return False


def fechar_dialog(driver) -> None:
    try:
        driver.execute_script("var wnd = $find('PesqOpniaoRadWindow'); if (wnd) wnd.close();")
        time.sleep(0.5)
    except Exception:
        pass


def get_cache_url(driver):
    try:
        return driver.execute_script(
            """
            var img = document.getElementById('DocumentoImg');
            return (img && img.src && img.src.indexOf('cache') > -1) ? img.src : null;
            """
        )
    except Exception:
        return None


def refresh_hires_view(driver, *, hires_size: str, page_num: int | None = None) -> None:
    """Reenvia a requisição da imagem hi-res da página atual."""
    try:
        driver.execute_script(
            """
            var size = document.getElementById('HiddenSize');
            if (size) size.value = arguments[0];

            var pagFis = document.getElementById('hPagFis');
            if (pagFis && arguments[1] !== null) pagFis.value = String(arguments[1]);

            var btn = document.getElementById('CarregaImagemHiddenButton');
            if (btn) btn.click();
            """,
            hires_size,
            page_num if page_num is not None else None,
        )
        from src.scraping.scraper_support import aguardar_carregamento

        time.sleep(0.5)
        aguardar_carregamento(driver, timeout=15)
    except Exception as exc:
        logger.debug("Falha ao forçar recarga hi-res: %s", exc)


def wait_for_cache_url(
    driver,
    old_src=None,
    timeout=20,
    *,
    captcha_visible_fn=None,
    captcha_resolve_fn=None,
    refresh_fn=None,
    poll_interval: float = 0.5,
):
    deadline = time.time() + timeout
    refresh_done = False

    while time.time() < deadline:
        if captcha_visible_fn and captcha_visible_fn(driver):
            logger.info("CAPTCHA visível durante espera da imagem; tentando resolver.")
            if captcha_resolve_fn and not captcha_resolve_fn():
                return None
            refresh_done = False

        src = get_cache_url(driver)
        if src and src != old_src:
            return src

        remaining = deadline - time.time()
        if refresh_fn and not refresh_done and remaining <= timeout / 2:
            refresh_fn()
            refresh_done = True

        time.sleep(poll_interval)
    return None


def proxima_pagina(driver, click_pause: float) -> None:
    try:
        driver.execute_script(
            "var btn = document.getElementById('PagPosBtn');"
            "if (btn) btn.click();"
        )
        time.sleep(click_pause)
        from src.scraping.scraper_support import aguardar_carregamento

        aguardar_carregamento(driver, timeout=15)
        time.sleep(0.5)
    except Exception as exc:
        logger.warning("Erro ao avançar página: %s", exc)


def get_cookie_str(driver) -> str:
    cookies = driver.get_cookies()
    return "; ".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies)


def get_page_metadata(driver, bib: str, nome: str, global_page: int) -> dict:
    metadata = {
        "bib": bib,
        "jornal": nome,
        "periodico": nome,
        "pagina": global_page,
    }

    try:
        state = driver.execute_script(
            """
            var pasta = document.getElementById('PastaTxt');
            var pagAtual = document.getElementById('PagAtualTxt');
            var pagFis = document.getElementById('hPagFis');
            return {
                pasta: pasta ? (pasta.innerText || pasta.title || '').trim() : '',
                pagAtual: pagAtual ? parseInt(pagAtual.value) || 1 : 1,
                pagFis: pagFis ? (pagFis.value || '') : '',
            };
            """
        )
    except Exception:
        state = None

    if not state:
        return metadata

    pasta = (state.get("pasta") or "").strip()
    metadata["pagina_logica"] = state.get("pagAtual")
    metadata["pagina_fisica"] = state.get("pagFis") or ""
    metadata["pasta"] = pasta

    if pasta:
        parts = [part.strip() for part in pasta.split("\\") if part.strip()]
        if len(parts) >= 2:
            metadata["ano"] = parts[0]
            metadata["edicao"] = parts[1]
        else:
            metadata["ano"] = parts[0] if parts else ""
            metadata["edicao"] = ""
    else:
        metadata["ano"] = ""
        metadata["edicao"] = ""

    return metadata


def setup_acervo(
    driver,
    *,
    bib: str,
    nome: str,
    hires_size: str,
    hdb_docreader_url: str,
    start_page: int,
    acervo_setup_retries: int,
    acervo_setup_sleep: float,
):
    url = f"{hdb_docreader_url}?bib={bib}"
    logger.info("Hi-res OCR: abrindo %s (%s) a partir da pág %s", nome, bib, start_page)

    low_res_src = None
    for attempt in range(acervo_setup_retries):
        driver.get(url)
        time.sleep(5)

        resolver_captcha(driver, captcha_visivel)
        fechar_dialog(driver)
        time.sleep(2)

        low_res_src = wait_for_cache_url(driver)
        if low_res_src:
            break

        logger.warning(
            "Imagem padrão não carregou para %s (tentativa %s/%s)",
            bib,
            attempt + 1,
            acervo_setup_retries,
        )
        time.sleep(acervo_setup_sleep)

    if not low_res_src:
        logger.error("Imagem padrão não carregou para %s após %s tentativas", bib, acervo_setup_retries)
        return None, None

    logger.info("Low-res carregada: %s", low_res_src.split("/")[-1])

    refresh_hires_view(driver, hires_size=hires_size, page_num=start_page)

    first_src = wait_for_cache_url(
        driver,
        old_src=low_res_src,
        timeout=30,
        captcha_visible_fn=captcha_visivel,
        captcha_resolve_fn=lambda: resolver_captcha(driver, captcha_visivel),
        refresh_fn=lambda: refresh_hires_view(driver, hires_size=hires_size, page_num=start_page),
    )
    if not first_src:
        logger.error("Imagem hi-res não carregou para %s", bib)
        return low_res_src, None

    logger.info("Hi-res OK (pág %s): %s", start_page, first_src.split("/")[-1])
    return low_res_src, first_src
