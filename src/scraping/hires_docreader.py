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


def wait_for_cache_url(driver, old_src=None, timeout=20):
    for _ in range(timeout):
        src = driver.execute_script(
            """
            var img = document.getElementById('DocumentoImg');
            return (img && img.src && img.src.indexOf('cache') > -1) ? img.src : null;
            """
        )
        if src and src != old_src:
            return src
        time.sleep(1)
    return None


def proxima_pagina(driver, click_pause: float) -> None:
    try:
        driver.execute_script(
            "var btn = document.getElementById('PagPosBtn');"
            "if (btn) btn.click();"
        )
        time.sleep(click_pause)
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

    driver.execute_script(
        f"""
        document.getElementById('HiddenSize').value = '{hires_size}';
        document.getElementById('hPagFis').value = '{start_page}';
        document.getElementById('CarregaImagemHiddenButton').click();
        """
    )

    first_src = wait_for_cache_url(driver, old_src=low_res_src, timeout=30)
    if not first_src:
        logger.error("Imagem hi-res não carregou para %s", bib)
        return low_res_src, None

    logger.info("Hi-res OK (pág %s): %s", start_page, first_src.split("/")[-1])
    return low_res_src, first_src
