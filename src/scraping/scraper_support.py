"""Helpers operacionais do scraper legado."""

from __future__ import annotations

import logging
import ssl
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

logger = logging.getLogger(__name__)

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


def download_image_task(img_src, img_file, cookie_str, user_agent, referer):
    if img_file.exists():
        return img_file
    try:
        req = urllib.request.Request(img_src)
        req.add_header("Cookie", cookie_str)
        req.add_header("User-Agent", user_agent)
        req.add_header("Referer", referer)
        req.add_header("Accept", "image/*")
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=20) as response:
            data = response.read()
            if len(data) > 1000:
                with open(img_file, "wb") as fh:
                    fh.write(data)
                return img_file
    except Exception as exc:
        logger.debug("Download falhou %s: %s", img_file.name, exc)
    return None


def create_download_pool(max_workers=3):
    return ThreadPoolExecutor(max_workers=max_workers)


def flush_downloads(download_futures):
    for future in download_futures:
        try:
            future.result(timeout=30)
        except Exception:
            pass


def get_pasta_atual(driver) -> str:
    try:
        element = driver.find_element(By.ID, "PastaTxt")
        return (element.text or element.get_attribute("title") or "").strip()
    except Exception:
        return ""


def get_pagina_atual_num(driver) -> int:
    try:
        element = driver.find_element(By.ID, "PagAtualTxt")
        return int(element.get_attribute("value") or "1")
    except Exception:
        return 1


def get_state_js(driver):
    try:
        return driver.execute_script(
            """
            var pasta = document.getElementById('PastaTxt');
            var pagAtual = document.getElementById('PagAtualTxt');
            var pagTotal = document.getElementById('PagTotalLbl');
            var pagFis = document.getElementById('hPagFis');
            var img = document.getElementById('DocumentoImg');
            var imgSrc = (img && img.src && img.src.indexOf('cache') > -1) ? img.src : null;
            var totalMatch = pagTotal ? pagTotal.innerText.match(/\\/(\\d+)/) : null;
            return {
                pasta: pasta ? (pasta.innerText || pasta.title || '').trim() : '',
                pagAtual: pagAtual ? parseInt(pagAtual.value) || 1 : 1,
                pagTotal: totalMatch ? parseInt(totalMatch[1]) : 0,
                pagFis: pagFis ? pagFis.value || '' : '',
                imgSrc: imgSrc
            };
            """
        )
    except Exception:
        return None


def fechar_dialog_copyright(driver):
    try:
        iframes = driver.find_elements(By.NAME, "PesqOpniaoRadWindow")
        if iframes:
            parent = iframes[0].find_element(By.XPATH, "./../../..")
            close_btns = parent.find_elements(By.CSS_SELECTOR, "a.rwCloseButton, a[title='Close']")
            if close_btns:
                driver.execute_script("arguments[0].click();", close_btns[0])
                time.sleep(0.5)
                return

        driver.execute_cdp_cmd(
            "Runtime.evaluate",
            {
                "expression": "var wnd = $find('PesqOpniaoRadWindow'); if (wnd) wnd.close();",
                "returnByValue": True,
            },
        )
        time.sleep(0.5)
    except Exception:
        pass


def get_total_paginas(driver) -> int:
    try:
        total = driver.execute_script(
            """
            var el = document.getElementById('PagTotalLbl');
            if (el) {
                var m = el.innerText.match(/\\/(\\d+)/);
                if (m) return parseInt(m[1]);
            }
            return 0;
            """
        )
        return total or 0
    except Exception:
        return 0


def navegar_para_pagina(driver, num_pagina: int, wait_fn):
    try:
        from selenium.webdriver.common.keys import Keys

        page_input = driver.find_element(By.ID, "PagAtualTxt")
        page_input.clear()
        page_input.send_keys(str(num_pagina))
        page_input.send_keys(Keys.RETURN)
        time.sleep(1)
        wait_fn()
    except Exception as exc:
        logger.warning("Erro ao navegar para página %s: %s", num_pagina, exc)


def proxima_pagina(driver, click_pause: float, wait_fn):
    try:
        driver.execute_script(
            "var btn = document.getElementById('PagPosBtn');"
            "if (btn) btn.click();"
        )
        time.sleep(click_pause)
        wait_fn()
    except Exception as exc:
        logger.warning("Erro ao avançar página: %s", exc)


def aguardar_carregamento(driver, timeout: int = 10):
    try:
        elements = driver.find_elements(By.ID, "updateprogressloaddiv")
        if elements and elements[0].is_displayed():
            WebDriverWait(driver, timeout).until(
                EC.invisibility_of_element_located((By.ID, "updateprogressloaddiv"))
            )
    except (TimeoutException, Exception):
        pass
