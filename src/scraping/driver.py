"""Setup do driver Selenium com anti-detecção Cloudflare."""

import logging
import time
import random

import undetected_chromedriver as uc

from src.config import PAGE_LOAD_TIMEOUT, IMPLICIT_WAIT

logger = logging.getLogger(__name__)


def create_driver(headless: bool = True) -> uc.Chrome:
    """Cria instância do Chrome com undetected-chromedriver (bypassa Cloudflare)."""
    options = uc.ChromeOptions()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-insecure-localhost")

    # Chrome mais leve
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-translate")

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=146)

    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.implicitly_wait(IMPLICIT_WAIT)

    logger.info("Driver Chrome criado com sucesso (undetected)")
    return driver


def human_delay(min_sec: float = 2.0, max_sec: float = 5.0):
    """Simula comportamento humano com delay aleatório."""
    delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)


def safe_click(driver, element, pause: float = 1.5):
    """Clica em elemento com scroll e pausa."""
    driver.execute_script("arguments[0].scrollIntoView(true);", element)
    time.sleep(0.3)
    element.click()
    time.sleep(pause)
