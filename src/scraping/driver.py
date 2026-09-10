"""Setup do driver Selenium com anti-detecção Cloudflare."""

import logging
import random
import time

import undetected_chromedriver as uc

from src.config import PAGE_LOAD_TIMEOUT, IMPLICIT_WAIT

logger = logging.getLogger(__name__)

STEALTH_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)
STEALTH_LANG = "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"


def _apply_runtime_stealth(driver) -> None:
    """Ajusta fingerprints que ainda vazam no Chrome headless."""
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {
                "userAgent": STEALTH_USER_AGENT,
                "acceptLanguage": STEALTH_LANG,
                "platform": "MacIntel",
            },
        )
    except Exception as exc:
        logger.debug("Falha ao aplicar override de user-agent: %s", exc)

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
Object.defineProperty(navigator, 'language', { get: () => 'pt-BR' });
Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] });
Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
""".strip()
            },
        )
    except Exception as exc:
        logger.debug("Falha ao aplicar script de stealth: %s", exc)


def create_driver(headless: bool = True) -> uc.Chrome:
    """Cria instância do Chrome com undetected-chromedriver (bypassa Cloudflare)."""
    options = uc.ChromeOptions()

    if headless:
        options.add_argument("--headless=new")
        options.add_argument(f"--user-agent={STEALTH_USER_AGENT}")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    options.add_argument("--lang=pt-BR")
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
    driver._codex_headless = headless
    _apply_runtime_stealth(driver)

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
