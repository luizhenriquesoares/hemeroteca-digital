"""Módulo compartilhado para detecção e resolução automática de CAPTCHAs da HDB.

O CAPTCHA da Biblioteca Nacional aparece em 2 lugares:
- Busca de acervos (Pesquisa.aspx)
- Visualização de páginas (DocReader.aspx) a cada ~50 páginas

Ambos usam o mesmo RadCaptcha do Telerik com os mesmos IDs.
"""

import io
import logging
import re
import time

import tesserocr
from selenium.webdriver.common.by import By

from src.config import TESSDATA_DIR

logger = logging.getLogger(__name__)


def _driver_is_headless(driver) -> bool:
    return bool(getattr(driver, "_codex_headless", False))


def captcha_visivel(driver) -> bool:
    """Retorna True se o iframe de CAPTCHA estiver visível na página."""
    try:
        iframes = driver.find_elements(By.NAME, "CaptchaWnd")
        if not iframes:
            return False
        parent = iframes[0].find_element(By.XPATH, "./..")
        return parent.is_displayed()
    except Exception:
        return False


def ocr_captcha(img_data: bytes) -> str:
    """Tenta reconhecer o texto da imagem do CAPTCHA via Tesseract.

    Usa múltiplas abordagens de pré-processamento (blur + threshold)
    e retorna o primeiro resultado com >= 3 caracteres.
    """
    try:
        from PIL import Image, ImageFilter

        img = Image.open(io.BytesIO(img_data))
        gray = img.convert("L")

        approaches = [
            lambda g: g.filter(ImageFilter.MedianFilter(5)).point(
                lambda p: 255 if p > 140 else 0
            ),
            lambda g: g.filter(ImageFilter.MedianFilter(3)).point(
                lambda p: 255 if p > 140 else 0
            ),
            lambda g: g.point(lambda p: 255 if p > 160 else 0),
        ]

        for preprocess in approaches:
            processed = preprocess(gray)
            big = processed.resize(
                (img.width * 3, img.height * 3), Image.LANCZOS
            )
            text = tesserocr.image_to_text(
                big, lang="eng", path=str(TESSDATA_DIR),
                psm=tesserocr.PSM.SINGLE_WORD,
            )
            cleaned = re.sub(r'[^A-Za-z0-9]', '', text.strip())
            if len(cleaned) >= 3:
                return cleaned

        return ""

    except Exception as e:
        logger.warning(f"OCR do CAPTCHA exception: {e}", exc_info=True)
        return ""


def resolver_captcha(driver, max_tentativas=5, timeout_manual=300,
                     on_success=None) -> bool:
    """Detecta e tenta resolver o CAPTCHA via OCR automaticamente.

    Args:
        driver: Selenium WebDriver
        max_tentativas: tentativas de OCR automático antes de fallback manual
        timeout_manual: segundos para esperar resolução manual se auto falhar
        on_success: callback opcional executado após resolver (ex: refresh headers)

    Returns:
        True se resolvido (auto ou manual), False se timeout
    """
    if not captcha_visivel(driver):
        return True

    headless = _driver_is_headless(driver)
    auto_retry_deadline = time.time() + timeout_manual if headless and timeout_manual > 0 else None
    tentativa = 0
    extended_mode_logged = False

    while True:
        if not captcha_visivel(driver):
            logger.info("CAPTCHA resolvido!")
            if on_success:
                on_success()
            return True

        tentativa += 1
        if tentativa > max_tentativas:
            if not headless:
                break
            if auto_retry_deadline and time.time() >= auto_retry_deadline:
                break
            if not extended_mode_logged:
                logger.warning(
                    "Auto-solve inicial falhou em headless; continuando tentativas automáticas por até %ss...",
                    timeout_manual,
                )
                extended_mode_logged = True

        if tentativa <= max_tentativas:
            logger.info("CAPTCHA detectado, tentativa %s/%s...", tentativa, max_tentativas)
        else:
            logger.info("CAPTCHA detectado, tentativa extra headless #%s...", tentativa - max_tentativas)

        try:
            iframe = driver.find_element(By.NAME, "CaptchaWnd")
            driver.switch_to.frame(iframe)
            time.sleep(0.5)

            captcha_img = driver.find_element(By.ID, "RadCaptcha1_CaptchaImageUP")

            # Pegar imagem via screenshot do elemento (bypassa Cloudflare 403)
            try:
                img_data = captcha_img.screenshot_as_png
            except Exception as e:
                logger.warning(f"Screenshot captcha falhou: {e}")
                driver.switch_to.default_content()
                continue

            texto_captcha = ocr_captcha(img_data)
            if not texto_captcha:
                logger.warning("OCR falhou no CAPTCHA, refreshing...")
                try:
                    refresh = driver.find_element(By.ID, "RadCaptcha1_CaptchaLinkButton")
                    driver.execute_script("arguments[0].click();", refresh)
                    time.sleep(1.5)
                except Exception:
                    pass
                driver.switch_to.default_content()
                continue

            logger.info(f"OCR leu CAPTCHA: '{texto_captcha}'")

            driver.execute_script(
                "var el = document.getElementById('RadCaptcha1_CaptchaTextBox');"
                "if (el) { el.value = arguments[0]; el.dispatchEvent(new Event('change')); }",
                texto_captcha
            )
            time.sleep(0.3)

            driver.execute_script(
                "document.getElementById('EnviarBtn_input').click();"
            )

            driver.switch_to.default_content()
            time.sleep(2)

            if not captcha_visivel(driver):
                logger.info("CAPTCHA resolvido automaticamente!")
                if on_success:
                    on_success()
                return True

            logger.warning("CAPTCHA incorreto, tentando novamente...")

        except Exception as e:
            logger.warning(f"Erro ao resolver CAPTCHA: {e}")
            try:
                driver.switch_to.default_content()
            except Exception:
                pass

    if headless:
        logger.error("Timeout esperando CAPTCHA em modo headless")
        return False

    logger.warning("Auto-solve falhou. Resolva o CAPTCHA manualmente...")
    for _ in range(timeout_manual):
        time.sleep(1)
        if not captcha_visivel(driver):
            logger.info("CAPTCHA resolvido manualmente!")
            if on_success:
                on_success()
            return True

    logger.error("Timeout esperando CAPTCHA")
    return False
