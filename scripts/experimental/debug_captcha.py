"""Debug: captura imagem do CAPTCHA e testa OCR."""
import time
import io
import urllib.request
import ssl
import re
from pathlib import Path
from PIL import Image, ImageFilter
import tesserocr

from src.driver import create_driver
from src.config import HDB_DOCREADER_URL, TESSDATA_DIR

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

OUT = Path("data/debug_captcha")
OUT.mkdir(parents=True, exist_ok=True)

driver = create_driver(headless=True)
try:
    url = f"{HDB_DOCREADER_URL}?bib=705110"
    print(f"Abrindo {url}")
    driver.get(url)
    time.sleep(5)

    # Check CAPTCHA
    iframes = driver.find_elements("name", "CaptchaWnd")
    if not iframes:
        print("Sem CAPTCHA!")
        driver.save_screenshot(str(OUT / "no_captcha.png"))
    else:
        print("CAPTCHA detectado!")
        driver.save_screenshot(str(OUT / "page_with_captcha.png"))

        iframe = driver.find_element("name", "CaptchaWnd")
        driver.switch_to.frame(iframe)
        time.sleep(0.5)

        # Save full iframe screenshot
        driver.save_screenshot(str(OUT / "captcha_iframe.png"))

        captcha_img = driver.find_element("id", "RadCaptcha1_CaptchaImageUP")
        img_src = captcha_img.get_attribute("src")
        print(f"CAPTCHA img src: {img_src}")

        # Also try screenshot of element directly
        try:
            captcha_img.screenshot(str(OUT / "captcha_element.png"))
            print("Saved captcha_element.png")
        except Exception as e:
            print(f"Element screenshot failed: {e}")

        if img_src:
            cookies = driver.get_cookies()
            cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
            req = urllib.request.Request(img_src)
            req.add_header("Cookie", cookie_str)
            req.add_header("Referer", driver.current_url)

            with urllib.request.urlopen(req, context=_SSL_CTX, timeout=15) as resp:
                img_data = resp.read()

            # Save raw
            with open(OUT / "captcha_raw.png", "wb") as f:
                f.write(img_data)
            print(f"Saved captcha_raw.png ({len(img_data)} bytes)")

            # Process and OCR
            img = Image.open(io.BytesIO(img_data))
            gray = img.convert("L")
            gray.save(OUT / "captcha_gray.png")
            print(f"Image size: {img.size}")

            approaches = {
                "median5_t140": lambda g: g.filter(ImageFilter.MedianFilter(5)).point(lambda p: 255 if p > 140 else 0),
                "median3_t140": lambda g: g.filter(ImageFilter.MedianFilter(3)).point(lambda p: 255 if p > 140 else 0),
                "plain_t160": lambda g: g.point(lambda p: 255 if p > 160 else 0),
                "plain_t120": lambda g: g.point(lambda p: 255 if p > 120 else 0),
                "plain_t100": lambda g: g.point(lambda p: 255 if p > 100 else 0),
                "median3_t100": lambda g: g.filter(ImageFilter.MedianFilter(3)).point(lambda p: 255 if p > 100 else 0),
                "invert_t140": lambda g: g.point(lambda p: 0 if p > 140 else 255),
            }

            for name, fn in approaches.items():
                processed = fn(gray)
                big = processed.resize((img.width * 3, img.height * 3), Image.LANCZOS)
                big.save(OUT / f"captcha_{name}.png")

                for psm in [tesserocr.PSM.SINGLE_WORD, tesserocr.PSM.SINGLE_LINE, tesserocr.PSM.SINGLE_BLOCK]:
                    text = tesserocr.image_to_text(big, lang="eng", path=str(TESSDATA_DIR), psm=psm)
                    cleaned = re.sub(r'[^A-Za-z0-9]', '', text.strip())
                    if cleaned:
                        print(f"  {name} PSM={psm}: raw='{text.strip()}' cleaned='{cleaned}'")

            # Also try without path (system tessdata)
            print("\n--- Without explicit tessdata path ---")
            for name, fn in approaches.items():
                processed = fn(gray)
                big = processed.resize((img.width * 3, img.height * 3), Image.LANCZOS)
                for psm in [tesserocr.PSM.SINGLE_WORD, tesserocr.PSM.SINGLE_LINE]:
                    try:
                        text = tesserocr.image_to_text(big, lang="eng", psm=psm)
                        cleaned = re.sub(r'[^A-Za-z0-9]', '', text.strip())
                        if cleaned:
                            print(f"  {name} PSM={psm}: raw='{text.strip()}' cleaned='{cleaned}'")
                    except Exception as e:
                        print(f"  {name} PSM={psm}: ERROR {e}")
                        break  # if eng not found without path, skip rest

        driver.switch_to.default_content()

finally:
    driver.quit()
    print("\nDone. Check data/debug_captcha/")
