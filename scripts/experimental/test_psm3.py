"""Teste rápido: baixa 1 imagem hi-res e compara PSM 3 vs PSM 6 (2 colunas)."""
import time, ssl, urllib.request
from pathlib import Path
from PIL import Image, ImageEnhance, ImageFilter
import tesserocr
from src.config import TESSDATA_DIR, TESSERACT_LANG, HDB_DOCREADER_URL
from src.driver import create_driver

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

driver = create_driver(headless=True)
try:
    bib = "029033_06"
    driver.get(f"{HDB_DOCREADER_URL}?bib={bib}")
    time.sleep(6)

    # Resolver CAPTCHA
    from src.hires_pipeline import _resolver_captcha, _fechar_dialog, _wait_for_cache_url
    _resolver_captcha(driver)
    _fechar_dialog(driver)
    time.sleep(2)

    # Low-res URL
    low_src = _wait_for_cache_url(driver)
    if not low_src:
        print("ERRO: não conseguiu pegar URL da imagem")
        exit(1)
    print(f"Low-res: {low_src}")

    # Pedir hi-res
    driver.execute_script("""
        document.getElementById('HiddenSize').value = '4000x5500';
        document.getElementById('hPagFis').value = '1';
        document.getElementById('CarregaImagemHiddenButton').click();
    """)
    hi_src = _wait_for_cache_url(driver, old_src=low_src, timeout=30)
    print(f"Hi-res: {hi_src}")

    # Download
    cookies = driver.get_cookies()
    cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    req = urllib.request.Request(hi_src)
    req.add_header("Cookie", cookie_str)
    req.add_header("Referer", driver.current_url)

    img_path = Path("data/debug_captcha/test_hires_sample.jpg")
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        with open(img_path, "wb") as f:
            f.write(resp.read())
    print(f"Imagem: {img_path} ({img_path.stat().st_size // 1024}KB)")

    # Preprocess
    img = Image.open(img_path).convert("L")
    img = img.filter(ImageFilter.MedianFilter(3))
    img = ImageEnhance.Contrast(img).enhance(1.8)
    img = ImageEnhance.Sharpness(img).enhance(2.0)
    img = img.point(lambda p: 255 if p > 130 else 0)
    print(f"Size: {img.size}")

    # PSM 3
    t3 = tesserocr.image_to_text(img, lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=3)
    w3 = len([w for w in t3.split() if len(w) > 2])

    # PSM 6 (2 colunas fixas - método atual)
    w, h = img.size
    margin = int(w * 0.03)
    header_end = int(h * 0.18)
    footer_start = int(h * 0.95)
    txts = []
    txts.append(tesserocr.image_to_text(img.crop((margin, 0, w - margin, header_end)), lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=6))
    col_w = (w - 2 * margin) // 2
    for i in range(2):
        x1 = margin + i * col_w
        x2 = margin + (i + 1) * col_w
        txts.append(tesserocr.image_to_text(img.crop((x1, header_end, x2, footer_start)), lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=6))
    t6 = "\n\n".join(txts)
    w6 = len([w for w in t6.split() if len(w) > 2])

    print(f"\n{'='*60}")
    print(f"PSM 3 (auto): {w3} palavras")
    print(f"PSM 6 (2col): {w6} palavras")
    print(f"{'='*60}")
    print("\n--- PSM 3 (primeiros 1500 chars) ---")
    print(t3[:1500])
    print(f"\n--- PSM 6 2-colunas (primeiros 1500 chars) ---")
    print(t6[:1500])

finally:
    driver.quit()
