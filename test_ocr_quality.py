"""Teste rápido de qualidade OCR: compara PSM 3 (auto) vs PSM 6 (2 colunas fixas)."""
from pathlib import Path
from PIL import Image, ImageEnhance, ImageFilter
import tesserocr
from src.config import TESSDATA_DIR, TESSERACT_LANG

# Usar uma imagem existente de um acervo hi-res já baixado
# Vamos pegar uma do cache ou baixar uma
import tempfile, time
from src.driver import create_driver
from src.config import HDB_DOCREADER_URL

OUT = Path("data/debug_captcha")
OUT.mkdir(parents=True, exist_ok=True)

print("Baixando imagem hi-res de teste...")
driver = create_driver(headless=True)

try:
    from src.hires_pipeline import _resolver_captcha, _fechar_dialog, _wait_for_cache_url

    bib = "029033_06"  # Diário de Pernambuco 1880-1889
    url = f"{HDB_DOCREADER_URL}?bib={bib}"
    driver.get(url)
    time.sleep(5)
    _resolver_captcha(driver)
    _fechar_dialog(driver)
    time.sleep(2)

    # Pegar imagem low-res primeiro
    low_src = _wait_for_cache_url(driver)
    print(f"Low-res URL: {low_src}")

    # Solicitar hi-res
    driver.execute_script("""
        document.getElementById('HiddenSize').value = '4000x5500';
        document.getElementById('hPagFis').value = '1';
        document.getElementById('CarregaImagemHiddenButton').click();
    """)

    hi_src = _wait_for_cache_url(driver, old_src=low_src, timeout=30)
    print(f"Hi-res URL: {hi_src}")

    # Download
    import urllib.request, ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    cookies = driver.get_cookies()
    cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)

    req = urllib.request.Request(hi_src)
    req.add_header("Cookie", cookie_str)
    req.add_header("Referer", driver.current_url)

    img_path = OUT / "test_hires.jpg"
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        with open(img_path, "wb") as f:
            f.write(resp.read())

    print(f"Imagem salva: {img_path} ({img_path.stat().st_size / 1024:.0f}KB)")

    # Preprocessing
    img = Image.open(img_path).convert("L")
    img = img.filter(ImageFilter.MedianFilter(3))
    img = ImageEnhance.Contrast(img).enhance(1.8)
    img = ImageEnhance.Sharpness(img).enhance(2.0)
    img = img.point(lambda p: 255 if p > 130 else 0)
    print(f"Image size: {img.size}")

    # === Método 1: PSM 3 (auto) página inteira ===
    print("\n" + "="*60)
    print("MÉTODO 1: PSM 3 (auto segmentation) - página inteira")
    print("="*60)
    text_psm3 = tesserocr.image_to_text(img, lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=3)
    words_psm3 = len([w for w in text_psm3.split() if len(w) > 2])
    print(f"Palavras (>2 chars): {words_psm3}")
    print(text_psm3[:2000])

    # === Método 2: PSM 6 com 2 colunas fixas ===
    print("\n" + "="*60)
    print("MÉTODO 2: PSM 6 (2 colunas fixas)")
    print("="*60)
    w, h = img.size
    margin = int(w * 0.03)
    header_end = int(h * 0.18)
    footer_start = int(h * 0.95)
    textos = []
    header = img.crop((margin, 0, w - margin, header_end))
    textos.append(tesserocr.image_to_text(header, lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=6))
    col_w = (w - 2 * margin) // 2
    for i in range(2):
        x1 = margin + i * col_w
        x2 = margin + (i + 1) * col_w
        col = img.crop((x1, header_end, x2, footer_start))
        textos.append(tesserocr.image_to_text(col, lang=TESSERACT_LANG, path=str(TESSDATA_DIR), psm=6))
    text_psm6 = "\n\n".join(textos)
    words_psm6 = len([w for w in text_psm6.split() if len(w) > 2])
    print(f"Palavras (>2 chars): {words_psm6}")
    print(text_psm6[:2000])

    print("\n" + "="*60)
    print(f"RESUMO: PSM 3 = {words_psm3} palavras | PSM 6 (2 cols) = {words_psm6} palavras")

finally:
    driver.quit()
