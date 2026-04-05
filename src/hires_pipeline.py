"""Pipeline de OCR em alta resolução.

Baixa imagens hi-res do DocReader da BN, faz OCR e deleta a imagem.
Usa apenas espaço temporário (~4MB por imagem), sem acumular no disco.

Uso:
    python main.py ocr-hires                  # Todos os acervos
    python main.py ocr-hires --bib 029033_06  # Um acervo específico
"""

import json
import logging
import re
import ssl
import tempfile
import time
import urllib.request
from pathlib import Path

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

logger = logging.getLogger(__name__)

# SSL reutilizável
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# Tamanho solicitado ao servidor (resolução máxima disponível)
HIRES_SIZE = "6464x8940"

# Delays anti-bloqueio (segundos)
MIN_DELAY = 1.5
MAX_DELAY = 4.0


# ── OCR para imagens hi-res com segmentação de colunas ───────────────

def _detectar_header_end(arr, w: int, h: int) -> int:
    """Detecta fim do cabeçalho via linha horizontal com grande bloco preto (título)."""
    import numpy as np
    row_proj = (arr < 80).sum(axis=1)
    search_end = int(h * 0.40)
    threshold = w * 0.25
    peaks = np.where(row_proj[:search_end] > threshold)[0]
    if len(peaks) == 0:
        return int(h * 0.20)
    last_peak = peaks[-1]
    return min(last_peak + int(h * 0.02), int(h * 0.40))


def _quality_score(texto: str) -> tuple[int, int, float]:
    """Retorna (total_palavras, palavras_reais, pct_qualidade)."""
    palavras = texto.split()
    total = len(palavras)
    if not total:
        return 0, 0, 0.0
    reais = sum(1 for p in palavras if len(re.findall(r'[a-zA-ZÀ-ú]', p)) >= 3)
    return total, reais, reais * 100 / total


def _limpar_texto(texto: str) -> str:
    """Remove linhas com pouco conteúdo útil."""
    lines = texto.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if len(re.findall(r'[a-zA-ZÀ-ú0-9]', stripped)) >= 2:
            cleaned.append(stripped)
    texto = "\n".join(cleaned)
    texto = re.sub(r'\n{3,}', '\n\n', texto)
    texto = re.sub(r' {3,}', ' ', texto)
    return texto.strip()


def _ocr_body_cols(img, header_end: int, n_cols: int, margin: int,
                   footer_start: int, w: int) -> str:
    """OCR somente no corpo, dividido em N colunas iguais com PSM 6."""
    textos = []
    col_w = (w - 2 * margin) // n_cols
    for i in range(n_cols):
        x1 = margin + i * col_w
        x2 = margin + (i + 1) * col_w
        col = img.crop((x1, header_end, x2, footer_start))
        t = tesserocr.image_to_text(col, lang=TESSERACT_LANG,
                                     path=str(TESSDATA_DIR), psm=6)
        if t.strip():
            textos.append(t.strip())
    return "\n\n".join(textos)


def _ocr_hires(img_path: Path) -> str:
    """OCR em imagem hi-res: skip header + testa N colunas, escolhe melhor."""
    import numpy as np
    from PIL import Image, ImageEnhance, ImageFilter

    img = Image.open(img_path).convert("L")

    # Preprocess: grayscale + normalização de contraste (sem binarização)
    img = img.filter(ImageFilter.MedianFilter(3))
    arr = np.array(img)
    p2 = np.percentile(arr, 2)
    p98 = np.percentile(arr, 98)
    if p98 > p2:
        arr = np.clip((arr - p2) * 255 / (p98 - p2), 0, 255).astype(np.uint8)
    img = Image.fromarray(arr)
    img = ImageEnhance.Contrast(img).enhance(1.3)
    img = ImageEnhance.Sharpness(img).enhance(1.5)
    arr = np.array(img)

    w, h = img.size
    margin = int(w * 0.02)
    footer_start = int(h * 0.96)

    # Header dinâmico: skip do cabeçalho (título/logo/bordas geram ruído)
    header_end = _detectar_header_end(arr, w, h)

    # Testar 3, 4, 5 colunas e escolher pela maior quantidade de palavras reais
    melhor_texto = ""
    melhor_reais = 0

    for n_cols in (3, 4, 5):
        texto = _ocr_body_cols(img, header_end, n_cols, margin, footer_start, w)
        texto = _limpar_texto(texto)
        _, reais, _ = _quality_score(texto)
        if reais > melhor_reais:
            melhor_reais = reais
            melhor_texto = texto

    # Fallback: página inteira PSM 3 se o resultado for muito ruim
    if melhor_reais < 200:
        texto_full = tesserocr.image_to_text(img, lang=TESSERACT_LANG,
                                              path=str(TESSDATA_DIR), psm=3)
        texto_full = _limpar_texto(texto_full)
        _, reais_full, _ = _quality_score(texto_full)
        if reais_full > melhor_reais:
            melhor_texto = texto_full

    return melhor_texto


# ── Helpers do Selenium ──────────────────────────────────────────────

def _resolver_captcha(driver, max_tentativas=5, timeout_manual=300):
    """Detecta e resolve CAPTCHA (reutiliza lógica do scraper)."""
    from src.scraper import JornalScraper
    # Criar instância temporária apenas para usar o método de CAPTCHA
    tmp = JornalScraper.__new__(JornalScraper)
    tmp.driver = driver
    tmp._captcha_visivel = lambda: _captcha_visivel(driver)
    return JornalScraper._resolver_captcha(tmp, max_tentativas, timeout_manual)


def _captcha_visivel(driver) -> bool:
    try:
        iframes = driver.find_elements("name", "CaptchaWnd")
        if not iframes:
            return False
        parent = iframes[0].find_element("xpath", "./..")
        return parent.is_displayed()
    except Exception:
        return False


def _fechar_dialog(driver):
    try:
        driver.execute_script(
            "var wnd = $find('PesqOpniaoRadWindow'); if (wnd) wnd.close();"
        )
        time.sleep(0.5)
    except Exception:
        pass


def _wait_for_cache_url(driver, old_src=None, timeout=20):
    """Espera a URL de cache da imagem aparecer (ou mudar)."""
    for _ in range(timeout):
        src = driver.execute_script("""
            var img = document.getElementById('DocumentoImg');
            return (img && img.src && img.src.indexOf('cache') > -1) ? img.src : null;
        """)
        if src and src != old_src:
            return src
        time.sleep(1)
    return None


def _proxima_pagina(driver):
    try:
        driver.execute_script(
            "var btn = document.getElementById('PagPosBtn');"
            "if (btn) btn.click();"
        )
        time.sleep(CLICK_PAUSE)
        # Aguardar loading
        driver.execute_script("""
            var el = document.getElementById('updateprogressloaddiv');
            // Se visível, esperar sumir (max 10s no JS)
        """)
        time.sleep(0.5)
    except Exception as e:
        logger.warning(f"Erro ao avançar página: {e}")


def _get_page_state(driver):
    """Retorna estado atual da página."""
    try:
        return driver.execute_script("""
            var pagAtual = document.getElementById('PagAtualTxt');
            var pagTotal = document.getElementById('PagTotalLbl');
            var pasta = document.getElementById('PastaTxt');
            var totalMatch = pagTotal ? pagTotal.innerText.match(/\\/(\\d+)/) : null;
            return {
                pagAtual: pagAtual ? parseInt(pagAtual.value) || 1 : 1,
                pagTotal: totalMatch ? parseInt(totalMatch[1]) : 0,
                pasta: pasta ? (pasta.innerText || pasta.title || '').trim() : ''
            };
        """)
    except Exception:
        return None


# ── Pipeline principal ───────────────────────────────────────────────

def processar_acervo_hires(bib: str, nome: str, headless: bool = True,
                           force: bool = False, max_pages: int = 0,
                           keep_images: bool = False) -> int:
    """
    Pipeline hi-res para um acervo completo.

    1. Abre o DocReader com Selenium
    2. Seta HiddenSize grande para obter imagens hi-res
    3. Para cada página: baixa → OCR → salva texto → (deleta ou mantém imagem)
    4. Retorna número de páginas processadas

    Args:
        max_pages: limite de páginas (0 = sem limite)
        keep_images: se True, salva imagens hi-res em IMAGES_DIR/bib/
    """
    from src.driver import create_driver

    txt_dir = TEXT_DIR / bib
    txt_dir.mkdir(parents=True, exist_ok=True)

    driver = create_driver(headless=headless)

    try:
        return _processar_com_driver(driver, bib, nome, txt_dir, force,
                                     max_pages=max_pages,
                                     keep_images=keep_images)
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _processar_com_driver(driver, bib, nome, txt_dir, force,
                          max_pages: int = 0, keep_images: bool = False):
    """Lógica principal do pipeline (separada para reuso com driver externo)."""
    url = f"{HDB_DOCREADER_URL}?bib={bib}"
    logger.info(f"Hi-res OCR: abrindo {nome} ({bib})")
    driver.get(url)
    time.sleep(5)

    # CAPTCHA e dialogs
    _resolver_captcha(driver)
    _fechar_dialog(driver)
    time.sleep(2)

    # Esperar imagem padrão (low-res) carregar primeiro
    low_res_src = _wait_for_cache_url(driver)
    if not low_res_src:
        logger.error(f"Imagem padrão não carregou para {bib}")
        return 0
    logger.info(f"Low-res carregada: {low_res_src.split('/')[-1]}")

    # Setar HiddenSize grande e recarregar com alta resolução
    driver.execute_script(f"""
        document.getElementById('HiddenSize').value = '{HIRES_SIZE}';
        document.getElementById('hPagFis').value = '1';
        document.getElementById('CarregaImagemHiddenButton').click();
    """)

    # Esperar a URL MUDAR (de low-res para hi-res)
    first_src = _wait_for_cache_url(driver, old_src=low_res_src, timeout=30)
    if not first_src:
        logger.error(f"Imagem hi-res não carregou para {bib}")
        return 0

    logger.info(f"Hi-res OK: {first_src.split('/')[-1]}")

    # Cookies para downloads HTTP
    cookie_str = _get_cookie_str(driver)
    ua = driver.execute_script("return navigator.userAgent")
    referer = driver.current_url

    processadas = 0
    skipped = 0
    global_page = 1
    stale_count = 0
    max_stale = 5
    prev_src = None
    t0 = time.time()

    # Detectar total de páginas via DOM (para ETA)
    try:
        total_esperado = driver.execute_script("""
            var el = document.getElementById('PagTotalLbl');
            if (el) {
                var m = el.innerText.match(/\\/(\\d+)/);
                if (m) return parseInt(m[1]);
            }
            return 0;
        """) or 0
    except Exception:
        total_esperado = 0

    # Dir para imagens hi-res (se keep_images=True)
    img_dir = IMAGES_DIR / bib if keep_images else None
    if img_dir:
        img_dir.mkdir(parents=True, exist_ok=True)

    while True:
        # Limite de páginas
        if max_pages and processadas >= max_pages:
            logger.info(f"Limite de {max_pages} páginas atingido para {bib}")
            break

        txt_path = txt_dir / f"{bib}_{global_page:05d}.txt"
        meta_path = txt_dir / f"{bib}_{global_page:05d}.json"

        # Skip se já processado em hi-res
        if txt_path.exists() and not force:
            # Checar se é hi-res
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text())
                    if meta.get("hires"):
                        skipped += 1
                        _proxima_pagina(driver)
                        global_page += 1
                        prev_src = None
                        continue
                except Exception:
                    pass

        # Garantir HiddenSize grande (pode resetar após postbacks)
        driver.execute_script(
            f"document.getElementById('HiddenSize').value = '{HIRES_SIZE}';"
        )

        # Pegar URL hi-res
        src = _wait_for_cache_url(driver, old_src=prev_src, timeout=15)
        if not src:
            stale_count += 1
            if stale_count >= max_stale:
                logger.info(f"Fim do acervo {bib} na página {global_page}")
                break
            _proxima_pagina(driver)
            global_page += 1
            prev_src = None
            continue

        stale_count = 0
        prev_src = src

        try:
            # Download para arquivo temporário
            tmp_path = Path(tempfile.mktemp(suffix=".jpg"))

            req = urllib.request.Request(src)
            req.add_header("Cookie", cookie_str)
            req.add_header("User-Agent", ua)
            req.add_header("Referer", referer)
            req.add_header("Accept", "image/*")

            with urllib.request.urlopen(req, context=_SSL_CTX, timeout=30) as resp:
                data = resp.read()

            if len(data) < 1000:
                logger.warning(f"Imagem muito pequena: {bib} p{global_page}")
                _proxima_pagina(driver)
                global_page += 1
                continue

            tmp_path.write_bytes(data)

            # Salvar imagem hi-res se keep_images=True
            if img_dir:
                img_persistent = img_dir / f"{bib}_{global_page:05d}.jpg"
                img_persistent.write_bytes(data)

            # OCR
            texto = _ocr_hires(tmp_path)

            if texto:
                txt_path.write_text(texto, encoding="utf-8")
                metadata = {
                    "bib": bib,
                    "imagem": f"{bib}_{global_page:05d}.jpg",
                    "arquivo_texto": txt_path.name,
                    "caracteres": len(texto),
                    "palavras": len(texto.split()),
                    "hires": True,
                }
                meta_path.write_text(
                    json.dumps(metadata, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                processadas += 1
            else:
                logger.warning(f"OCR vazio: {bib} p{global_page}")

        except Exception as e:
            logger.error(f"Erro hi-res {bib} p{global_page}: {e}")
        finally:
            # Sempre deletar imagem temporária
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

        # Progresso
        if processadas % 50 == 0 and processadas > 0:
            elapsed = time.time() - t0
            rate = processadas / elapsed
            eta = (total_esperado - global_page) / rate if rate > 0 else 0
            logger.info(
                f"Hi-res {bib}: {processadas} feitas "
                f"(p{global_page}/{total_esperado}) | "
                f"{rate:.1f} pg/s | ETA {eta/60:.0f}min"
            )
            # Refresh cookies
            cookie_str = _get_cookie_str(driver)
            # Checar CAPTCHA
            if _captcha_visivel(driver):
                _resolver_captcha(driver)
                cookie_str = _get_cookie_str(driver)

        # Próxima página
        _proxima_pagina(driver)
        global_page += 1

    elapsed = time.time() - t0
    logger.info(
        f"Hi-res {bib} concluído: {processadas} páginas em {elapsed/60:.1f}min "
        f"(skipped: {skipped})"
    )
    return processadas


def _get_cookie_str(driver):
    cookies = driver.get_cookies()
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies)


# ── Orquestrador multi-acervo (paralelo) ─────────────────────────────

def _worker_hires(worker_id, acervos_queue, progress_file, headless, force):
    """Worker que processa acervos da fila com seu próprio Chrome driver."""
    from src.driver import create_driver

    logger.info(f"Worker {worker_id}: iniciando")
    driver = create_driver(headless=headless)

    try:
        while True:
            # Pegar próximo acervo da fila
            try:
                acervo = acervos_queue.pop(0)
            except IndexError:
                break  # Fila vazia

            bib = acervo["bib"]
            nome = acervo["nome"]
            logger.info(f"Worker {worker_id}: processando {nome} ({bib})")

            txt_dir = TEXT_DIR / bib
            txt_dir.mkdir(parents=True, exist_ok=True)

            try:
                count = _processar_com_driver(driver, bib, nome, txt_dir, force)

                # Salvar progresso (thread-safe com lock de arquivo)
                _save_progress(progress_file, bib)

                logger.info(f"Worker {worker_id}: ✓ {nome} — {count} páginas")

            except Exception as e:
                logger.error(f"Worker {worker_id}: ERRO em {bib}: {e}")
                # Recriar driver em caso de erro fatal
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = create_driver(headless=headless)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        logger.info(f"Worker {worker_id}: finalizado")


def _save_progress(progress_file, bib):
    """Salva progresso de forma segura (lock de arquivo)."""
    import fcntl

    for _ in range(5):
        try:
            with open(progress_file, "r+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                data = json.load(f)
                if bib not in data["done"]:
                    data["done"].append(bib)
                f.seek(0)
                json.dump(data, f)
                f.truncate()
                fcntl.flock(f, fcntl.LOCK_UN)
            return
        except Exception:
            time.sleep(0.5)


def processar_todos_hires(headless: bool = True, force: bool = False,
                          bib: str = None, workers: int = 4,
                          max_pages: int = 0, keep_images: bool = False):
    """Processa todos os acervos com pipeline hi-res em paralelo.

    Args:
        workers: número de Chrome drivers paralelos (default: 4)
        max_pages: limite de páginas por acervo (0 = sem limite)
        keep_images: se True, salva imagens hi-res em IMAGES_DIR/bib/
    """
    from threading import Thread

    # Carregar lista de acervos
    if bib:
        acervos = [{"bib": bib, "nome": f"Acervo {bib}"}]
    else:
        cache_file = CACHE_DIR / "acervos_pe.json"
        if not cache_file.exists():
            logger.error("Cache de acervos não encontrado. Execute 'listar' primeiro.")
            return {}
        with open(cache_file) as f:
            acervos = json.load(f)

    # Progresso
    progress_file = CACHE_DIR / "hires_progress.json"
    done = set()
    if progress_file.exists():
        with open(progress_file) as f:
            done = set(json.load(f).get("done", []))
    else:
        with open(progress_file, "w") as f:
            json.dump({"done": []}, f)

    pendentes = [a for a in acervos if a["bib"] not in done or force]

    logger.info(
        f"Hi-res pipeline: {len(acervos)} acervos | "
        f"{len(done)} concluídos | {len(pendentes)} pendentes | "
        f"{workers} workers"
    )

    if not pendentes:
        logger.info("Nenhum acervo pendente.")
        return {}

    # Fila compartilhada (lista thread-safe com pop atômico via lock)
    from threading import Lock
    queue_lock = Lock()
    queue = list(pendentes)

    def safe_pop():
        with queue_lock:
            if queue:
                return queue.pop(0)
            return None

    def worker_fn(worker_id):
        from src.driver import create_driver
        driver = create_driver(headless=headless)

        try:
            while True:
                acervo = safe_pop()
                if acervo is None:
                    break

                bib = acervo["bib"]
                nome = acervo["nome"]
                logger.info(f"Worker {worker_id}: processando {nome} ({bib})")

                txt_dir = TEXT_DIR / bib
                txt_dir.mkdir(parents=True, exist_ok=True)

                try:
                    count = _processar_com_driver(driver, bib, nome, txt_dir, force,
                                                  max_pages=max_pages,
                                                  keep_images=keep_images)
                    _save_progress(progress_file, bib)
                    logger.info(f"Worker {worker_id}: ✓ {nome} — {count} páginas")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: ERRO em {bib}: {e}")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = create_driver(headless=headless)
        finally:
            try:
                driver.quit()
            except Exception:
                pass
            logger.info(f"Worker {worker_id}: finalizado")

    # Lançar threads
    threads = []
    for i in range(min(workers, len(pendentes))):
        t = Thread(target=worker_fn, args=(i + 1,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(3)  # Escalonar início para evitar CAPTCHA simultâneo

    # Aguardar todos terminarem
    for t in threads:
        t.join()

    # Contar resultados
    with open(progress_file) as f:
        final_done = set(json.load(f).get("done", []))

    new_done = final_done - done
    logger.info(f"Hi-res pipeline concluído: {len(new_done)} acervos processados")

    return {b: 1 for b in new_done}
