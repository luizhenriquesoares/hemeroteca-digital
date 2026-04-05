"""Módulo principal de scraping: navega páginas dos jornais e baixa imagens."""

import io
import json
import logging
import re
import ssl
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

import tesserocr

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)

from src.config import (
    HDB_BASE_URL,
    HDB_DOCREADER_URL,
    IMAGES_DIR,
    CACHE_DIR,
    CLICK_PAUSE,
    TESSDATA_DIR,
)
from src.driver import human_delay

logger = logging.getLogger(__name__)

# SSL context reutilizável (evita recriar por imagem)
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _download_image_task(img_src, img_file, cookie_str, user_agent, referer):
    """Tarefa de download executada em thread separada."""
    if img_file.exists():
        return img_file
    try:
        req = urllib.request.Request(img_src)
        req.add_header("Cookie", cookie_str)
        req.add_header("User-Agent", user_agent)
        req.add_header("Referer", referer)
        req.add_header("Accept", "image/*")
        with urllib.request.urlopen(req, context=_SSL_CTX, timeout=20) as response:
            data = response.read()
            if len(data) > 1000:
                with open(img_file, "wb") as f:
                    f.write(data)
                return img_file
    except Exception as e:
        logger.debug(f"Download falhou {img_file.name}: {e}")
    return None


class JornalScraper:
    """Scraper para um jornal específico da HDB."""

    def __init__(self, driver, bib: str, nome: str):
        self.driver = driver
        self.bib = bib
        self.nome = nome
        self.jornal_img_dir = IMAGES_DIR / bib
        self.jornal_img_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = CACHE_DIR / f"{bib}_pages.json"
        self.pages_done = self._load_cache()
        # Thread pool para downloads assíncronos
        self._download_pool = ThreadPoolExecutor(max_workers=3)
        self._download_futures = []
        # Cache de cookies/headers (evita pedir ao driver toda página)
        self._cookie_str = None
        self._user_agent = None
        self._referer = None

    def _load_cache(self) -> set:
        if self.cache_file.exists():
            with open(self.cache_file) as f:
                return set(json.load(f))
        return set()

    def _save_cache(self):
        with open(self.cache_file, "w") as f:
            json.dump(list(self.pages_done), f)

    def _refresh_http_headers(self):
        """Atualiza cookies/headers do driver (chamar 1x por acervo ou após CAPTCHA)."""
        try:
            cookies = self.driver.get_cookies()
            self._cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
            self._user_agent = self.driver.execute_script("return navigator.userAgent")
            self._referer = self.driver.current_url
        except Exception:
            pass

    def _flush_downloads(self):
        """Aguarda downloads pendentes terminarem."""
        for fut in self._download_futures:
            try:
                fut.result(timeout=30)
            except Exception:
                pass
        self._download_futures.clear()

    def _get_pasta_atual(self) -> str:
        try:
            el = self.driver.find_element(By.ID, "PastaTxt")
            return (el.text or el.get_attribute("title") or "").strip()
        except Exception:
            return ""

    def _avancar_para_pagina_global(self, target: int):
        """Avança via PagPosBtn até chegar na página global desejada."""
        max_skip = target - 1
        skipped = 0
        while skipped < max_skip:
            pages_in_edition = self._get_total_paginas()
            current_page_in_ed = self._get_pagina_atual_num()
            remaining_in_ed = pages_in_edition - current_page_in_ed
            pages_to_skip = max_skip - skipped

            if pages_to_skip >= remaining_in_ed:
                if remaining_in_ed > 0:
                    self._navegar_para_pagina(pages_in_edition)
                    time.sleep(0.5)
                    skipped += remaining_in_ed
                self._proxima_pagina()
                time.sleep(0.5)
                skipped += 1
            else:
                target_in_ed = current_page_in_ed + pages_to_skip
                self._navegar_para_pagina(target_in_ed)
                skipped += pages_to_skip
        logger.info(f"Avançou para página global {target} (pulou {skipped})")

    def _get_pagina_atual_num(self) -> int:
        try:
            el = self.driver.find_element(By.ID, "PagAtualTxt")
            return int(el.get_attribute("value") or "1")
        except Exception:
            return 1

    def _get_state_js(self):
        """Obtém todo o estado da página em uma única chamada JS."""
        try:
            return self.driver.execute_script("""
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
            """)
        except Exception:
            return None

    def scrape_todas_paginas(self, max_pages: int = 0) -> list[dict]:
        """Navega por TODAS as páginas de TODAS as edições do jornal."""
        url = f"{HDB_DOCREADER_URL}?bib={self.bib}"
        logger.info(f"Abrindo jornal: {self.nome} ({self.bib})")
        self.driver.get(url)
        time.sleep(3)

        self._resolver_captcha()
        self._fechar_dialog_copyright()

        total_ed = self._get_total_paginas()
        if total_ed == 0:
            logger.warning(f"Não foi possível determinar total de páginas para {self.bib}")
            return []

        # Capturar headers HTTP uma vez
        self._refresh_http_headers()

        pasta_ini = self._get_pasta_atual()
        logger.info(f"Jornal {self.nome}: edição '{pasta_ini}' {total_ed} pgs, "
                     f"{len(self.pages_done)} já processadas")

        resultados = []
        global_page = 1

        if self.pages_done:
            global_page = max(int(p) for p in self.pages_done) + 1
            logger.info(f"Retomando da página global {global_page}...")
            self._avancar_para_pagina_global(global_page)

        stale_count = 0
        max_stale = 5
        save_interval = 10  # salvar cache a cada N páginas

        while True:
            if max_pages and len(resultados) >= max_pages:
                break

            page_str = str(global_page)
            if page_str in self.pages_done:
                self._proxima_pagina()
                global_page += 1
                continue

            # Estado atual em UMA chamada JS
            state = self._get_state_js()
            if not state:
                global_page += 1
                self._proxima_pagina()
                continue

            pasta_antes = state["pasta"]
            pag_antes = state["pagAtual"]

            try:
                # Extrair metadata + enfileirar download em paralelo
                metadata = {
                    "bib": self.bib,
                    "jornal": self.nome,
                    "pagina": global_page,
                    "pagina_fisica": state["pagFis"],
                    "pasta": pasta_antes,
                    "link": f"{HDB_BASE_URL}/docreader/{self.bib}/{pag_antes}",
                }

                # Parsear ano/edição da pasta
                parts = pasta_antes.split("\\")
                if len(parts) >= 2:
                    metadata["ano"] = parts[0].strip()
                    metadata["edicao"] = parts[1].strip()
                else:
                    metadata["ano"] = pasta_antes
                    metadata["edicao"] = ""

                # Enfileirar download assíncrono se temos img src
                img_file = self.jornal_img_dir / f"{self.bib}_{global_page:05d}.jpg"
                if state["imgSrc"] and not img_file.exists():
                    fut = self._download_pool.submit(
                        _download_image_task,
                        state["imgSrc"], img_file,
                        self._cookie_str, self._user_agent, self._referer,
                    )
                    self._download_futures.append(fut)
                    metadata["imagem"] = str(img_file)
                elif img_file.exists():
                    metadata["imagem"] = str(img_file)
                else:
                    # Sem src ainda - tentar uma vez rápida
                    time.sleep(0.3)
                    src = self.driver.execute_script("""
                        var img = document.getElementById('DocumentoImg');
                        return (img && img.src && img.src.indexOf('cache') > -1) ? img.src : null;
                    """)
                    if src:
                        fut = self._download_pool.submit(
                            _download_image_task,
                            src, img_file,
                            self._cookie_str, self._user_agent, self._referer,
                        )
                        self._download_futures.append(fut)
                        metadata["imagem"] = str(img_file)
                    else:
                        metadata["imagem"] = ""

                resultados.append(metadata)
                self.pages_done.add(page_str)

                if len(resultados) % save_interval == 0:
                    self._save_cache()

                if len(resultados) % 100 == 0:
                    logger.info(f"  [{self.bib}] {len(resultados)} pgs "
                                 f"(#{global_page}, {pasta_antes})")

            except Exception as e:
                logger.error(f"Erro na página {global_page} ({pasta_antes}): {e}")

            # Avançar para próxima página
            self._proxima_pagina()

            # Checar mudança de edição via JS rápido
            try:
                new_state = self.driver.execute_script("""
                    var p = document.getElementById('PastaTxt');
                    var pg = document.getElementById('PagAtualTxt');
                    return {
                        pasta: p ? (p.innerText || p.title || '').trim() : '',
                        pagAtual: pg ? parseInt(pg.value) || 1 : 1
                    };
                """)
                pasta_depois = new_state["pasta"]
                pag_depois = new_state["pagAtual"]
            except Exception:
                pasta_depois = pasta_antes
                pag_depois = pag_antes

            if pasta_depois == pasta_antes and pag_depois == pag_antes:
                total_ed = self._get_total_paginas()
                if pag_antes >= total_ed:
                    stale_count += 1
                    if stale_count >= max_stale:
                        logger.info(f"Fim do acervo em '{pasta_antes}' pag {pag_antes}")
                        break
                else:
                    stale_count = 0
            else:
                stale_count = 0
                if pasta_depois != pasta_antes:
                    logger.info(f"  Nova edição: '{pasta_depois}'")
                    # Atualizar headers após mudança de edição
                    self._refresh_http_headers()

            global_page += 1

            if global_page % 50 == 0:
                self._resolver_captcha()
                # Limpar futures já concluídas
                self._download_futures = [f for f in self._download_futures if not f.done()]

        # Salvar cache final e aguardar downloads pendentes
        self._save_cache()
        self._flush_downloads()
        self._download_pool.shutdown(wait=False)

        logger.info(f"Jornal {self.nome}: {len(resultados)} novas páginas "
                     f"(global: {global_page})")
        return resultados

    def _captcha_visivel(self) -> bool:
        try:
            iframes = self.driver.find_elements(By.NAME, "CaptchaWnd")
            if not iframes:
                return False
            parent = iframes[0].find_element(By.XPATH, "./..")
            return parent.is_displayed()
        except Exception:
            return False

    def _resolver_captcha(self, max_tentativas=5, timeout_manual=300):
        """Detecta e tenta resolver o CAPTCHA automaticamente via OCR."""
        if not self._captcha_visivel():
            return True

        for tentativa in range(max_tentativas):
            if not self._captcha_visivel():
                logger.info("CAPTCHA resolvido!")
                return True

            logger.info(f"CAPTCHA detectado, tentativa {tentativa + 1}/{max_tentativas}...")

            try:
                iframe = self.driver.find_element(By.NAME, "CaptchaWnd")
                self.driver.switch_to.frame(iframe)
                time.sleep(0.5)

                captcha_img = self.driver.find_element(By.ID, "RadCaptcha1_CaptchaImageUP")

                # Pegar imagem via screenshot do elemento (bypassa Cloudflare 403)
                try:
                    img_data = captcha_img.screenshot_as_png
                except Exception as e:
                    logger.warning(f"Screenshot captcha falhou: {e}")
                    self.driver.switch_to.default_content()
                    continue

                texto_captcha = self._ocr_captcha(img_data)
                if not texto_captcha:
                    logger.warning("OCR falhou no CAPTCHA, refreshing...")
                    try:
                        refresh = self.driver.find_element(By.ID, "RadCaptcha1_CaptchaLinkButton")
                        self.driver.execute_script("arguments[0].click();", refresh)
                        time.sleep(1.5)
                    except Exception:
                        pass
                    self.driver.switch_to.default_content()
                    continue

                logger.info(f"OCR leu CAPTCHA: '{texto_captcha}'")

                self.driver.execute_script(
                    "var el = document.getElementById('RadCaptcha1_CaptchaTextBox');"
                    "if (el) { el.value = arguments[0]; el.dispatchEvent(new Event('change')); }",
                    texto_captcha
                )
                time.sleep(0.3)

                self.driver.execute_script(
                    "document.getElementById('EnviarBtn_input').click();"
                )

                self.driver.switch_to.default_content()
                time.sleep(2)

                if not self._captcha_visivel():
                    logger.info("CAPTCHA resolvido automaticamente!")
                    self._refresh_http_headers()
                    return True

                logger.warning("CAPTCHA incorreto, tentando novamente...")

            except Exception as e:
                logger.warning(f"Erro ao resolver CAPTCHA: {e}")
                try:
                    self.driver.switch_to.default_content()
                except Exception:
                    pass

        logger.warning("Auto-solve falhou. Resolva o CAPTCHA manualmente...")
        for _ in range(timeout_manual):
            time.sleep(1)
            if not self._captcha_visivel():
                logger.info("CAPTCHA resolvido manualmente!")
                self._refresh_http_headers()
                return True

        logger.error("Timeout esperando CAPTCHA")
        return False

    def _ocr_captcha(self, img_data: bytes) -> str:
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

    def _fechar_dialog_copyright(self):
        try:
            iframes = self.driver.find_elements(By.NAME, "PesqOpniaoRadWindow")
            if iframes:
                parent = iframes[0].find_element(By.XPATH, "./../../..")
                close_btns = parent.find_elements(By.CSS_SELECTOR, "a.rwCloseButton, a[title='Close']")
                if close_btns:
                    self.driver.execute_script("arguments[0].click();", close_btns[0])
                    time.sleep(0.5)
                    return

            self.driver.execute_cdp_cmd("Runtime.evaluate", {
                "expression": "var wnd = $find('PesqOpniaoRadWindow'); if (wnd) wnd.close();",
                "returnByValue": True,
            })
            time.sleep(0.5)
        except Exception:
            pass

    def _get_total_paginas(self) -> int:
        try:
            total = self.driver.execute_script("""
                var el = document.getElementById('PagTotalLbl');
                if (el) {
                    var m = el.innerText.match(/\\/(\\d+)/);
                    if (m) return parseInt(m[1]);
                }
                return 0;
            """)
            return total or 0
        except Exception:
            return 0

    def _navegar_para_pagina(self, num_pagina: int):
        try:
            from selenium.webdriver.common.keys import Keys
            page_input = self.driver.find_element(By.ID, "PagAtualTxt")
            page_input.clear()
            page_input.send_keys(str(num_pagina))
            page_input.send_keys(Keys.RETURN)
            time.sleep(1)
            self._aguardar_carregamento()
        except Exception as e:
            logger.warning(f"Erro ao navegar para página {num_pagina}: {e}")

    def _proxima_pagina(self):
        try:
            self.driver.execute_script(
                "var btn = document.getElementById('PagPosBtn');"
                "if (btn) btn.click();"
            )
            time.sleep(CLICK_PAUSE)
            self._aguardar_carregamento()
        except Exception as e:
            logger.warning(f"Erro ao avançar página: {e}")

    def _aguardar_carregamento(self, timeout: int = 10):
        try:
            els = self.driver.find_elements(By.ID, "updateprogressloaddiv")
            if els and els[0].is_displayed():
                WebDriverWait(self.driver, timeout).until(
                    EC.invisibility_of_element_located((By.ID, "updateprogressloaddiv"))
                )
        except (TimeoutException, Exception):
            pass


def scrape_jornal(driver, bib: str, nome: str, max_pages: int = 0) -> list[dict]:
    """Função de conveniência para scraping de um jornal."""
    scraper = JornalScraper(driver, bib, nome)
    return scraper.scrape_todas_paginas(max_pages=max_pages)
