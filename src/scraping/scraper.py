"""Módulo principal de scraping: navega páginas dos jornais e baixa imagens."""

import json
import logging
import time
from pathlib import Path

from selenium.webdriver.common.by import By

from src.config import (
    HDB_BASE_URL,
    HDB_DOCREADER_URL,
    IMAGES_DIR,
    CACHE_DIR,
    CLICK_PAUSE,
)
from src.scraping.driver import human_delay
from src.scraping.captcha import captcha_visivel, resolver_captcha
from src.scraping.scraper_support import (
    aguardar_carregamento as _aguardar_carregamento_impl,
    create_download_pool,
    download_image_task as _download_image_task,
    fechar_dialog_copyright as _fechar_dialog_copyright_impl,
    flush_downloads as _flush_downloads_impl,
    get_pagina_atual_num as _get_pagina_atual_num_impl,
    get_pasta_atual as _get_pasta_atual_impl,
    get_state_js as _get_state_js_impl,
    get_total_paginas as _get_total_paginas_impl,
    navegar_para_pagina as _navegar_para_pagina_impl,
    proxima_pagina as _proxima_pagina_impl,
)

logger = logging.getLogger(__name__)

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
        self._download_pool = create_download_pool(max_workers=3)
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
        _flush_downloads_impl(self._download_futures)
        self._download_futures.clear()

    def _get_pasta_atual(self) -> str:
        return _get_pasta_atual_impl(self.driver)

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
        return _get_pagina_atual_num_impl(self.driver)

    def _get_state_js(self):
        """Obtém todo o estado da página em uma única chamada JS."""
        return _get_state_js_impl(self.driver)

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
        return captcha_visivel(self.driver)

    def _resolver_captcha(self, max_tentativas=5, timeout_manual=300):
        """Detecta e tenta resolver o CAPTCHA. Refresh headers HTTP após sucesso."""
        return resolver_captcha(
            self.driver,
            max_tentativas=max_tentativas,
            timeout_manual=timeout_manual,
            on_success=self._refresh_http_headers,
        )

    def _fechar_dialog_copyright(self):
        _fechar_dialog_copyright_impl(self.driver)

    def _get_total_paginas(self) -> int:
        return _get_total_paginas_impl(self.driver)

    def _navegar_para_pagina(self, num_pagina: int):
        _navegar_para_pagina_impl(self.driver, num_pagina, self._aguardar_carregamento)

    def _proxima_pagina(self):
        _proxima_pagina_impl(self.driver, CLICK_PAUSE, self._aguardar_carregamento)

    def _aguardar_carregamento(self, timeout: int = 10):
        _aguardar_carregamento_impl(self.driver, timeout=timeout)


def scrape_jornal(driver, bib: str, nome: str, max_pages: int = 0) -> list[dict]:
    """Função de conveniência para scraping de um jornal."""
    scraper = JornalScraper(driver, bib, nome)
    return scraper.scrape_todas_paginas(max_pages=max_pages)
