"""Módulo para buscar e listar todos os acervos (jornais) de Pernambuco na HDB."""

import json
import logging
import re
import time

from src.config import HDB_SEARCH_URL, CACHE_DIR, UF_ALVO
from src.scraping.driver import create_driver
from src.scraping.captcha import resolver_captcha
from src.scraping.acervos_telerik import (
    aguardar_ajax as _aguardar_ajax_impl,
    clicar_aba_local as _clicar_aba_local_impl,
    eval_js as _eval_js_impl,
    telerik_get_items as _telerik_get_items_impl,
    telerik_select as _telerik_select_impl,
)

logger = logging.getLogger(__name__)

ACERVOS_CACHE = CACHE_DIR / "acervos_pe.json"


def _eval_js(driver, code):
    return _eval_js_impl(driver, code)

def buscar_acervos(driver=None, headless: bool = False) -> list[dict]:
    """Busca todos os acervos de PE, iterando por todos os períodos."""
    if ACERVOS_CACHE.exists():
        logger.info("Carregando acervos do cache")
        with open(ACERVOS_CACHE) as f:
            acervos = json.load(f)
        logger.info(f"Cache: {len(acervos)} acervos de {UF_ALVO}")
        return acervos

    own_driver = driver is None
    if own_driver:
        driver = create_driver(headless=headless)

    todos_acervos = []
    seen_bibs = set()

    # Passo 1: ir à HDB, selecionar PE, e ler os períodos disponíveis
    periodos = _setup_e_ler_periodos(driver)
    logger.info(f"Períodos disponíveis para {UF_ALVO}: {len(periodos)} -> {periodos}")

    if not periodos:
        logger.error("Nenhum período encontrado! Verifique se o site está acessível.")
        if own_driver:
            driver.quit()
        return []

    # Passo 2: para cada período, selecionar e pesquisar
    for i, periodo in enumerate(periodos):
        logger.info(f"[{i+1}/{len(periodos)}] Período {periodo}...")
        try:
            acervos_periodo = _pesquisar_periodo(driver, periodo)
            novos = 0
            for a in acervos_periodo:
                if a["bib"] not in seen_bibs:
                    seen_bibs.add(a["bib"])
                    a["periodo_busca"] = periodo
                    todos_acervos.append(a)
                    novos += 1
            logger.info(f"  -> {len(acervos_periodo)} jornais, {novos} novos "
                        f"({len(todos_acervos)} únicos total)")
        except Exception as e:
            logger.error(f"  Erro no período {periodo}: {e}")
            # Recriar driver e continuar
            try:
                driver.quit()
            except Exception:
                pass
            driver = create_driver(headless=headless)

        # Salvar cache parcial a cada período
        if todos_acervos:
            with open(ACERVOS_CACHE, "w", encoding="utf-8") as f:
                json.dump(todos_acervos, f, ensure_ascii=False, indent=2)

    logger.info(f"Total de acervos únicos de {UF_ALVO}: {len(todos_acervos)}")

    if own_driver:
        driver.quit()

    return todos_acervos


def _aguardar_ajax(driver, timeout=20):
    _aguardar_ajax_impl(driver, timeout=timeout)


def _telerik_select(driver, combo_id, valor, timeout=15):
    return _telerik_select_impl(driver, combo_id, valor, timeout=timeout)


def _telerik_get_items(driver, combo_id) -> list[str]:
    return _telerik_get_items_impl(driver, combo_id)


def _clicar_aba_local(driver):
    _clicar_aba_local_impl(driver)


def _setup_e_ler_periodos(driver) -> list[str]:
    """Navega à HDB, seleciona aba Local + PE, retorna períodos disponíveis."""
    driver.get(HDB_SEARCH_URL)
    time.sleep(5)

    # CAPTCHA pode aparecer ao entrar na página
    resolver_captcha(driver, max_tentativas=5, timeout_manual=300)

    # Aba Local (3a)
    _clicar_aba_local(driver)

    # Selecionar PE
    _telerik_select(driver, "UFCmb3", UF_ALVO)
    time.sleep(3)
    _aguardar_ajax(driver)

    # CAPTCHA pode aparecer após o postback do combo UF
    resolver_captcha(driver, max_tentativas=5, timeout_manual=300)

    # Aguardar PeriodoCmb3 ficar habilitado e populado
    for _ in range(20):
        enabled = _eval_js(driver,
            "var c=$find('PeriodoCmb3'); return c ? c.get_enabled() : false;"
        )
        if enabled:
            break
        time.sleep(1)
    else:
        logger.warning("PeriodoCmb3 não habilitou após selecionar PE")

    time.sleep(1)

    # Ler períodos disponíveis para PE (filtrar "Todos" e "n.i.")
    periodos = _telerik_get_items(driver, "PeriodoCmb3")
    periodos = [p for p in periodos
                if not p.lower().startswith("todos") and p != "n.i."]
    return periodos


def _pesquisar_periodo(driver, periodo: str) -> list[dict]:
    """
    Navega à HDB, seleciona PE + período, pesquisa, e extrai resultados.
    Cada chamada faz a navegação completa para garantir estado limpo.
    """
    driver.get(HDB_SEARCH_URL)
    time.sleep(5)

    # CAPTCHA pode aparecer ao entrar na página
    resolver_captcha(driver, max_tentativas=5, timeout_manual=300)

    # Aba Local
    _clicar_aba_local(driver)

    # Selecionar PE
    _telerik_select(driver, "UFCmb3", UF_ALVO)
    time.sleep(3)
    _aguardar_ajax(driver)

    # CAPTCHA pode aparecer após o postback do combo UF
    resolver_captcha(driver, max_tentativas=5, timeout_manual=300)

    # Aguardar PeriodoCmb3 ficar habilitado
    for _ in range(20):
        enabled = _eval_js(driver,
            "var c=$find('PeriodoCmb3'); return c ? c.get_enabled() : false;"
        )
        if enabled:
            break
        time.sleep(1)

    # Selecionar período
    _telerik_select(driver, "PeriodoCmb3", periodo)
    time.sleep(3)
    _aguardar_ajax(driver)

    # CAPTCHA pode aparecer após selecionar período
    resolver_captcha(driver, max_tentativas=5, timeout_manual=300)

    # Aguardar botão Pesquisar ficar habilitado
    for _ in range(15):
        btn_enabled = _eval_js(driver,
            "var btn = $find('PesquisarBtn3'); "
            "return btn ? btn.get_enabled() : false;"
        )
        if btn_enabled:
            break
        time.sleep(1)

    # Clicar Pesquisar via Selenium (mouse real — JS .click() não dispara submit)
    btn = driver.find_element(By.ID, "PesquisarBtn3")
    btn.click()
    time.sleep(3)

    # CAPTCHA pode aparecer após clicar Pesquisar (bloqueia abertura da janela)
    resolver_captcha(driver, max_tentativas=5, timeout_manual=300)

    # Aguardar nova janela abrir
    janela_principal = driver.window_handles[0]
    for _ in range(30):
        if len(driver.window_handles) > 1:
            break
        time.sleep(1)
    else:
        logger.warning(f"  Nenhuma janela de resultados para {periodo}")
        return []

    # Trocar para janela de resultados
    driver.switch_to.window(driver.window_handles[-1])
    time.sleep(3)

    # Verificar CAPTCHA / aguardar resultados
    _aguardar_resultados(driver)

    # Extrair da tabela
    acervos = _extrair_tabela(driver)

    # Fechar janela e voltar
    try:
        driver.close()
    except Exception:
        pass
    try:
        driver.switch_to.window(janela_principal)
    except Exception:
        pass
    time.sleep(1)

    return acervos


def _aguardar_resultados(driver, timeout=300):
    """Aguarda resultados carregarem. Tenta resolver CAPTCHA automaticamente."""
    page = driver.page_source.lower()
    if "rgrow" in page or "bibmaisbutton" in page:
        return

    # Tentar auto-resolver CAPTCHA (com fallback manual)
    resolver_captcha(driver, max_tentativas=5, timeout_manual=timeout)

    # Após CAPTCHA, aguardar resultados aparecerem
    for _ in range(30):
        time.sleep(1)
        try:
            page = driver.page_source.lower()
            if "rgrow" in page or "bibmaisbutton" in page:
                logger.info("Resultados carregados!")
                time.sleep(2)
                return
        except Exception:
            return

    logger.error("Timeout aguardando resultados após CAPTCHA")


def _extrair_tabela(driver) -> list[dict]:
    """Extrai bibs e metadados da tabela de resultados."""
    acervos = []
    rows = driver.find_elements(By.CSS_SELECTOR, "tr.rgRow, tr.rgAltRow")

    for row in rows:
        try:
            tds = row.find_elements(By.TAG_NAME, "td")
            if len(tds) < 3:
                continue

            nome = tds[0].text.strip()
            paginas = tds[1].text.strip()

            try:
                bib_img = row.find_element(By.ID, "BibMaisButton")
            except NoSuchElementException:
                continue

            onmouseover = bib_img.get_attribute("onmouseover") or ""
            match = re.search(r"showMenu\(event,\s*[\"']([^\"']+)[\"']", onmouseover)
            if not match:
                continue

            acervos.append({
                "bib": match.group(1),
                "nome": nome,
                "paginas": int(paginas) if paginas.isdigit() else 0,
            })
        except Exception as e:
            logger.debug(f"  Erro ao extrair linha: {e}")

    return acervos


def listar_acervos():
    """Lista os acervos salvos no cache."""
    if not ACERVOS_CACHE.exists():
        print("Nenhum acervo em cache. Execute 'python main.py listar' primeiro.")
        return

    with open(ACERVOS_CACHE) as f:
        acervos = json.load(f)

    total_paginas = sum(a.get("paginas", 0) for a in acervos)
    print(f"\nAcervos de {UF_ALVO}: {len(acervos)} jornais | {total_paginas:,} páginas total\n")
    for i, a in enumerate(acervos, 1):
        print(f"  {i:3d}. [{a['bib']:>10}] {a['nome']} ({a.get('paginas', '?')} pág.)")


def limpar_cache():
    """Remove o cache de acervos."""
    if ACERVOS_CACHE.exists():
        ACERVOS_CACHE.unlink()
        print("Cache de acervos removido.")
