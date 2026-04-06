"""Helpers de interação com RadComboBox/Telerik na busca de acervos."""

from __future__ import annotations

import logging
import time

from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)


def eval_js(driver, code):
    """Executa JS via CDP Runtime.evaluate para evitar strict mode do Chrome."""
    expr = code.strip()
    if "return " in expr and not expr.startswith("(function"):
        expr = f"(function() {{ {expr} }})()"

    result = driver.execute_cdp_cmd(
        "Runtime.evaluate",
        {
            "expression": expr,
            "returnByValue": True,
            "awaitPromise": False,
        },
    )
    if "exceptionDetails" in result:
        msg = result["exceptionDetails"].get("text", "Unknown JS error")
        exc = result["exceptionDetails"].get("exception", {})
        desc = exc.get("description", msg)
        raise Exception(f"JS Error: {desc}")
    return result.get("result", {}).get("value")


def aguardar_ajax(driver, timeout=20):
    for _ in range(timeout * 2):
        try:
            in_progress = eval_js(
                driver,
                "try { return Sys.WebForms.PageRequestManager.getInstance()"
                ".get_isInAsyncPostBack(); } catch(e) { return false; }",
            )
            if not in_progress:
                overlays = driver.find_elements(By.CSS_SELECTOR, ".raDiv")
                visible = any(overlay.is_displayed() for overlay in overlays)
                if not visible:
                    return
        except Exception:
            pass
        time.sleep(0.5)
    logger.warning("Timeout %ss aguardando AJAX", timeout)


def telerik_select(driver, combo_id, valor, timeout=15):
    aguardar_ajax(driver)

    js_code = (
        "(function() {"
        f"  var combo = $find('{combo_id}');"
        "  if (!combo) return 'COMBO_NOT_FOUND';"
        "  if (!combo.get_enabled()) return 'COMBO_DISABLED';"
        "  var items = combo.get_items();"
        "  if (!items || items.get_count() === 0) return 'NO_ITEMS';"
        "  var found = null;"
        "  var available = [];"
        "  for (var i = 0; i < items.get_count(); i++) {"
        "    var text = items.getItem(i).get_text().trim();"
        "    available.push(text);"
        f"    if (text === '{valor}') found = items.getItem(i);"
        "  }"
        "  if (!found) return 'NOT_FOUND:' + available.join('|');"
        "  found.select();"
        "  combo.hideDropDown();"
        "  return 'OK';"
        "})()"
    )
    result = eval_js(driver, js_code)

    if result == "OK":
        time.sleep(1)
        aguardar_ajax(driver)
        return True
    if result == "COMBO_DISABLED":
        for _ in range(timeout):
            time.sleep(1)
            aguardar_ajax(driver)
            enabled = eval_js(driver, f"var c=$find('{combo_id}'); return c ? c.get_enabled() : false;")
            if enabled:
                return telerik_select(driver, combo_id, valor, timeout=5)
        raise Exception(f"{combo_id} continua desabilitado após {timeout}s")
    if result and result.startswith("NOT_FOUND:"):
        available = result.split(":", 1)[1].split("|")
        raise Exception(f"'{valor}' não encontrado em {combo_id}. Disponíveis: {available}")
    raise Exception(f"Erro ao selecionar {valor} em {combo_id}: {result}")


def telerik_get_items(driver, combo_id) -> list[str]:
    items = eval_js(
        driver,
        "(function() {"
        f"  var combo = $find('{combo_id}');"
        "  if (!combo) return [];"
        "  var items = combo.get_items();"
        "  if (!items) return [];"
        "  var result = [];"
        "  for (var i = 0; i < items.get_count(); i++) {"
        "    var text = items.getItem(i).get_text();"
        "    if (text && text.trim()) result.push(text.trim());"
        "  }"
        "  return result;"
        "})()",
    )
    return items or []


def clicar_aba_local(driver):
    eval_js(
        driver,
        "(function() {"
        "  var ts = $find('RadTabStrip1');"
        "  if (ts) {"
        "    var tab = ts.get_tabs().getTab(2);"
        "    if (tab) tab.click();"
        "  }"
        "})()",
    )
    time.sleep(2)
    aguardar_ajax(driver)
