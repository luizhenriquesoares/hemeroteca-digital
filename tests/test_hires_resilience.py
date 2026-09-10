import unittest
from unittest.mock import patch

from src.scraping import captcha as scraping_captcha
from src.scraping import driver as scraping_driver
from src.scraping import hires_docreader, hires_orchestrator


class _FakeDriver:
    def __init__(self, src=None):
        self.src = src
        self.captcha_visible = False

    def execute_script(self, script, *args):
        if "DocumentoImg" in script:
            return self.src
        return None


class _DummyDriver:
    def quit(self):
        return None


class _ChromeDriverStub:
    def __init__(self):
        self.page_load_timeout = None
        self.implicit_wait_timeout = None
        self.cdp_calls = []

    def set_page_load_timeout(self, timeout):
        self.page_load_timeout = timeout

    def implicitly_wait(self, timeout):
        self.implicit_wait_timeout = timeout

    def execute_cdp_cmd(self, command, params):
        self.cdp_calls.append((command, params))
        return {}


class _SetupDriver:
    def __init__(self):
        self.urls = []

    def get(self, url):
        self.urls.append(url)


class _CaptchaParentElement:
    def __init__(self, driver):
        self.driver = driver

    def is_displayed(self):
        return self.driver.visible


class _CaptchaFrameElement:
    def __init__(self, driver):
        self.driver = driver

    def find_element(self, by, value):
        if by == "xpath" and value == "./..":
            return _CaptchaParentElement(self.driver)
        raise AssertionError(f"Unexpected nested lookup: {by=} {value=}")


class _CaptchaImageElement:
    screenshot_as_png = b"fake-captcha"


class _CaptchaRefreshElement:
    pass


class _CaptchaSwitchTo:
    def frame(self, _frame):
        return None

    def default_content(self):
        return None


class _CaptchaDriver:
    def __init__(self, success_text):
        self._codex_headless = True
        self.success_text = success_text
        self.visible = True
        self.entered_text = None
        self.submit_calls = 0
        self.switch_to = _CaptchaSwitchTo()

    def find_elements(self, by, value):
        if by == "name" and value == "CaptchaWnd" and self.visible:
            return [_CaptchaFrameElement(self)]
        return []

    def find_element(self, by, value):
        if by == "name" and value == "CaptchaWnd" and self.visible:
            return _CaptchaFrameElement(self)
        if by == "id" and value == "RadCaptcha1_CaptchaImageUP":
            return _CaptchaImageElement()
        if by == "id" and value == "RadCaptcha1_CaptchaLinkButton":
            return _CaptchaRefreshElement()
        raise AssertionError(f"Unexpected lookup: {by=} {value=}")

    def execute_script(self, script, *args):
        if "RadCaptcha1_CaptchaTextBox" in script:
            self.entered_text = args[0]
            return None
        if "EnviarBtn_input" in script:
            self.submit_calls += 1
            if self.entered_text == self.success_text:
                self.visible = False
            return None
        return None


class HiresDocreaderResilienceTests(unittest.TestCase):
    def test_wait_for_cache_url_resolve_captcha_durante_espera(self):
        driver = _FakeDriver()
        driver.captcha_visible = True
        resolved = []

        def resolver():
            resolved.append(True)
            driver.captcha_visible = False
            driver.src = "https://memoria.bn.gov.br/cache/p00042.jpg"
            return True

        src = hires_docreader.wait_for_cache_url(
            driver,
            old_src="https://memoria.bn.gov.br/cache/p00041.jpg",
            timeout=0.05,
            captcha_visible_fn=lambda d: d.captcha_visible,
            captcha_resolve_fn=resolver,
            poll_interval=0.001,
        )

        self.assertEqual(src, "https://memoria.bn.gov.br/cache/p00042.jpg")
        self.assertEqual(len(resolved), 1)

    def test_wait_for_cache_url_forca_refresh_quando_cache_nao_muda(self):
        driver = _FakeDriver(src="https://memoria.bn.gov.br/cache/p00041.jpg")
        refreshed = []

        def refresh():
            refreshed.append(True)
            driver.src = "https://memoria.bn.gov.br/cache/p00042.jpg"

        src = hires_docreader.wait_for_cache_url(
            driver,
            old_src="https://memoria.bn.gov.br/cache/p00041.jpg",
            timeout=0.05,
            refresh_fn=refresh,
            poll_interval=0.001,
        )

        self.assertEqual(src, "https://memoria.bn.gov.br/cache/p00042.jpg")
        self.assertEqual(len(refreshed), 1)

    def test_setup_acervo_monitora_captcha_tambem_na_espera_low_res(self):
        driver = _SetupDriver()

        with patch.object(hires_docreader, "resolver_captcha", return_value=True), \
             patch.object(hires_docreader, "fechar_dialog"), \
             patch.object(hires_docreader, "refresh_hires_view"), \
             patch.object(hires_docreader, "wait_for_cache_url", side_effect=["low-res", "hi-res"]) as wait_mock, \
             patch.object(hires_docreader.time, "sleep", lambda *_: None):
            low_res, hi_res = hires_docreader.setup_acervo(
                driver,
                bib="029033_01",
                nome="Diario de Pernambuco",
                hires_size="6464x8940",
                hdb_docreader_url="https://memoria.bn.gov.br/docreader/docreader.aspx",
                start_page=1564,
                acervo_setup_retries=1,
                acervo_setup_sleep=0,
            )

        self.assertEqual(low_res, "low-res")
        self.assertEqual(hi_res, "hi-res")
        first_wait = wait_mock.call_args_list[0]
        self.assertIn("captcha_visible_fn", first_wait.kwargs)
        self.assertIn("captcha_resolve_fn", first_wait.kwargs)
        self.assertEqual(driver.urls[0], "https://memoria.bn.gov.br/docreader/docreader.aspx?bib=029033_01")


class DriverStealthTests(unittest.TestCase):
    def test_create_driver_headless_remove_headlesschrome_do_user_agent(self):
        driver_stub = _ChromeDriverStub()
        captured = {}

        def fake_chrome(*, options, use_subprocess, version_main):
            captured["options"] = options
            captured["use_subprocess"] = use_subprocess
            captured["version_main"] = version_main
            return driver_stub

        with patch.object(scraping_driver.uc, "Chrome", side_effect=fake_chrome):
            driver = scraping_driver.create_driver(headless=True)

        self.assertIs(driver, driver_stub)
        args = getattr(captured["options"], "arguments", [])
        self.assertIn("--headless=new", args)
        self.assertIn("--lang=pt-BR", args)
        self.assertIn("--disable-blink-features=AutomationControlled", args)
        self.assertTrue(driver._codex_headless)
        ua_arg = next(arg for arg in args if arg.startswith("--user-agent="))
        self.assertIn("Chrome/146.0.0.0", ua_arg)
        self.assertNotIn("HeadlessChrome", ua_arg)
        self.assertIn(
            ("Network.setUserAgentOverride", unittest.mock.ANY),
            driver_stub.cdp_calls,
        )


class CaptchaHeadlessTests(unittest.TestCase):
    def test_resolver_captcha_headless_continua_no_automatico_apos_limite_inicial(self):
        driver = _CaptchaDriver(success_text="840")
        ocr_results = iter(["", "", "840"])

        with patch.object(scraping_captcha, "ocr_captcha", side_effect=lambda *_: next(ocr_results)), \
             patch.object(scraping_captcha.time, "sleep", lambda *_: None):
            resolved = scraping_captcha.resolver_captcha(
                driver,
                max_tentativas=2,
                timeout_manual=5,
            )

        self.assertTrue(resolved)
        self.assertFalse(driver.visible)
        self.assertEqual(driver.entered_text, "840")
        self.assertEqual(driver.submit_calls, 1)


class HiresOrchestratorStatsTests(unittest.TestCase):
    def test_processar_acervo_paralelo_consolida_stats_finais(self):
        stats_calls = []
        mark_done_calls = []

        with patch.object(hires_orchestrator, "get_total_pages", return_value=10), \
             patch.object(hires_orchestrator.time, "sleep", lambda *_: None):
            result = hires_orchestrator.processar_acervo_paralelo(
                {"bib": "029033_01", "nome": "Diario de Pernambuco"},
                workers=2,
                headless=True,
                force=True,
                max_pages=0,
                keep_images=False,
                processar_com_driver_fn=lambda *args, **kwargs: {
                    "processed": 5,
                    "skipped": 1,
                    "failed_pages": [],
                    "complete": False,
                },
                load_progress_fn=lambda: {
                    "done": [],
                    "failed_pages": {"029033_01": [3]},
                    "stats": {},
                },
                mark_done_fn=lambda bib: mark_done_calls.append(bib),
                set_bib_stats_fn=lambda bib, stats: stats_calls.append((bib, stats)),
                create_driver_fn=lambda headless=True: _DummyDriver(),
            )

        self.assertEqual(result, {})
        self.assertEqual(mark_done_calls, [])
        self.assertEqual(len(stats_calls), 1)
        bib, stats = stats_calls[0]
        self.assertEqual(bib, "029033_01")
        self.assertEqual(stats["processed"], 10)
        self.assertEqual(stats["skipped"], 2)
        self.assertEqual(stats["failed_pages"], [3])
        self.assertFalse(stats["complete"])
        self.assertFalse(stats["running"])
        self.assertEqual(stats["workers"], 2)


if __name__ == "__main__":
    unittest.main()
