import unittest
from unittest.mock import patch

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
