import importlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

provider = importlib.import_module("src.correcao_provider")
llm_openai = importlib.import_module("src.llm_correcao")


class CorrecaoProviderTests(unittest.TestCase):
    def test_list_pending_files_ignora_corrigidos(self):
        with tempfile.TemporaryDirectory() as tmp:
            text_dir = Path(tmp) / "text" / "029033_02"
            text_dir.mkdir(parents=True)
            (text_dir / "a.txt").write_text("x", encoding="utf-8")
            (text_dir / "a_corrigido.txt").write_text("y", encoding="utf-8")
            (text_dir / "b.txt").write_text("x", encoding="utf-8")

            with patch("src.correcao_provider.TEXT_DIR", Path(tmp) / "text"):
                files = provider.list_pending_files(bib="029033_02", force=False)

            self.assertEqual([f.name for f in files], ["b.txt"])

    def test_corrigir_arquivo_dispatch_openai(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "x.txt"
            p.write_text("abc", encoding="utf-8")
            with patch("src.llm_correcao.corrigir_arquivo", return_value=True) as mocked:
                ok = provider.corrigir_arquivo(p, provider="openai", model="gpt-test", force=True)
            self.assertTrue(ok)
            mocked.assert_called_once()

    def test_corrigir_arquivo_dispatch_claude(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "x.txt"
            p.write_text("abc", encoding="utf-8")
            with patch("src.llm_correcao_claude.corrigir_arquivo", return_value=True) as mocked:
                ok = provider.corrigir_arquivo(p, provider="claude", model="opus", force=True)
            self.assertTrue(ok)
            mocked.assert_called_once()

    def test_corrigir_texto_dispatch_openai(self):
        with patch("src.llm_correcao.corrigir_texto_ocr", return_value="ok") as mocked:
            text = provider.corrigir_texto("abc", provider="openai", model="gpt-test")
        self.assertEqual(text, "ok")
        mocked.assert_called_once()

    def test_openai_gpt5_uses_max_completion_tokens(self):
        captured = {}

        class FakeCompletions:
            def create(self, **kwargs):
                captured.update(kwargs)
                class Msg:
                    content = "corrigido"
                class Choice:
                    message = Msg()
                class Resp:
                    choices = [Choice()]
                return Resp()

        class FakeChat:
            completions = FakeCompletions()

        class FakeClient:
            chat = FakeChat()

        out = llm_openai._corrigir_parte(FakeClient(), "texto", "gpt-5", 1234)
        self.assertEqual(out, "corrigido")
        self.assertEqual(captured["max_completion_tokens"], 1234)
        self.assertNotIn("max_tokens", captured)

    def test_openai_4o_uses_max_tokens(self):
        captured = {}

        class FakeCompletions:
            def create(self, **kwargs):
                captured.update(kwargs)
                class Msg:
                    content = "corrigido"
                class Choice:
                    message = Msg()
                class Resp:
                    choices = [Choice()]
                return Resp()

        class FakeChat:
            completions = FakeCompletions()

        class FakeClient:
            chat = FakeChat()

        out = llm_openai._corrigir_parte(FakeClient(), "texto", "gpt-4o-mini", 4321)
        self.assertEqual(out, "corrigido")
        self.assertEqual(captured["max_tokens"], 4321)
        self.assertNotIn("max_completion_tokens", captured)


if __name__ == "__main__":
    unittest.main()
