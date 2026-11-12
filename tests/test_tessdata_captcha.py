"""OCR do CAPTCHA: acha o tessdata do sistema e falha rápido quando o Tesseract não inicializa."""

from pathlib import Path

import pytest

import src.config as config
from src.scraping import captcha


def _modelos(pasta: Path) -> Path:
    pasta.mkdir(parents=True)
    for lang in config.TESSDATA_LANGS:
        (pasta / f"{lang}.traineddata").write_bytes(b"x")
    return pasta


def test_tessdata_local_vazio_cai_no_do_sistema(tmp_path, monkeypatch):
    sistema = _modelos(tmp_path / "sistema")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setenv("TESSDATA_PREFIX", str(sistema))
    assert config._resolver_tessdata() == sistema


def test_tessdata_local_completo_tem_prioridade(tmp_path, monkeypatch):
    local = _modelos(tmp_path / "data" / "tessdata")
    _modelos(tmp_path / "sistema")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setenv("TESSDATA_PREFIX", str(tmp_path / "sistema"))
    assert config._resolver_tessdata() == local


def test_ocr_sem_tessdata_levanta_erro_de_instalacao(tmp_path, monkeypatch):
    import io

    from PIL import Image

    class FakeTess:  # o mesmo erro que o tesserocr dá com a pasta errada
        class PSM:
            SINGLE_WORD = 8

        @staticmethod
        def image_to_text(*a, **k):
            raise RuntimeError(f"Failed to init API, possibly an invalid tessdata path: {k.get('path')}")

    monkeypatch.setattr(captcha, "tesserocr", FakeTess)
    buf = io.BytesIO()
    Image.new("L", (40, 20), 255).save(buf, format="PNG")
    with pytest.raises(captcha.OCRIndisponivel):
        captcha.ocr_captcha(buf.getvalue())


def test_ocr_le_imagem_com_tessdata_real():
    import io

    if not hasattr(captcha.tesserocr, "image_to_text"):
        pytest.skip("tesserocr substituído por stub em outro teste")

    from PIL import Image

    buf = io.BytesIO()
    Image.new("L", (40, 20), 255).save(buf, format="PNG")
    assert captcha.ocr_captcha(buf.getvalue()) == ""  # imagem em branco: lê nada, sem exceção
