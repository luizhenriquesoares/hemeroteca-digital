from pathlib import Path
from PIL import Image
import pytest

from src.vision.local_ocr import transcribe
from src.scraping.driver import installed_chrome_major


def test_small_scan_is_not_reduced(tmp_path, monkeypatch):
    import tesserocr
    path = tmp_path / "scan.png"
    Image.new("L", (400, 600)).save(path)
    monkeypatch.setattr(tesserocr, "image_to_text", lambda image, **kw: f"{image.size}", raising=False)
    result = transcribe(path)
    assert result["ocr_size"] == [400, 600]
    assert result["scale"] == 1


def test_invalid_scale_rejected():
    with pytest.raises(ValueError):
        transcribe(Path("missing.png"), scale=0)


def test_browser_version_follows_installation(monkeypatch):
    import subprocess
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **kw: "Google Chrome 152.0.7977.83")
    assert installed_chrome_major("/chrome") == 152
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **kw: "unknown")
    with pytest.raises(RuntimeError):
        installed_chrome_major("/chrome")
