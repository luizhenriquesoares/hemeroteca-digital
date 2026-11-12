"""OCR local conservador: escala explícita, sem modernizar ou completar palavras."""
from pathlib import Path

from PIL import Image, ImageOps


def transcribe(image_path: Path, *, scale: float = 0.5) -> dict:
    import tesserocr
    from src.config import TESSDATA_DIR

    if not 0.1 <= scale <= 1:
        raise ValueError("A escala deve estar entre 0.1 e 1")
    with Image.open(image_path) as source:
        gray = ImageOps.exif_transpose(source).convert("L")
    original_size = gray.size
    # Do not shrink small scans: the half-scale experiment concerns hi-res pages.
    effective_scale = scale if max(gray.size) >= 5000 else 1.0
    if effective_scale != 1:
        gray = gray.resize(tuple(max(1, round(v * effective_scale)) for v in gray.size),
                           Image.Resampling.LANCZOS)
    text = tesserocr.image_to_text(gray, lang="por", path=str(TESSDATA_DIR), psm=3)
    return {"page": image_path.stem, "config": "tesseract-gray", "mode": "full",
            "backend": "tesseract", "scale": effective_scale,
            "source_size": list(original_size), "ocr_size": list(gray.size),
            "extraction": {"segments": [{"text": text}]},
            "note": "OCR bruto; sem validação de entidades ou correção por modelo."}
