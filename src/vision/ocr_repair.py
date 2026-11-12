"""Correção experimental por consenso de caracteres em releituras de linhas.

Inspirada em reconhecimento por linhas e ensembles, não reproduz os modelos
Calamari/AAAI. Não usa gabaritos, léxicos modernos nem modelos generativos.
"""
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
import hashlib
import time
import re

from PIL import Image, ImageOps


def majority(values):
    counts = Counter(values)
    candidate, count = counts.most_common(1)[0]
    return candidate if count > len(values) / 2 else values[0]


def vote_characters(base: str, alternatives: list[str]) -> str:
    """Alinha hipóteses ao original; só maioria estrita altera um caractere.

    Empates preservam o original. Lacunas votam em inserções e exclusões.
    Releituras usam o mesmo motor: seus votos são correlacionados, não evidência
    estatística independente de correção.
    """
    slots = [[char] for char in base]
    gaps = [[''] for _ in range(len(base) + 1)]
    for alternative in alternatives:
        row = [''] * len(base)
        insertions = [''] * (len(base) + 1)
        for tag, i, j, k, l in SequenceMatcher(None, base, alternative, autojunk=False).get_opcodes():
            if tag == 'equal':
                row[i:j] = list(alternative[k:l])
            elif tag == 'insert':
                insertions[i] = alternative[k:l]
            elif tag == 'replace':
                for offset, char in enumerate(alternative[k:l]):
                    if i + offset < j:
                        row[i + offset] = char
                    else:
                        insertions[j] += char
        for index, value in enumerate(row):
            slots[index].append(value)
        for index, value in enumerate(insertions):
            gaps[index].append(value)
    return ''.join(majority(gaps[i]) + majority(slot) for i, slot in enumerate(slots)) + majority(gaps[-1])


def guarded_vote(base: str, alternatives: list[str]) -> tuple[str, str | None]:
    candidate = vote_characters(base, alternatives)
    if candidate == base:
        return base, None
    if sum(c.isalnum() for c in base) < 8:
        return base, 'fragmento_curto'
    # Do not turn a questionable date/price into a different asserted value.
    numeric = lambda text: re.findall(r'\S*\d\S*', text)
    if numeric(base) != numeric(candidate):
        return base, 'numero_protegido'
    return candidate, None


def transcribe_repaired(image_path: Path, *, scale=.5, progress=None, refine=False) -> dict:
    import tesserocr
    from src.config import TESSDATA_DIR

    if not .1 <= scale <= 1:
        raise ValueError('A escala deve estar entre 0.1 e 1')
    started = time.monotonic()
    with Image.open(image_path) as source:
        image = ImageOps.exif_transpose(source).convert('L')
    effective_scale = scale if max(image.size) >= 5000 else 1.0
    size = tuple(max(1, round(v * effective_scale)) for v in image.size)
    small = image.resize(size, Image.Resampling.LANCZOS) if size != image.size else image
    lines = []
    with tesserocr.PyTessBaseAPI(path=str(TESSDATA_DIR), lang='por', psm=3) as api:
        api.SetImage(small)
        raw_text = api.GetUTF8Text()
        iterator = api.GetIterator()
        while iterator:
            box = iterator.BoundingBox(tesserocr.RIL.TEXTLINE)
            text = iterator.GetUTF8Text(tesserocr.RIL.TEXTLINE)
            if box and text and text.strip():
                sx, sy = image.width / small.width, image.height / small.height
                lines.append({'box': [round(box[0]*sx), round(box[1]*sy), round(box[2]*sx), round(box[3]*sy)],
                              'base': text.strip(), 'confidence': iterator.Confidence(tesserocr.RIL.TEXTLINE)})
            if not iterator.Next(tesserocr.RIL.TEXTLINE):
                break
    with tesserocr.PyTessBaseAPI(path=str(TESSDATA_DIR), lang='por', psm=7) as api:
        for index, line in enumerate(lines):
            x0, y0, x1, y1 = line['box']
            crop_box = [max(0, x0-6), max(0, y0-3), min(image.width, x1+6), min(image.height, y1+3)]
            line['crop_box'] = crop_box
            crop = image.crop(crop_box)
            line['reads'] = []
            seen = set()
            for height in (40, 60, 80):
                factor = min(1, height / max(1, crop.height))
                target = (max(1, round(crop.width*factor)), max(1, round(crop.height*factor)))
                # Duplicate pixel views must not be counted as extra votes.
                if target in seen:
                    continue
                seen.add(target)
                view = crop.resize(target, Image.Resampling.LANCZOS)
                view = ImageOps.expand(view, border=10, fill=255)
                api.SetImage(view)
                text = api.GetUTF8Text().strip()
                line['reads'].append({'height': height, 'text': text, 'confidence': api.MeanTextConf()})
            # A failed/empty reread cannot erase an existing line.
            valid = [r['text'] for r in line['reads'] if r['text']]
            if len(valid) >= 2:
                line['corrected'], line['abstention'] = guarded_vote(line['base'], valid)
            else:
                line['corrected'], line['abstention'] = line['base'], 'releituras_insuficientes'
            if not line['corrected'].strip():
                line['corrected'] = line['base']
            if refine:
                import cv2
                import numpy as np
                line['consensus_v1'] = line['corrected']
                factor = min(1, 60 / max(1, crop.height))
                view = crop.resize((max(1, round(crop.width*factor)), max(1, round(crop.height*factor))), Image.Resampling.LANCZOS)
                softened = Image.fromarray(cv2.morphologyEx(np.asarray(view), cv2.MORPH_CLOSE, np.ones((2, 2), np.uint8)))
                line['refinement_reads'] = []
                for name, candidate_image, mode in [('rawline', view, 13), ('stroke', softened, 7)]:
                    api.SetPageSegMode(mode)
                    api.SetImage(ImageOps.expand(candidate_image, border=10, fill=255))
                    line['refinement_reads'].append({'name': name, 'text': api.GetUTF8Text().strip()})
                api.SetPageSegMode(7)
                candidates = [r['text'] for r in line['refinement_reads'] if r['text']]
                if len(candidates) == 2:
                    line['corrected'], line['refinement_abstention'] = guarded_vote(line['corrected'], candidates)
                if not line['corrected'].strip():
                    line['corrected'] = line['consensus_v1']
            line['changed'] = line['corrected'] != line['base']
            if progress:
                progress(index + 1, len(lines))
    corrected = '\n'.join(line['corrected'] for line in lines) if lines else raw_text
    return {'page': image_path.stem, 'config': 'tesseract-line-consensus-v2' if refine else 'tesseract-line-consensus-v1', 'mode': 'full',
            'backend': 'tesseract', 'experimental': True, 'scale': effective_scale,
            'source_size': list(image.size), 'raw_text': raw_text,
            'extraction': {'segments': [{'text': corrected}]}, 'lines': lines,
            'changed_lines': sum(line['changed'] for line in lines),
            'elapsed_s': round(time.monotonic()-started, 2),
            'provenance': {'image_sha256': hashlib.sha256(image_path.read_bytes()).hexdigest(),
                           'model_sha256': hashlib.sha256((TESSDATA_DIR/'por.traineddata').read_bytes()).hexdigest(),
                           'engine': tesserocr.tesseract_version()},
            'note': 'Correções automáticas experimentais, com votos correlacionados. Nomes e números exigem conferência. Linhas ausentes na segmentação não são recuperadas.'}


def repair_review(result, image_path):
    """Reuse the embedded source-crop review, with accurate correction labels."""
    from src.vision.ocr_consensus import review_html
    differences = []
    for line in result['lines']:
        if line['changed']:
            differences.append({'base': [{'text': line['base'], 'box': line['crop_box']}],
                                'alternative': [{'text': line['corrected'], 'box': line['crop_box']}]})
    report = {'page': result['page'], 'differences': [{'scale': result['scale'], 'spans': differences}],
              'extraction': result['extraction']}
    html = review_html(report, image_path)
    html = html.replace('Compare as leituras com o original. Concordância entre escalas não garante acerto; nenhuma alternativa foi aplicada automaticamente.',
                        'Correção automática experimental por consenso. Cada alteração abaixo pode ser comparada com os pixels originais. O arquivo .raw.txt preserva a leitura anterior. Concordância entre releituras não garante acerto.')
    html = html.replace('Alternativas na escala', 'Correções · escala da página').replace('Principal:</b>', 'Antes:</b>').replace('Alternativa:</b>', 'Depois:</b>').replace('Texto principal</h2>', 'Texto corrigido experimental</h2>')
    return html
