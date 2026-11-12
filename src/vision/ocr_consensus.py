"""Leituras em múltiplas escalas com rastreabilidade espacial das divergências."""
from difflib import SequenceMatcher
from html import escape
from pathlib import Path

from PIL import Image, ImageOps


def align_readings(base, other):
    """Align tokens in reading order; retain insertions instead of losing them."""
    matcher = SequenceMatcher(None, [w['text'] for w in base],
                              [w['text'] for w in other], autojunk=False)
    differences = []
    for tag, i, j, k, l in matcher.get_opcodes():
        if tag != 'equal':
            differences.append({'kind': tag, 'base_range': [i, j],
                                'base': base[i:j], 'alternative': other[k:l]})
    return differences


def transcribe_review(image_path: Path, *, scale=.5):
    import tesserocr
    from src.config import TESSDATA_DIR
    if not .1 <= scale <= 1:
        raise ValueError('A escala deve estar entre 0.1 e 1')
    with Image.open(image_path) as source:
        original = ImageOps.exif_transpose(source).convert('L')
    center = scale if max(original.size) >= 5000 else 1.0
    scales = list(dict.fromkeys([center, max(.1, center*.9), min(1, center*1.1)])) if center != 1 else [1.0]
    readings = []
    for factor in scales:
        size = tuple(max(1, round(v*factor)) for v in original.size)
        image = original.resize(size, Image.Resampling.LANCZOS) if size != original.size else original
        words = []
        with tesserocr.PyTessBaseAPI(path=str(TESSDATA_DIR), lang='por', psm=3) as api:
            api.SetImage(image)
            text = api.GetUTF8Text()
            iterator = api.GetIterator()
            if iterator:
                while True:
                    word = iterator.GetUTF8Text(tesserocr.RIL.WORD)
                    box = iterator.BoundingBox(tesserocr.RIL.WORD)
                    if word and box:
                        sx, sy = original.width/image.width, original.height/image.height
                        words.append({'text': word.strip(), 'confidence': round(iterator.Confidence(tesserocr.RIL.WORD), 1),
                                      'box': [round(box[0]*sx), round(box[1]*sy), round(box[2]*sx), round(box[3]*sy)]})
                    if not iterator.Next(tesserocr.RIL.WORD):
                        break
            readings.append({'scale': factor, 'text': text, 'words': words})
    differences = [{'scale': r['scale'], 'spans': align_readings(readings[0]['words'], r['words'])} for r in readings[1:]]
    return {'page': image_path.stem, 'config': 'tesseract-multipass-review', 'mode': 'full',
            'backend': 'tesseract', 'source_size': list(original.size), 'scale': center,
            'extraction': {'segments': [{'text': readings[0]['text']}]},
            'readings': readings, 'differences': differences,
            'low_confidence': [w for w in readings[0]['words'] if w['confidence'] < 65],
            'note': 'Texto principal preservado; divergências não são correções. Confiança do motor não é probabilidade de acerto.'}


def review_html(result, image_path=None):
    import base64
    import io
    source = None
    if image_path:
        with Image.open(image_path) as image:
            source = ImageOps.exif_transpose(image).convert('RGB')
    parts = ['<!doctype html><html lang="pt-BR"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Revisão do OCR</title><style>body{max-width:1000px;margin:40px auto;padding:20px;background:#faf7ef;color:#25241f;font:18px/1.6 system-ui}section{padding:18px;border:1px solid #ccc;margin:15px 0}pre{white-space:pre-wrap}small{color:#555}</style>',
             '<h1>Revisão do OCR · '+escape(result['page'])+'</h1>',
             '<p>Compare as leituras com o original. Concordância entre escalas não garante acerto; nenhuma alternativa foi aplicada automaticamente.</p>']
    for item in result['differences']:
        parts.append('<h2>Alternativas na escala '+str(round(item['scale'],3))+'</h2>')
        for span in item['spans']:
            base=' '.join(w['text'] for w in span['base']) or '[sem texto]'
            alternative=' '.join(w['text'] for w in span['alternative']) or '[sem texto]'
            boxes=[w['box'] for w in span['base'] or span['alternative']]
            parts.append('<section><b>Principal:</b> '+escape(base)+'<br><b>Alternativa:</b> '+escape(alternative)+'<br><small>Coordenadas no original: '+escape(str(boxes))+'</small></section>')
            if source and boxes:
                box=(max(0,min(b[0] for b in boxes)-20),max(0,min(b[1] for b in boxes)-20),
                     min(source.width,max(b[2] for b in boxes)+20),min(source.height,max(b[3] for b in boxes)+20))
                crop=source.crop(box)
                crop.thumbnail((1200,700))
                buffer=io.BytesIO();crop.save(buffer,format='JPEG',quality=90)
                parts.append('<img style="max-width:100%;height:auto" alt="Recorte do documento original para conferir a divergência" src="data:image/jpeg;base64,'+base64.b64encode(buffer.getvalue()).decode()+'">')
    parts.append('<h2>Texto principal</h2><pre>'+escape(result['extraction']['segments'][0]['text'])+'</pre></html>')
    return '\n'.join(parts)
