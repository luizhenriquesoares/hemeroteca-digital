from src.vision.ocr_consensus import align_readings, review_html


def words(*values):
    return [{'text': v, 'box': [0, 0, 10, 10]} for v in values]


def test_insertions_and_deletions_are_reviewable():
    delta = align_readings(words('Diario', 'de', '1825'), words('Diario', '1825', 'novo'))
    assert [d['kind'] for d in delta] == ['delete', 'insert']
    assert delta[0]['base'][0]['text'] == 'de'
    assert delta[1]['alternative'][0]['text'] == 'novo'


def test_html_escapes_document_text():
    result = {'page': '<script>', 'differences': [], 'extraction': {'segments': [{'text': '<img onerror=alert(1)>'}]}}
    html = review_html(result)
    assert '<script>' not in html
    assert '&lt;img onerror=' in html
