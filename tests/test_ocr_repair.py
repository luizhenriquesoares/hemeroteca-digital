from src.vision.ocr_repair import vote_characters, guarded_vote, majority, transcribe_repaired
from pathlib import Path
import pytest


def test_strict_majority_corrects_and_ties_preserve():
    assert vote_characters('diar1o', ['diario', 'diario', 'diario']) == 'diario'
    assert vote_characters('hum', ['um', 'um', 'hum']) == 'hum'
    assert majority(['a', 'b', 'b', 'a']) == 'a'


def test_insertions_deletions_and_accents():
    assert vote_characters('notcias', ['noticias']*3) == 'noticias'
    assert vote_characters('annnuncios', ['annuncios']*3) == 'annuncios'
    assert vote_characters('publicarao', ['publicarão']*3) == 'publicarão'
    assert vote_characters('', ['anno']*3) == 'anno'


def test_no_language_normalization_or_unsupported_change():
    original = 'hum Diario de Annuncios, anno 1825'
    assert vote_characters(original, [original]*3) == original
    assert vote_characters(original, []) == original
    assert vote_characters('1825', ['1835', '1845', '1855']) == '1825'


def test_scale_validated_before_opening_image():
    with pytest.raises(ValueError):
        transcribe_repaired(Path('missing.jpg'), scale=0)


def test_numerical_changes_require_review_even_with_majority():
    assert guarded_vote('anno de 1825', ['anno de 1835']*3) == ('anno de 1825', 'numero_protegido')
    assert guarded_vote('GÊ', ['DE']*3) == ('GÊ', 'fragmento_curto')
    assert guarded_vote('DIAR1O 1825', ['DIARIO 1825']*3)[1] == 'numero_protegido'
    assert guarded_vote('DIARlO DE PERNAMBUCO', ['DIARIO DE PERNAMBUCO']*3)[0] == 'DIARIO DE PERNAMBUCO'


def test_refinement_requires_two_agreeing_alternatives():
    assert guarded_vote('DIARlO antigo', ['DIARIO antigo', 'DIARlO antigo'])[0] == 'DIARlO antigo'
    assert guarded_vote('DIARlO antigo', ['DIARIO antigo', 'DIARIO antigo'])[0] == 'DIARIO antigo'
    assert guarded_vote('anno de 1825', ['anno de 1835', 'anno de 1835'])[0] == 'anno de 1825'


def test_refine_cli_requires_correct(tmp_path):
    import click
    from click.testing import CliRunner
    from src.cli.commands.vision import register_vision_commands
    group = click.Group()
    register_vision_commands(group)
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'placeholder')
    output = tmp_path / 'out'
    result = CliRunner().invoke(group, ['ocr-local', '--image', str(image), '--refine', '--out', str(output)])
    assert result.exit_code == 2
    assert '--refine exige --correct' in result.output
    assert not output.exists()
