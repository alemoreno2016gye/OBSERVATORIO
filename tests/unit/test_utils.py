from packages.etl.src.etl.utils import (
    as_hs,
    expand_chapter_token,
    normalize_column_name,
    normalize_text_value,
    parse_periodo,
)


def test_parse_periodo():
    y, m = parse_periodo('2024 / 03 - Marzo')
    assert y == 2024
    assert m == 3


def test_parse_periodo_variant():
    y, m = parse_periodo('2024-7')
    assert y == 2024
    assert m == 7


def test_as_hs():
    assert as_hs('803901100', 10) == '0803901100'


def test_normalize_column_name_with_accents():
    assert normalize_column_name('Código Subpartida 10') == 'codigo_subpartida_10'


def test_expand_chapter_token_unicode_dash():
    assert expand_chapter_token('01–05') == ['01', '02', '03', '04', '05']


def test_normalize_text_value_cleanup():
    assert normalize_text_value('\ufeff - China;  ') == 'China'
