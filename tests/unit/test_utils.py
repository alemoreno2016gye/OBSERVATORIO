from packages.etl.src.etl.utils import parse_periodo, as_hs, normalize_column_name


def test_parse_periodo():
    y, m = parse_periodo('2024 / 03 - Marzo')
    assert y == 2024
    assert m == 3


def test_as_hs():
    assert as_hs('803901100', 10) == '0803901100'


def test_normalize_column_name_with_accents():
    assert normalize_column_name('Código Subpartida 10') == 'codigo_subpartida_10'
