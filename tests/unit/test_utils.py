import pytest
pytest.importorskip('pandas')

from packages.etl.src.etl.utils import parse_periodo, as_hs


def test_parse_periodo():
    y, m = parse_periodo('2024 / 03 - Marzo')
    assert y == 2024
    assert m == 3


def test_as_hs():
    assert as_hs('803901100', 10) == '0803901100'
