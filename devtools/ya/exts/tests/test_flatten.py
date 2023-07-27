# coding=utf-8

import pytest

from exts import flatten


@pytest.mark.parametrize(
    'coll, expected_result',
    [
        ([[1, 2, 3], [4, 5]], [1, 2, 3, 4, 5]),
        ([[1, 2, 3], [4, 5], 6], [1, 2, 3, 4, 5, 6]),
        ([[[1, 2, 3], [4, 5]], 6], [1, 2, 3, 4, 5, 6]),
        (["aa", "bb", "cc"], ["aa", "bb", "cc"]),
        (["aa", ["bb"], "cc"], ["aa", "bb", "cc"]),
        (("aa", ("bb", "cc"), ("dd", "ee")), ["aa", "bb", "cc", "dd", "ee"]),
    ],
)
def test_flatten(coll, expected_result):
    assert list(flatten.flatten(coll)) == expected_result
