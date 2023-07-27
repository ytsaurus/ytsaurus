import pytest

from exts.detect_recursive_dict import detect_recursive_dict

correct = [
    {},
    {1: 1, 2: 2, 3: {4: 4, 5: 5, 6: {7: 7, 8: 8}}, 9: 9, 10: 10},
    {1: [2, 3, 4, [5, 6, 7], (8, 9, 10), {11, 12, 13}], 19: (14, 15, 16), 20: {17, 18}, 21: {22: 23}},
]

one_level = {1: 1, 2: 2, 3: 3}
one_level[4] = one_level

two_level = {1: 1, 2: 2}
two_level[3] = {4: two_level}

three_level = {1: 1, 2: 2}
three_level[3] = {4: {5: three_level}}

double_two_three = {1: 1, 2: 2}
double_two_three[3] = {4: double_two_three}
double_two_three[5] = {6: {7: double_two_three}}

inner_recursive = {1: 1, 2: 2}
inner_recursive[3] = inner_recursive
inner = {4: 4, 5: 5, 6: inner_recursive}

lrid = [1, 2, 3]
list_recursive_in_dict = {4: lrid}
lrid.extend([list_recursive_in_dict, 5, 6])


tuple_recursive_in_dict = {}
tuple_recursive_in_dict[1] = (1, 2, tuple_recursive_in_dict)

list_recursive_deep = [1, 2]
list_recursive_deep.append({3: [4, 5, (list_recursive_deep, 7)]})

two_eq = {'a': {1: 1, 2: 2}}
two_eq['b'] = two_eq['a']
two_eq['c'] = {'d': two_eq}


_slr = {}
second_level_recursive = {1: _slr}
_slr['3'] = {'4': _slr}


recursive = [
    one_level,
    two_level,
    three_level,
    double_two_three,
    inner_recursive,
    list_recursive_in_dict,
    tuple_recursive_in_dict,
    two_eq,
    second_level_recursive,
]


@pytest.mark.parametrize('obj', correct)
def test_recursive_detector_ok(obj):
    assert not detect_recursive_dict(obj)


@pytest.mark.parametrize('obj', recursive)
def test_recursive_detector_recursive(obj):
    assert detect_recursive_dict(obj)
