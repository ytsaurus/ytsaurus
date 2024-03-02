from functools import cmp_to_key


def compare_values_like_in_ql(lhs, rhs):
    if lhs is None and rhs is None:
        return 0
    if lhs is None:
        return -1
    if rhs is None:
        return 1
    if lhs == rhs:
        return 0
    if lhs < rhs:
        return -1
    return 1


def compare_rows_like_in_ql(lhs, rhs):
    assert len(lhs) == len(rhs)
    for index in range(len(lhs)):
        c = compare_values_like_in_ql(lhs[index], rhs[index])
        if c < 0:
            return -1
        if c > 0:
            return 1
    return 0


sort_key_like_in_ql = cmp_to_key(compare_rows_like_in_ql)


def from_yt_to_list(rows):
    return [[y for x, y in row.items()] for row in rows]
