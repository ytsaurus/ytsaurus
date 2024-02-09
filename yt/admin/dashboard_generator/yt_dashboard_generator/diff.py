from .helpers import break_long_lines_in_multiline_cell

import colorama

"""
This more a piece of shame than a masterpiece. I'll refactor it a bit later,
and now I'm too tired for it.

Dear reader, I'm very sorry.
"""


def _diff_cells(lhs, rhs):
    if lhs == rhs:
        return None

    lhs = list(map(str.strip, lhs.split("\n")))
    rhs = list(map(str.strip, rhs.split("\n")))
    lptr = 0
    rptr = 0
    result = []

    def _red(s):
        s = break_long_lines_in_multiline_cell(s).split("\n")
        return "\n".join(colorama.Fore.RED + l + colorama.Style.RESET_ALL for l in s)  # noqa: E741

    def _green(s):
        s = break_long_lines_in_multiline_cell(s).split("\n")
        return "\n".join(colorama.Fore.GREEN + l + colorama.Style.RESET_ALL for l in s)  # noqa: E741

    while lptr < len(lhs) or rptr < len(rhs):
        if lptr == len(lhs):
            result.append(_green(rhs[rptr]))
            rptr += 1
            continue
        if rptr == len(rhs):
            result.append(_red(lhs[lptr]))
            lptr += 1
            continue
        if lhs == rhs:
            result.append(lhs)
            rptr += 1
            lptr += 1
            continue
        lkey = lhs[lptr]
        rkey = rhs[rptr]
        if lkey == rkey:
            result.append(break_long_lines_in_multiline_cell(lkey))
            lptr += 1
            rptr += 1
            continue
        ldist = rhs.index(lkey, rptr) if lkey in rhs[rptr:] else 1000
        rdist = lhs.index(rkey, lptr) if rkey in lhs[lptr:] else 1000
        if ldist > rdist:
            result.append(_red(lkey))
            lptr += 1
            continue
        else:
            result.append(_green(rkey))
            rptr += 1
            continue

    return "\n".join(result)


def diff(lhs, rhs):
    result = []
    lptr = 0
    rptr = 0

    def _how_soon_in_lhs(key):
        for i in range(lptr, len(lhs)):
            if lhs[i][0] == key:
                return i - lptr
        return len(lhs) + len(rhs)

    def _how_soon_in_rhs(key):
        for i in range(rptr, len(rhs)):
            if rhs[i][0] == key:
                return i - rptr
        return len(lhs) + len(rhs)

    def _red(row):
        return "{}{}{}".format(
            colorama.Fore.RED,
            " ".join(row[0]),
            colorama.Style.RESET_ALL)

    def _green(row):
        return "{}{}{}".format(
            colorama.Fore.GREEN,
            " ".join(row[0]),
            colorama.Style.RESET_ALL)

    def _append_cell_diff(lcells, rcells):
        while len(lcells) < len(rcells):
            lcells.append("")
        while len(rcells) < len(lcells):
            rcells.append("")
        if not any(_diff_cells(x, y) for x, y in zip(lcells, rcells)):
            return
        row = []
        for x, y in zip(lcells, rcells):
            diff = _diff_cells(x, y)
            if diff:
                row.append(diff)
            else:
                row.append(x)
        result.append(row)

    while lptr < len(lhs) or rptr < len(rhs):
        if lptr == len(lhs):
            _append_cell_diff([], rhs[rptr][1])
            rptr += 1
            continue
        if rptr == len(rhs):
            _append_cell_diff(lhs[lptr][1], [])
            lptr += 1
            continue
        lkey = lhs[lptr][0]
        rkey = rhs[rptr][0]
        if lkey == rkey:
            _append_cell_diff(lhs[lptr][1], rhs[rptr][1])
            rptr += 1
            lptr += 1
            continue
        ldist = _how_soon_in_rhs(lkey)
        rdist = _how_soon_in_lhs(rkey)
        if ldist > rdist:
            _append_cell_diff(lhs[lptr][1], [])
            lptr += 1
            continue
        else:
            _append_cell_diff([], rhs[rptr][1])
            rptr += 1
            continue

    return result
