def break_long_lines(s, max_width=50):
    num_leading_spaces = len(s) - len(s.lstrip(" "))
    s = s.lstrip(" ")
    separator = " " * (num_leading_spaces + 4)
    return " " * num_leading_spaces + f" \\\n{separator}".join(s[i:i+max_width] for i in range(0, len(s), max_width))


def break_long_lines_in_multiline_cell(cell, max_width=50):
    return "\n".join(break_long_lines(s, max_width) for s in cell.split("\n"))


def pretty_dump_fixed_indent(x, offset=""):
    INDENT = "  "
    if issubclass(type(x), list):
        if not x:
            return "[]"
        res = "[\n"
        for y in x:
            res += offset + INDENT + pretty_dump_fixed_indent(y, offset + INDENT) + ",\n"
        res += offset + "]"
        return res
    elif issubclass(type(x), dict):
        if not x:
            return "{}"
        res = "{\n"
        for k, v in x.items():
            pv = pretty_dump_fixed_indent(v, offset + INDENT)
            res += offset + INDENT + f"{k}: {pv},\n"
        res += offset + "}"
        return res
    else:
        return str(x)


def pretty_print_fixed_indent(x):
    print(pretty_dump_fixed_indent(x))
