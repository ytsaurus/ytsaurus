import re
from typing import Any, Dict, List, Optional, Tuple

PROMQL_KEYWORDS = frozenset(
    {
        "and",
        "avg",
        "bool",
        "bottomk",
        "by",
        "count_values",
        "count",
        "end",
        "group_left",
        "group_right",
        "group",
        "if",
        "ignoring",
        "Inf",
        "max",
        "min",
        "NaN",
        "offset",
        "on",
        "or",
        "quantile",
        "scalar",
        "start",
        "stddev",
        "stdvar",
        "sum",
        "time",
        "topk",
        "unless",
        "vector",
        "without",
    }
)

UNRESOLVED_VAR_RE = re.compile(r'\$\{?[A-Za-z_][A-Za-z0-9_]*\}?')

_OP_CHARS = "=!"


def _tokenize(expr: str) -> List[Tuple[str, Any]]:
    tokens: List[Tuple[str, Any]] = []
    i, n = 0, len(expr)
    while i < n:
        ch = expr[i]
        if ch.isspace():
            i += 1
            continue
        if ch.isalpha() or ch == "_":
            j = i + 1
            while j < n and (expr[j].isalnum() or expr[j] == "_"):
                j += 1
            tokens.append(("IDENT", expr[i:j]))
            i = j
            continue
        if ch in "\"'`":
            quote = ch
            j = i + 1
            buf = []
            while j < n:
                c = expr[j]
                if c == "\\" and quote != "`" and j + 1 < n:
                    buf.append(expr[j + 1])
                    j += 2
                    continue
                if c == quote:
                    j += 1
                    break
                buf.append(c)
                j += 1
            tokens.append(("STRING", "".join(buf)))
            i = j
            continue
        if ch == "{":
            tokens.append(("LBRACE", ch))
            i += 1
            continue
        if ch == "}":
            tokens.append(("RBRACE", ch))
            i += 1
            continue
        if ch == "(":
            tokens.append(("LPAREN", ch))
            i += 1
            continue
        if ch == ")":
            tokens.append(("RPAREN", ch))
            i += 1
            continue
        if ch == "[":
            tokens.append(("LBRACKET", ch))
            i += 1
            continue
        if ch == "]":
            tokens.append(("RBRACKET", ch))
            i += 1
            continue
        if ch == ",":
            tokens.append(("COMMA", ch))
            i += 1
            continue
        if ch in _OP_CHARS or (ch == "~" and tokens and tokens[-1][0] == "OP"):
            j = i
            while j < n and (expr[j] in _OP_CHARS or expr[j] == "~"):
                j += 1
            tokens.append(("OP", expr[i:j]))
            i = j
            continue
        tokens.append(("OTHER", ch))
        i += 1
    return tokens


def _parse_matchers_from_tokens(tokens: List[Tuple[str, Any]], start: int) -> Tuple[List[Tuple[str, str, str]], int]:
    assert tokens[start][0] == "LBRACE"
    i = start + 1
    matchers: List[Tuple[str, str, str]] = []
    n = len(tokens)
    while i < n and tokens[i][0] != "RBRACE":
        if tokens[i][0] != "IDENT" or i + 2 >= n:
            return [], _skip_to_rbrace(tokens, start)
        label = tokens[i][1]
        if tokens[i + 1][0] != "OP" or tokens[i + 1][1] not in ("=", "!=", "=~", "!~"):
            return [], _skip_to_rbrace(tokens, start)
        op = tokens[i + 1][1]
        if tokens[i + 2][0] != "STRING":
            return [], _skip_to_rbrace(tokens, start)
        matchers.append((label, op, tokens[i + 2][1]))
        i += 3
        if i < n and tokens[i][0] == "COMMA":
            i += 1
    if i < n and tokens[i][0] == "RBRACE":
        return matchers, i + 1
    return matchers, i


def _skip_to_rbrace(tokens: List[Tuple[str, Any]], start: int) -> int:
    i = start + 1
    while i < len(tokens) and tokens[i][0] != "RBRACE":
        i += 1
    return i + 1 if i < len(tokens) else i


def _format_selector(metric: Optional[str], matchers: List[Tuple[str, str, str]]) -> str:
    sorted_matchers = sorted(matchers, key=lambda m: m[0])
    if sorted_matchers:
        inner = ",".join(f'{label}{op}"{escape_label_value(val)}"' for label, op, val in sorted_matchers)
        return (metric or "") + "{" + inner + "}"
    return metric or "{}"


def escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")


def extract_selectors(expr: str) -> List[str]:
    tokens = _tokenize(expr)
    selectors: List[str] = []
    i, n = 0, len(tokens)
    bracket_depth = 0
    while i < n:
        kind, value = tokens[i]
        if kind == "LBRACKET":
            bracket_depth += 1
            i += 1
            continue
        if kind == "RBRACKET":
            bracket_depth = max(0, bracket_depth - 1)
            i += 1
            continue
        if bracket_depth > 0:
            i += 1
            continue
        if kind == "IDENT":
            if value in ("by", "without") and i + 1 < n and tokens[i + 1][0] == "LPAREN":
                i += 2
                paren_depth = 1
                while i < n and paren_depth > 0:
                    if tokens[i][0] == "LPAREN":
                        paren_depth += 1
                    elif tokens[i][0] == "RPAREN":
                        paren_depth -= 1
                    i += 1
                continue
            next_kind = tokens[i + 1][0] if i + 1 < n else None
            if next_kind == "LBRACE":
                matchers, after = _parse_matchers_from_tokens(tokens, i + 1)
                selectors.append(_format_selector(value, matchers))
                i = after
                continue
            if next_kind == "LPAREN" or value in PROMQL_KEYWORDS:
                i += 1
                continue
            selectors.append(value)
            i += 1
            continue
        if kind == "LBRACE":
            matchers, after = _parse_matchers_from_tokens(tokens, i)
            selectors.append(_format_selector(None, matchers))
            i = after
            continue
        i += 1
    return selectors


def _parse_selector(selector: str) -> Tuple[Optional[str], Dict[str, Tuple[str, str]]]:
    tokens = _tokenize(selector)
    if not tokens:
        return None, {}
    metric: Optional[str] = None
    i = 0
    if tokens[0][0] == "IDENT":
        metric = tokens[0][1]
        i = 1
    if i < len(tokens) and tokens[i][0] == "LBRACE":
        matchers, _ = _parse_matchers_from_tokens(tokens, i)
        return metric, {label: (op, val) for label, op, val in matchers}
    return metric, {}


def _value_subsumes(a_op: str, a_val: str, b_op: str, b_val: str) -> bool:
    if a_op == "=" and b_op == "=":
        return a_val == b_val
    if a_op == "=~" and b_op == "=":
        try:
            return re.fullmatch(a_val, b_val) is not None
        except re.error:
            return False
    if a_op == "=~" and b_op == "=~":
        return a_val == b_val or a_val in (".*", ".+")
    if a_op == "!=" and b_op == "!=":
        return a_val == b_val
    return False


def _selector_subsumes(a: str, b: str) -> bool:
    if a == b:
        return True
    a_metric, a_matchers = _parse_selector(a)
    b_metric, b_matchers = _parse_selector(b)
    a_matchers_copy = dict(a_matchers)
    b_matchers_copy = dict(b_matchers)
    if a_metric:
        a_matchers_copy["__name__"] = ("=", a_metric)
    if b_metric:
        b_matchers_copy["__name__"] = ("=", b_metric)
    for label, (a_op, a_val) in a_matchers_copy.items():
        if label not in b_matchers_copy:
            return False
        b_op, b_val = b_matchers_copy[label]
        if not _value_subsumes(a_op, a_val, b_op, b_val):
            return False
    return True


def eliminate_subsets(selectors: List[str]) -> List[str]:
    unique = sorted(set(selectors), key=lambda x: (x.startswith('{'), len(x), x))
    keep: List[str] = []

    for i, candidate in enumerate(unique):
        is_absorbed = any(
            i != j and _selector_subsumes(other, candidate) and (j < i or not _selector_subsumes(candidate, other))
            for j, other in enumerate(unique)
        )
        if not is_absorbed:
            keep.append(candidate)
    return keep
