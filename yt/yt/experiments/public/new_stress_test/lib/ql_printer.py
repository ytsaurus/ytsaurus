from .ql_base import Query


def to_ql_notation(query: Query, table_path: str, pretty: bool) -> str:
    # TODO(dtorilov): Support having predicates.
    # TODO(dtorilov): Support totals.

    CBOLD = "\33[1m"
    CGREEN = "\33[32m"
    CEND = "\033[0m"
    NEWLINE = "\n"
    if not pretty:
        CBOLD = ""
        CEND = ""
        CGREEN = ""
        NEWLINE = " "

    result = CBOLD + "select " + CEND

    if pretty:
        result += ", ".join(query.projection_columns)
        if query.projection_aggregates:
            result += ", "
            result += ", ".join(
                map(
                    lambda x: f"{CGREEN}{x.function}{CEND}({x.column})",
                    query.projection_aggregates,
                )
            )
    else:
        alias_number = 0
        for column in query.projection_columns:
            result += f"{column} as alias{alias_number}, "
            alias_number += 1
        result = result[:-2]

        if query.projection_aggregates:
            result += ", "
            for agg in query.projection_aggregates:
                result += f"{agg.function}({agg.column}) as alias{alias_number}, "
                alias_number += 1
        result = result[:-2]

    result += NEWLINE
    result += f"{CBOLD}from{CEND} `{table_path}`"

    if query.filter_predicates:
        result += f"{NEWLINE}{CBOLD}where{CEND}{NEWLINE + pretty * '    '}"
        result += f"{NEWLINE + pretty * '    '}{CGREEN}and{CEND} ".join(query.filter_predicates)

    result += NEWLINE
    result += CBOLD + "group by " + CEND
    result += ", ".join(query.group_keys)

    if query.order_keys:
        result += NEWLINE
        result += CBOLD + "order by " + CEND
        result += ", ".join(query.order_keys)

    if query.limit is not None:
        result += NEWLINE
        result += f"{CBOLD}limit{CEND} {query.limit}"

    return result
