from enum import StrEnum, auto


class SQLLogicSortMode(StrEnum):
    NOSORT = auto()
    ROWSORT = auto()
    VALUESORT = auto()


class SQLLogicStatementType(StrEnum):
    STATEMENT = auto()
    QUERY = auto()


class StatementType(StrEnum):
    SELECT = auto()
    CREATE = auto()
    INSERT = auto()
    UNION = auto()
    EXCEPT = auto()
    INTERSECT = auto()
