ERROR_INDEX = 1
SORT_MODE_INDEX = 2
LABEL_INDEX = 3

LINE_COMMENT_MARK = "#"
LINE_RESULT_MARK = "----"
STATEMENT_END_MARK = ";"
STATEMENT_SEPARATOR = "\n\n"

DEFAULT_YT_ROWS_COUNT_LIMIT = 1_000_000

SQL_TYPE_TO_YT_TYPE = {
    "INT": "int64",
    "INTEGER": "int64",
    "TEXT": "string",
    "VARCHAR(*)": "string",
    "CHARACTER(*)": "string",
    "VARYING CHARACTER(*)": "string",
    "DOUBLE": "double",
    "FLOAT": "float",
    "BOOLEAN": "boolean",
}
