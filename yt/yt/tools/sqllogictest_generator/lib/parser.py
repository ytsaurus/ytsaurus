import logging
import sqlglot
from pathlib import Path


from .constants import (
    STATEMENT_SEPARATOR,
    LABEL_INDEX,
    SORT_MODE_INDEX,
    ERROR_INDEX,
    LINE_COMMENT_MARK,
    LINE_RESULT_MARK,
    STATEMENT_END_MARK,
)
from .enum import SQLLogicSortMode, SQLLogicStatementType, StatementType


class BaseStatement:
    def __init__(self, statement: str, statement_type: SQLLogicStatementType):
        self.statement_type = statement_type
        self.statement = statement
        self.sql_statement_type = self._get_sql_statement_type()

    def _get_sql_statement_type(self) -> StatementType:
        ast = sqlglot.parse_one(self.statement)
        type = ast.__class__.__name__.upper()
        return StatementType[type]


class Statement(BaseStatement):
    def __init__(
        self,
        statement: str,
        statement_type: SQLLogicStatementType,
        with_error: bool,
    ):
        super().__init__(statement, statement_type)
        self.with_error = with_error


class Query(BaseStatement):
    def __init__(
        self,
        statement: str,
        statement_type: SQLLogicStatementType,
        name: str,
        sort_mode: SQLLogicSortMode,
    ):
        super().__init__(statement, statement_type)
        self.name = name
        self.sort_mode = sort_mode


class SQLLogicParser:
    def __init__(self):
        self.query_index = 0
        self.logger = logging.getLogger(self.__class__.__name__)

    def _get_statement_type(self, line: str) -> SQLLogicStatementType:
        for t in SQLLogicStatementType:
            if t.value in line:
                return t
        raise RuntimeError("Unknown statement type")

    def _get_query_name(self, line: str) -> str:
        words = line.split(" ")
        label = None if len(words) < (LABEL_INDEX + 1) else words[LABEL_INDEX]
        self.query_index += 1
        return f"query_{self.query_index}" + (f"_{label}" if label else "")

    def _get_sort_mode(self, line: str) -> bool:
        words = line.split(" ")
        sort_mode = SQLLogicSortMode.NOSORT

        try:
            if len(words) >= (SORT_MODE_INDEX + 1):
                sort_mode_word = words[SORT_MODE_INDEX].upper()
                sort_mode = SQLLogicSortMode[sort_mode_word]
        except KeyError as e:
            self.logger.warning(f"Unknown sort mode: {e}")

        return sort_mode

    def _get_statement_with_error(self, line: str) -> bool:
        words = line.split(" ")
        return words[ERROR_INDEX] == "error"

    def _make_statement(self, block: str) -> BaseStatement:
        lines = [line for line in block.splitlines() if line.strip()]
        statement_type = self._get_statement_type(lines[0])

        statement_lines = []
        for line in lines[1:]:
            if line.startswith(LINE_COMMENT_MARK):
                continue
            if line.startswith(LINE_RESULT_MARK):
                break
            statement_lines.append(line)

        statement_lines[-1] = statement_lines[-1] + STATEMENT_END_MARK
        statement = "\n".join(statement_lines)

        if statement_type == SQLLogicStatementType.STATEMENT:
            statement_with_error = self._get_statement_with_error(lines[0])
            return Statement(statement, statement_type, statement_with_error)
        elif statement_type == SQLLogicStatementType.QUERY:
            statement_name = self._get_query_name(lines[0])
            statement_sorted_output = self._get_sort_mode(lines[0])

            return Query(statement, statement_type, statement_name, statement_sorted_output)
        else:
            raise RuntimeError("Unknown statement type")

    def parse(self, source_path: Path) -> list[BaseStatement]:
        with open(source_path, "r") as f:
            content = f.read()

        statement_group = []
        blocks = content.split(STATEMENT_SEPARATOR)

        for block in blocks:
            self.logger.debug(f"Processing block {block}")

            try:
                statement = self._make_statement(block)
                statement_group.append(statement)
            except Exception as e:
                self.logger.error(f"Unable to make statement from block {block}: {e}")

        return statement_group
