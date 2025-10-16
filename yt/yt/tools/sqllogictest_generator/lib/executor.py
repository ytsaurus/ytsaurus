import sqlite3
import abc
import typing
from pathlib import Path

from .data import TableColumnInfo


class Executor(abc.ABC):
    @abc.abstractmethod
    def get_schema(self, table_name: str) -> list[TableColumnInfo]:
        pass

    @abc.abstractmethod
    def create(self, statement: str):
        pass

    @abc.abstractmethod
    def insert(self, statement: str) -> list[tuple[typing.Any]]:
        pass

    @abc.abstractmethod
    def select(self, statement: str) -> list[tuple[typing.Any]]:
        pass


class SQLiteExecutor(Executor):
    def __init__(self, db_path: Path):
        self.db_path = db_path

        if self.db_path.exists():
            self.db_path.unlink()

        self.connection = sqlite3.connect(self.db_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

        if self.db_path.exists():
            self.db_path.unlink()

    def get_schema(self, table_name: str) -> list[TableColumnInfo]:
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}');")
        self.connection.commit()
        schema_info = cursor.fetchall()
        return [TableColumnInfo(c[1], c[2]) for c in schema_info]

    def create(self, statement: str):
        cursor = self.connection.cursor()
        cursor.execute(statement)
        self.connection.commit()

    def insert(self, statement: str) -> list[tuple[typing.Any]]:
        cursor = self.connection.cursor()
        inserted_rows = cursor.execute(statement).fetchall()
        return inserted_rows

    def select(self, statement: str) -> list[tuple[typing.Any]]:
        cursor = self.connection.cursor()
        expected_rows = cursor.execute(statement).fetchall()
        return expected_rows
