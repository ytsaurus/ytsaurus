from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Generator

from yt.yson import yson_to_json
from yt.yson import YsonList
from yt.yt_sync.core.spec import Column
from yt.yt_sync.core.spec.details import SchemaSpec

from .column import YtColumn
from .types import Types


@dataclass
class YtSchema:
    columns: list[YtColumn]
    strict: bool
    unique_keys: bool

    def __eq__(self, other: YtSchema) -> bool:
        assert isinstance(other, type(self))
        return (
            self._is_key_eq(other)
            and self._is_non_key_eq(other)
            and self.strict == other.strict
            and self.unique_keys == other.unique_keys
        )

    def _is_key_eq(self, other: YtSchema) -> bool:
        def _extract_key(schema: YtSchema) -> list[YtColumn]:
            return list([c for c in schema.columns if c.sort_order])

        return _extract_key(self) == _extract_key(other)

    def _is_non_key_eq(self, other: YtSchema) -> bool:
        def _extract_non_key(schema: YtSchema) -> dict[str, YtColumn]:
            return {c.name: c for c in schema.columns if not c.sort_order}

        return _extract_non_key(self) == _extract_non_key(other)

    @property
    def yt_schema(self) -> YsonList:
        schema = YsonList()
        schema.attributes["unique_keys"] = self.unique_keys
        schema.attributes["strict"] = self.strict
        for column in self.columns:
            schema.append(column.yt_attributes)
        return schema

    @property
    def is_uniform_distribution_supported(self) -> bool:
        if not self.columns:
            return False
        first = self.columns[0]
        return bool(first.sort_order) and first.column_type in ("int64", "uint64")

    @property
    def has_hunks(self) -> bool:
        for column in self.columns:
            if column.has_hunks:
                return True
        return False

    def _columns_as_dict(self, column_filter: Callable[[YtColumn], bool] = lambda c: True) -> dict[str, YtColumn]:
        return {c.name: c for c in self.columns if column_filter(c)}

    def is_compatible(self, other: YtSchema, allow_full_downtime: bool = True) -> bool:
        assert isinstance(other, type(self))
        if not self.columns or not other.columns:
            return False

        self_keys = set(self._columns_as_dict(lambda c: c.sort_order is not None and c.expression is None).keys())
        other_keys = set(other._columns_as_dict(lambda c: c.sort_order is not None and c.expression is None).keys())

        self_columns = set(self._columns_as_dict(lambda c: not c.sort_order).keys())
        other_columns = set(other._columns_as_dict(lambda c: not c.sort_order).keys())

        # minimal data columns & keys intersection required
        if not (self_columns & other_columns):
            return False
        if not self.is_ordered and not (self_keys & other_keys):
            return False

        for added_column in self.get_added_columns(other):
            if added_column.required:
                return False

        for self_column, other_column in self.get_changed_columns(other):
            if not self_column.is_compatible(other_column):
                return False

        if not allow_full_downtime and self.is_change_with_downtime(other):
            return False

        return True

    def _get_columns_diff(self, other: YtSchema) -> Generator[tuple[YtColumn | None, YtColumn | None], None, None]:
        assert isinstance(other, type(self))

        self_columns = self._columns_as_dict()
        other_columns = other._columns_as_dict()

        for column_name in self_columns.keys() | other_columns.keys():
            self_column = self_columns.get(column_name, None)
            other_column = other_columns.get(column_name, None)
            if self_column != other_column:
                yield (self_column, other_column)

    def get_deleted_columns(self, other: YtSchema) -> Generator[YtColumn, None, None]:
        for self_column, other_column in self._get_columns_diff(other):
            if self_column is not None and other_column is None:
                yield self_column

    def get_added_columns(self, other: YtSchema) -> Generator[YtColumn, None, None]:
        for self_column, other_column in self._get_columns_diff(other):
            if self_column is None and other_column is not None:
                yield other_column

    def get_changed_columns(self, other: YtSchema) -> Generator[tuple[YtColumn, YtColumn], None, None]:
        for self_column, other_column in self._get_columns_diff(other):
            if self_column is None or other_column is None:
                continue
            if self_column != other_column:
                yield (self_column, other_column)

    def is_data_modification_required(self, other: YtSchema) -> bool:
        if bool(next(self.get_deleted_columns(other), None)):
            return True

        return self.is_key_changed_with_data_modification(other)

    def is_key_changed(self, other: YtSchema) -> bool:
        if any(c.is_key_column for c in self.get_added_columns(other)):
            return True
        if any(c.is_key_column for c in self.get_deleted_columns(other)):
            return True
        if any(sc.is_data_modification_required(oc) for sc, oc in self.get_changed_columns(other)):
            return True
        self_key = [c.name for c in self.columns if c.is_key_column]
        other_key = [c.name for c in other.columns if c.is_key_column]
        if self_key != other_key:
            return True
        return False

    def is_key_changed_with_data_modification(self, other: YtSchema) -> bool:
        self_key: list[YtColumn] = self.table_key
        other_key: list[YtColumn] = other.table_key

        if len(self_key) > len(other_key):
            return True

        for self_key_column, other_key_column in zip(self_key, other_key):
            # check key columns order
            if self_key_column.name != other_key_column.name:
                return True
            # check sort order
            if self_key_column.is_data_modification_required(other_key_column):
                return True

        return False

    def is_key_changed_with_downtime(self, other: YtSchema) -> bool:
        self_key: list[YtColumn] = self.table_key
        other_key: list[YtColumn] = other.table_key

        for self_key_column, other_key_column in zip(self_key, other_key):
            # check key columns order
            if self_key_column.name != other_key_column.name:
                return True
            # check sort order
            if self_key_column.is_downtime_required(other_key_column):
                return True

        return False

    def is_column_set_changed(self, other: YtSchema) -> bool:
        has_added = next(self.get_added_columns(other), None) is not None
        has_deleted = next(self.get_deleted_columns(other), None) is not None
        return has_added or has_deleted

    def is_change_with_downtime(self, other: YtSchema) -> bool:
        if self.is_data_modification_required(other):
            # Until https://st.yandex-team.ru/YTSYNC-25
            return True
        has_added = next(self.get_added_columns(other), None) is not None
        has_deleted = next(self.get_deleted_columns(other), None) is not None
        return (has_added and has_deleted) or self.is_key_changed_with_downtime(other)

    @property
    def is_ordered(self) -> bool:
        return bool(self.columns) and self.columns[0].sort_order is None

    @property
    def table_key(self) -> list[YtColumn]:
        return [c for c in self.columns if c.sort_order is not None]

    def to_unsorted(self) -> YtSchema:
        unsorted_schema: YtSchema = YtSchema(columns=list(), strict=self.strict, unique_keys=False)
        for column in self.columns:
            patched_column: YtColumn = deepcopy(column)
            patched_column.sort_order = None
            unsorted_schema.columns.append(patched_column)
        return unsorted_schema

    @staticmethod
    def is_strict(schema: YsonList | Any) -> bool:
        if isinstance(schema, YsonList):
            return schema.attributes.get("strict", True)
        return True

    @classmethod
    def make(cls, schema: list[Column], is_strict: bool, ensure: bool) -> YtSchema:
        if ensure:
            SchemaSpec.ensure(schema)
        return cls(
            columns=[YtColumn.make(column) for column in schema],
            strict=is_strict,
            unique_keys=SchemaSpec.is_sorted(schema),
        )

    @classmethod
    def preprocessed_schema(cls, schema: Types.Schema | YsonList) -> Types.Schema:
        if isinstance(schema, YsonList):
            converted = yson_to_json(schema)
            if isinstance(converted, dict):
                return converted["$value"]
            assert isinstance(converted, list)
            return converted
        return schema

    @classmethod
    def parse(cls, schema: Types.Schema | YsonList) -> YtSchema:
        return cls.make(
            SchemaSpec.parse(cls.preprocessed_schema(schema), with_ensure=False), cls.is_strict(schema), ensure=True
        )
