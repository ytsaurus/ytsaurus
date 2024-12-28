from yt_proto.yt.orm.client.proto import object_pb2

from .common import OrmColumn, DEFAULT_DATA_GROUP
from .filters import camel_case_from_parts, snake_case_from_parts

from copy import copy
from dataclasses import dataclass, field
from enum import Enum
from itertools import chain
from typing import Any, Optional, TYPE_CHECKING
from re import finditer

if TYPE_CHECKING:
    from .object import OrmObject, OrmAttribute


class OrmIndexCppMode(Enum):
    DISABLED = "Disabled"
    REMOVE_ONLY = "RemoveOnly"
    BUILDING = "Building"
    ENABLED = "Enabled"


def _from_proto_index_mode(mode: object_pb2.TIndexOption.EIndexMode) -> OrmIndexCppMode:
    if mode == object_pb2.TIndexOption.EIndexMode.IM_ENABLED:
        return OrmIndexCppMode.ENABLED
    elif mode == object_pb2.TIndexOption.EIndexMode.IM_REMOVE_ONLY:
        return OrmIndexCppMode.REMOVE_ONLY
    elif mode == object_pb2.TIndexOption.EIndexMode.IM_BUILDING:
        return OrmIndexCppMode.BUILDING
    elif mode == object_pb2.TIndexOption.EIndexMode.IM_DISABLED:
        return OrmIndexCppMode.DISABLED
    else:
        raise Exception(f"Unknown index mode: {mode}")


@dataclass
class OrmIndex:
    camel_case_name: str
    snake_case_name: str
    hash_expression: str
    object_type_snake_case_name: str
    object_type_camel_case_name: str
    object: "OrmObject"
    index_attributes: list["OrmAttribute"] = field(default_factory=list)
    is_repeated: bool = False
    is_unique: bool = False
    underlying_table: str = ""
    underlying_table_snake_case: str = ""
    mode: OrmIndexCppMode = OrmIndexCppMode.DISABLED
    predicate: Optional[str] = None
    predicate_attributes: list["OrmAttribute"] = field(default_factory=list)
    custom_table_name: str = ""

    def __repr__(self) -> str:
        return f"({self.snake_case_name})"

    @property
    def table_name(self):
        return self.custom_table_name or self.underlying_table_snake_case or self.snake_case_name

    @classmethod
    def make(
        cls,
        index_option: Any,
        object_type: "OrmObject",
    ):
        result = cls(
            camel_case_name=index_option.camel_case_name,
            snake_case_name=index_option.snake_case_name,
            hash_expression=index_option.hash_expression,
            object_type_snake_case_name=object_type.snake_case_name,
            object_type_camel_case_name=object_type.camel_case_name,
            object=object_type,
            is_unique=index_option.unique,
            predicate=index_option.predicate,
            mode=_from_proto_index_mode(index_option.mode),
            custom_table_name=index_option.custom_table_name,
        )

        result._analyze_keys(index_option.key)
        result._analyze_predicate()
        result._analyze_deprecated_references()
        result._validate(index_option)

        return result

    @property
    def object_table_keys(self):
        if self.object.parent:
            yield from self.object.parent.key_columns(source=True, parent=True)
        yield from self.object.key_columns(source=True)

    @property
    def table_columns(self):
        key_columns = []

        for attribute in self.index_attributes:
            snake_case_name = attribute.field.snake_case_name
            camel_case_name = attribute.field.camel_case_name
            if attribute.suffix:
                snake_case_name = snake_case_from_parts(attribute.suffix.split("/")[1:])
                camel_case_name = camel_case_from_parts(snake_case_name.split("_"))

            field = copy(attribute.field)
            field.is_primary = True
            field.set_is_base_column(False)
            field.group = DEFAULT_DATA_GROUP

            key_columns.append(
                OrmColumn.make(
                    field,
                    name=snake_case_name,
                    cpp_name=camel_case_name,
                    type=field.yt_schema_value_type,
                    cpp_yt_type=field.cpp_yt_value_type,
                    is_reference_target=True,
                )
            )

        if self.hash_expression:
            yield OrmColumn.make_hash(self.hash_expression, group=None)

        yield from key_columns

        object_key_columns = list(self.object_table_keys)
        # The `group` and `lock` attributes are useless for the index table, so reset them.
        for column in object_key_columns:
            column.group = DEFAULT_DATA_GROUP
            column.lock = None
            if self.is_unique:
                column.sort_order = None

        yield from object_key_columns

        if not self.is_unique:
            yield OrmColumn.make_dummy()

    @property
    def all_attributes(self):
        return chain(self.index_attributes, self.predicate_attributes)

    def _analyze_keys(self, keys):
        for path in keys:
            attribute = self.object.resolve_attribute_by_path(path)
            assert not attribute.column_field.deprecated, "Index attribute must not be deprecated"
            assert (
                attribute not in self.index_attributes
            ), f"Attribute {path} specified in index {self.snake_case_name} twice"
            self.is_repeated = self.is_repeated or attribute.field.is_repeated

            attribute.field.indexed_by.append(self)
            self.index_attributes.append(attribute)

    def _analyze_predicate(self):
        if not self.predicate:
            return

        for match in finditer(r"\[((/\w+)+)\]", self.predicate):
            path = match[1]
            attribute = self.object.resolve_attribute_by_path(path)
            assert (
                not attribute.column_field.deprecated
            ), f"Index {self.snake_case_name} predicate attribute must not be deprecated"
            if attribute not in self.predicate_attributes:
                self.predicate_attributes.append(attribute)
                attribute.field.used_in_predicate = True

        assert self.predicate_attributes, f"Index {self.snake_case_name} has ill-formed attributes in predicate"

    def _analyze_deprecated_references(self):
        has_reference_field = any([attribute.field.foreign_object_type for attribute in self.index_attributes])
        if has_reference_field and not self.predicate and len(self.index_attributes) == 1:
            attribute = self.index_attributes[0]
            assert (
                not attribute.field.index_over_reference_table
            ), f"There are more than 1 index over reference field {attribute.full_path}"
            attribute.field.index_over_reference_table = self

    def _validate(self, index_option):
        assert index_option.object_type_snake_case_name == self.object.snake_case_name
        assert self.index_attributes, f"List of index {self.snake_case_name} attributes must be non-empty"
        assert not self.is_repeated or 1 == len(
            self.index_attributes
        ), f"Repeated index {self.snake_case_name} must contain exactly one attribute"
        assert all([attribute.field.is_primitive for attribute in self.all_attributes]), (
            f"Index {self.snake_case_name} and its predicate " f"must contain only primitive attribute types"
        )
        lock_group = self.index_attributes[0].field.lock_group
        for attribute in self.all_attributes:
            assert attribute.field.lock_group == lock_group, (
                f"Attributes of index {self.snake_case_name} and its predicates must have "
                f"equal lock groups, but got at least two different ones: "
                f"{lock_group} and {attribute.field.lock_group}"
            )
        assert not (self.is_unique and self.is_repeated), f"Unique index {self.snake_case_name} cannot be repeated"
