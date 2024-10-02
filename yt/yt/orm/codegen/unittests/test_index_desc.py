from yt.yt.orm.codegen.model.index import OrmIndex, OrmIndexCppMode

from yt_proto.yt.orm.client.proto import object_pb2

from dataclasses import dataclass, field
from typing import List, Dict, Optional

import pytest


@dataclass
class OrmIndexOptionMock:
    object_type_snake_case_name: str
    camel_case_name: str
    snake_case_name: str
    hash_expression: str
    key: List[str]
    mode: object_pb2.TIndexOption.EIndexMode
    unique: bool
    predicate: Optional[str] = None
    custom_table_name: str = ""


@dataclass
class OrmFieldMock:
    is_repeated: bool = False
    is_primitive: bool = True
    foreign_object_type: Optional[str] = None
    index_over_reference_table: Optional[OrmIndex] = None
    lock_group: str = "api"
    deprecated: bool = False

    # This hack discards the stored values because comparison of recursive structures is not fun.
    @property
    def indexed_by(self) -> list[OrmIndex]:
        return []

    def get_lock_group(self):
        return self.lock_group


@dataclass
class OrmAttributeMock:
    name: str
    column_field: OrmFieldMock = field(default_factory=OrmFieldMock)
    field: OrmFieldMock = field(default_factory=OrmFieldMock)
    full_path: Optional[str] = None

    def __hash__(self):
        return hash(self.name)


@dataclass
class OrmObjectMock:
    snake_case_name: str
    camel_case_name: str
    parent: Optional["OrmObject"] = None
    mock_attributes: Dict[str, "OrmAttributeMock"] = field(default_factory=dict)

    def resolve_attribute_by_path(self, path):
        if path in self.mock_attributes:
            return self.mock_attributes[path]
        return OrmAttributeMock(name=path)


class TestOrmIndex:
    @pytest.fixture
    def index_option(self):
        return OrmIndexOptionMock(
            object_type_snake_case_name="book",
            camel_case_name="BooksByYear",
            snake_case_name="books_by_year",
            hash_expression="farm_hash([book_id])",
            key=["/spec/year"],
            mode=object_pb2.TIndexOption.EIndexMode.IM_ENABLED,
            unique=False,
        )

    @pytest.fixture
    def object_type(self):
        return OrmObjectMock(
            snake_case_name="book",
            camel_case_name="Book",
        )

    @pytest.fixture
    def expected(self, object_type):
        return OrmIndex(
            camel_case_name="BooksByYear",
            snake_case_name="books_by_year",
            hash_expression="farm_hash([book_id])",
            object_type_snake_case_name="book",
            object_type_camel_case_name="Book",
            object=object_type,
            index_attributes=[OrmAttributeMock(name="/spec/year")],
            mode=OrmIndexCppMode.ENABLED,
            is_unique=False,
        )

    def test_scalar_single(self, index_option, object_type, expected):
        assert expected == OrmIndex.make(index_option, object_type)

    def test_scalar_several(self, index_option, object_type, expected):
        index_option.key = ["/first", "/second"]
        expected.index_attributes = [
            OrmAttributeMock(name="/first"),
            OrmAttributeMock(name="/second"),
        ]
        assert expected == OrmIndex.make(index_option, object_type)

    def test_repeated(self, index_option, object_type, expected):
        index_option.key = ["/spec/keywords"]
        attribute = OrmAttributeMock(
            name="/spec/keywords",
            field=OrmFieldMock(is_repeated=True),
        )
        object_type.mock_attributes["/spec/keywords"] = attribute
        expected.index_attributes = [attribute]
        expected.is_repeated = True

        assert expected == OrmIndex.make(index_option, object_type)

    def test_one_to_many(self, index_option, object_type, expected):
        index_option.key = ["/spec/editor_id"]
        attribute = OrmAttributeMock(
            name="/spec/editor_id",
            field=OrmFieldMock(foreign_object_type="Editor"),
            full_path="/spec/editor_id",
        )
        object_type.mock_attributes["/spec/editor_id"] = attribute
        expected.index_attributes = [attribute]

        assert expected == OrmIndex.make(index_option, object_type)
        assert expected == attribute.field.index_over_reference_table
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_duplicate_key(self, index_option, object_type):
        index_option.key = ["/duplicate", "/duplicate"]
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_difference_name(self, index_option, object_type):
        index_option.object_type_snake_case_name = "not_a_book"
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_empty_key(self, index_option, object_type):
        index_option.key = []
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_repeated_several(self, index_option, object_type):
        index_option.key = ["/first", "/second"]
        object_type.mock_attributes["/first"] = OrmAttributeMock(
            name="/first", field=OrmFieldMock(is_repeated=True)
        )
        object_type.mock_attributes["/second"] = OrmAttributeMock(
            name="/second", field=OrmFieldMock(is_repeated=True)
        )
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_non_primitive(self, index_option, object_type):
        index_option.key = ["/spec/design"]
        object_type.mock_attributes["/spec/design"] = OrmAttributeMock(
            name="/spec/design",
            field=OrmFieldMock(is_primitive=False),
        )
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_difference_lock(self, index_option, object_type):
        index_option.key = ["/first", "/second"]
        object_type.mock_attributes["/first"] = OrmAttributeMock(
            name="/first",
            field=OrmFieldMock(lock_group="api"),
        )
        object_type.mock_attributes["/second"] = OrmAttributeMock(
            name="/second",
            field=OrmFieldMock(lock_group="web"),
        )
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_unique_repeated(self, index_option, object_type):
        index_option.key = ["/spec/keywords"]
        index_option.unique = True
        attribute = OrmAttributeMock(
            name="/spec/keywords",
            field=OrmFieldMock(is_repeated=True),
        )
        object_type.mock_attributes["/spec/keywords"] = attribute
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_predicate(self, index_option, object_type, expected):
        index_option.key = ["/first"]
        index_option.predicate = '[/first] > 100 and [/nested/second] = "500" or [/first] < 50'
        scalar_attribute = OrmAttributeMock(name="/first")
        repeated_attribute = OrmAttributeMock(
            name="/nested/second", field=OrmFieldMock(is_repeated=True)
        )
        object_type.mock_attributes["/nested/second"] = repeated_attribute
        expected.index_attributes = [scalar_attribute]
        expected.predicate = index_option.predicate
        expected.predicate_attributes = [scalar_attribute, repeated_attribute]
        assert expected == OrmIndex.make(index_option, object_type)

    def test_fail_predicate_with_deprecated_attribute(self, index_option, object_type):
        index_option.predicate = "[/first] > 100"
        repeated_attribute = OrmAttributeMock(
            name="/first", column_field=OrmFieldMock(deprecated=True)
        )
        object_type.mock_attributes["/first"] = repeated_attribute
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_predicate_with_illformed_attributes(self, index_option, object_type):
        index_option.predicate = "/first > 100 and 499 < /second < 501"
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_predicate_with_not_primitive_attribute(self, index_option, object_type):
        index_option.predicate = "[/first] > 100"
        repeated_attribute = OrmAttributeMock(name="/first", field=OrmFieldMock(is_primitive=False))
        object_type.mock_attributes["/first"] = repeated_attribute
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)

    def test_fail_difference_lock_with_predicate(self, index_option, object_type):
        index_option.key = ["/first"]
        index_option.predicate = '[/nested/second] = "500"'
        object_type.mock_attributes["/first"] = OrmAttributeMock(
            name="/first",
            field=OrmFieldMock(lock_group="api"),
        )
        object_type.mock_attributes["/nested/second"] = OrmAttributeMock(
            name="/nested/second",
            field=OrmFieldMock(lock_group="web"),
        )
        with pytest.raises(AssertionError):
            OrmIndex.make(index_option, object_type)
