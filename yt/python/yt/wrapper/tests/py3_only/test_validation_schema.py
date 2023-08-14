import io
import pytest
import typing

from yt.testlib import authors
from yt.testlib.helpers import set_config_option
from yt.wrapper.config import get_config

import yt.yson.yson_types

from yt.wrapper.schema import (
    yt_dataclass,
    _create_row_py_schema,
    _SchemaRuntimeCtx,
    TableSchema,
)

from yt.wrapper.format import StructuredSkiffFormat

import yt.wrapper as yt
import yt.type_info as ti


class TestDefaultOptionalNotOptional(object):

    @yt_dataclass
    class StructRequired:
        int_field: int

    @yt_dataclass
    class StructOptional:
        int_field: typing.Optional[int]

    @yt_dataclass
    class StructTupleRequired:
        tuple_field: typing.Tuple[int]
    py_data_tuple_required_value = (15, )
    py_data_tuple_required = StructTupleRequired(tuple_field=py_data_tuple_required_value)
    yt_schema_tuple_required = TableSchema()
    yt_schema_tuple_required.add_column(name="tuple_field", type=ti.Tuple[ti.Int64])
    yt_schema_tuple_required.strict = True
    yt_schema_tuple_required.unique_keys = False
    stream = io.BytesIO()

    StructuredSkiffFormat([_create_row_py_schema(StructTupleRequired, yt_schema_tuple_required)], for_reading=False).dump_rows([py_data_tuple_required], stream)
    yt_data_tuple_required = stream.getvalue()

    @yt_dataclass
    class StructTupleOptional:
        tuple_field: typing.Tuple[typing.Optional[int]]
    py_data_tuple_empty_value = (None,)
    py_data_tuple_optional_empty = StructTupleOptional(tuple_field=py_data_tuple_empty_value)
    py_data_tuple_optional = StructTupleOptional(tuple_field=py_data_tuple_required_value)
    yt_schema_tuple_optional = TableSchema()
    yt_schema_tuple_optional.add_column(name="tuple_field", type=ti.Tuple[ti.Optional[ti.Int64]])
    yt_schema_tuple_optional.strict = True
    yt_schema_tuple_optional.unique_keys = False
    stream = io.BytesIO()
    StructuredSkiffFormat([_create_row_py_schema(StructTupleOptional, yt_schema_tuple_optional)], for_reading=False).dump_rows([py_data_tuple_optional], stream)
    yt_data_tuple_optional = stream.getvalue()
    stream = io.BytesIO()
    StructuredSkiffFormat([_create_row_py_schema(StructTupleOptional, yt_schema_tuple_optional)], for_reading=False).dump_rows([py_data_tuple_optional_empty], stream)
    yt_data_tuple_optional_empty = stream.getvalue()

    @yt_dataclass
    class StructListRequired:
        list_field: typing.List[int]
    py_data_list_required_value = [15]
    py_data_list_required = StructListRequired(list_field=py_data_list_required_value)

    @yt_dataclass
    class StructListOptional:
        list_field: typing.List[typing.Optional[int]]
    py_data_list_empty_value = [None]
    py_data_list_optional_empty = StructListOptional(list_field=py_data_list_empty_value)
    py_data_list_optional = StructListOptional(list_field=py_data_list_required_value)
    yt_schema_list_optional = TableSchema()
    yt_schema_list_optional.add_column(name="list_field", type=ti.List[ti.Optional[ti.Int64]])
    yt_schema_list_optional.strict = True
    yt_schema_list_optional.unique_keys = False
    stream = io.BytesIO()
    StructuredSkiffFormat([_create_row_py_schema(StructListOptional, yt_schema_list_optional)], for_reading=False).dump_rows([py_data_list_optional], stream)
    yt_data_list_optional = stream.getvalue()
    stream = io.BytesIO()
    StructuredSkiffFormat([_create_row_py_schema(StructListOptional, yt_schema_list_optional)], for_reading=False).dump_rows([py_data_list_optional_empty], stream)
    yt_data_list_optional_empty = stream.getvalue()

    yt_schema_optional = TableSchema()
    yt_schema_optional.add_column(name="int_field", type=ti.Optional[ti.Int64])
    yt_schema_optional.strict = True
    yt_schema_optional.unique_keys = False

    yt_schema_required = TableSchema()
    yt_schema_required.add_column(name="int_field", type=ti.Int64)
    yt_schema_required.strict = True
    yt_schema_required.unique_keys = False

    py_data_value = 12345
    py_data_empty_value = None

    py_data_required = StructRequired(int_field=py_data_value)
    py_data_optional = StructOptional(int_field=py_data_value)
    py_data_optional_empty = StructOptional(int_field=py_data_empty_value)

    stream = io.BytesIO()
    StructuredSkiffFormat([_create_row_py_schema(StructRequired, yt_schema_required)], for_reading=False).dump_rows([py_data_required], stream)
    yt_data_required = stream.getvalue()

    stream = io.BytesIO()
    StructuredSkiffFormat([_create_row_py_schema(StructOptional, yt_schema_optional)], for_reading=False).dump_rows([py_data_optional], stream)
    yt_data_optional = stream.getvalue()

    stream = io.BytesIO()
    StructuredSkiffFormat([_create_row_py_schema(StructOptional, yt_schema_optional)], for_reading=False).dump_rows([py_data_optional_empty], stream)
    yt_data_optional_empty = stream.getvalue()

    @pytest.mark.parametrize("py_struct, yt_struct, is_read, exception_str", [
        # check reading on start
        (StructRequired, yt_schema_optional, True, "is non-nullable in yt_dataclass and optional in table schema"),
        (StructOptional, yt_schema_optional, True, None),
        (StructRequired, yt_schema_required, True, None),
        (StructOptional, yt_schema_required, True, None),
        (StructRequired, None, True, None),
        (StructOptional, None, True, None),
        # check writing on start
        (StructRequired, yt_schema_optional, False, None),
        (StructOptional, yt_schema_optional, False, None),
        (StructRequired, yt_schema_required, False, None),
        (StructOptional, yt_schema_required, False, "is optional in yt_dataclass and required in table schema"),
        (StructRequired, None, True, None),
        (StructOptional, None, True, None),
    ])
    @authors("denvr")
    def test_strict_required_optional_on_start(self, py_struct, yt_struct, is_read, exception_str):
        py_schema = _create_row_py_schema(py_struct, yt_struct)

        if exception_str:
            with pytest.raises(yt.YtError) as ex:
                format = StructuredSkiffFormat([py_schema], for_reading=is_read)
            assert exception_str in str(ex.value)
        else:
            format = StructuredSkiffFormat([py_schema], for_reading=is_read)
            assert format

    @pytest.mark.parametrize("py_struct, yt_struct, yt_data, yt_data_value, exception_str", [
        (StructRequired, yt_schema_required, yt_data_required, py_data_value, None),
        (StructOptional, yt_schema_required, yt_data_required, py_data_value, None),
        (StructRequired, yt_schema_optional, yt_data_optional, py_data_value, "is non-nullable in yt_dataclass and optional in table schema"),
        (StructRequired, yt_schema_optional, yt_data_optional_empty, py_data_empty_value, "is non-nullable in yt_dataclass and optional in table schema"),
        (StructOptional, yt_schema_optional, yt_data_optional, py_data_value, None),
        (StructOptional, yt_schema_optional, yt_data_optional_empty, py_data_empty_value, None),
        (StructRequired, None, yt_data_required, py_data_value, None),
        (StructOptional, None, yt_data_optional, py_data_value, None),
        (StructOptional, None, yt_data_optional_empty, py_data_empty_value, None),
        # impossible case
        (StructRequired, None, yt_data_optional, 3160321, None),
        (StructOptional, None, yt_data_required, -1, "Skiff to Python conversion failed"),
    ])
    @authors("denvr")
    def test_strict_required_optional_on_reading(self, py_struct, yt_struct, yt_data, yt_data_value, exception_str):
        py_schema = _create_row_py_schema(py_struct, yt_struct)

        if exception_str:
            with pytest.raises(yt.YtError) as ex:
                format = StructuredSkiffFormat([py_schema], for_reading=True)
                record = format.load_rows(io.BytesIO(yt_data)).__next__()
            assert exception_str in str(ex.value)
        else:
            format = StructuredSkiffFormat([py_schema], for_reading=True)
            record = format.load_rows(io.BytesIO(yt_data)).__next__()
            assert record
            assert type(record) == py_struct
            assert record.int_field == yt_data_value

    @pytest.mark.parametrize("test_name, py_struct, yt_struct, is_read, exception_str", [
        # check reading on start
        ("yt_optional->py_required", StructRequired, yt_schema_optional, True, None),
        ("yt_optional->py_optional", StructOptional, yt_schema_optional, True, None),
        ("yt_required->py_required", StructRequired, yt_schema_required, True, None),
        ("yt_required->py_optional", StructOptional, yt_schema_required, True, None),
        ("yt_None->py_required", StructRequired, None, True, None),
        ("yt_None->py_optional", StructOptional, None, True, None),
        # check writing on start
        ("py_required->yt_optional", StructRequired, yt_schema_optional, False, None),
        ("py_optional->yt_optional", StructOptional, yt_schema_optional, False, None),
        ("py_required->yt_required", StructRequired, yt_schema_required, False, None),
        ("py_optional->yt_required", StructOptional, yt_schema_required, False, None),
        ("py_required->yt_None", StructRequired, None, True, None),
        ("py_optional->yt_None", StructOptional, None, True, None),
    ])
    @authors("denvr")
    def test_validation_required_optional_on_start(self, test_name, py_struct, yt_struct, is_read, exception_str):
        assert not get_config(client=None)["runtime_type_validation"]
        with set_config_option("runtime_type_validation", True):
            assert get_config(client=None)["runtime_type_validation"]
            py_schema = _SchemaRuntimeCtx().set_validation_mode_from_config(get_config(client=None)).create_row_py_schema(py_struct, yt_struct)

            if exception_str:
                with pytest.raises(yt.YtError) as ex:
                    format = StructuredSkiffFormat([py_schema], for_reading=is_read)
                assert exception_str in str(ex.value)
            else:
                format = StructuredSkiffFormat([py_schema], for_reading=is_read)
                assert format

    @pytest.mark.parametrize("test_name, validate_runtime, py_struct, yt_struct, yt_data, yt_data_value, exception_str", [
        # validate on INIT
        # same or "greater" types
        ("init YT:int -> PY:int -> ok", False, StructRequired, yt_schema_required, yt_data_required, py_data_required, None),
        ("init YT:int -> PY:int(Optional) -> ok", False, StructOptional, yt_schema_required, yt_data_required, py_data_optional, None),
        ("init YT:int(Optional) -> PY:int(Optional) -> ok", False, StructOptional, yt_schema_optional, yt_data_optional, py_data_optional, None),
        ("init YT:None(Optional) -> PY:None(Optional) -> ok", False, StructOptional, yt_schema_optional, yt_data_optional_empty, py_data_optional_empty, None),
        ("init YT_no_schema:int -> PY:int -> ok", False, StructRequired, None, yt_data_required, py_data_required, None),
        ("init YT_no_schema:int(Optional) -> PY:int(Optional) -> ok", False, StructOptional, None, yt_data_optional, py_data_optional, None),
        ("init YT_no_schema:None(Optional) -> PY:Optional[None] -> ok", False, StructOptional, None, yt_data_optional_empty, py_data_optional_empty, None),
        ("init YT:Tuple[int] -> PY:Tuple[int] -> ok", False, StructTupleRequired, yt_schema_tuple_required, yt_data_tuple_required, py_data_tuple_required, None),
        ("init YT:Tuple[None(Optional)] -> PY:Tuple[None(Optional)] -> ok", False, StructTupleOptional, yt_schema_tuple_optional, yt_data_tuple_optional_empty, py_data_tuple_optional_empty, None),
        ("init YT:Tuple[int(Optional)] -> PY:Tuple[int(Optional)] -> ok", False, StructTupleOptional, yt_schema_tuple_optional, yt_data_tuple_optional, py_data_tuple_optional, None),
        ("init YT:List[None(Optional)] -> PY:List[None(Optional)] -> !", False, StructListOptional, yt_schema_list_optional, yt_data_list_optional_empty, py_data_list_optional_empty, None),
        # "less" types
        ("init YT:int(Optional) -> PY:int -> !", False, StructRequired, yt_schema_optional, yt_data_optional, py_data_required, "Schema and yt_dataclass mismatch"),
        ("init YT:None(Optional) -> PY:int -> !", False, StructRequired, yt_schema_optional, yt_data_optional_empty, None, "Schema and yt_dataclass mismatch"),
        ("init YT:Tuple[int(Optional)] -> PY:Tuple[int] -> !", False, StructTupleRequired, yt_schema_tuple_optional, yt_data_tuple_optional, py_data_tuple_required, "Schema and yt_dataclass mismatch"),  # noqa: E501
        ("init YT:Tuple[None(Optional)] -> PY:Tuple[int] -> !", False, StructTupleRequired, yt_schema_tuple_optional, yt_data_tuple_optional_empty, py_data_tuple_required, "Schema and yt_dataclass mismatch"),  # noqa: E501
        ("init YT:List[None(Optional)] -> PY:List[int] -> !", False, StructListRequired, yt_schema_list_optional, yt_data_list_optional_empty, py_data_list_required, "Schema and yt_dataclass mismatch"),  # noqa: E501

        # validate on RUNTIME
        # same or "greater" types
        ("runtime YT:int -> PY:int -> ok", True, StructRequired, yt_schema_required, yt_data_required, py_data_required, None),
        ("runtime YT:int -> PY:int(Optional) -> ok", True, StructOptional, yt_schema_required, yt_data_required, py_data_optional, None),
        ("runtime YT:int(Optional) -> PY:int(Optional) -> ok", True, StructOptional, yt_schema_optional, yt_data_optional, py_data_optional, None),
        ("runtime YT:None(Optional) -> PY:None(Optional) -> ok", True, StructOptional, yt_schema_optional, yt_data_optional_empty, py_data_optional_empty, None),
        ("runtime YT_no_schema:int -> PY:int -> ok", True, StructRequired, None, yt_data_required, py_data_required, None),
        ("runtime YT_no_schema:int(Optional) -> PY:int(Optional) -> ok", True, StructOptional, None, yt_data_optional, py_data_optional, None),
        ("runtime YT_no_schema:None(Optional) -> PY:Optional[None] -> ok", True, StructOptional, None, yt_data_optional_empty, py_data_optional_empty, None),
        ("runtime YT:Tuple[int] -> PY:Tuple[int] -> ok", True, StructTupleRequired, yt_schema_tuple_required, yt_data_tuple_required, py_data_tuple_required, None),
        ("runtime YT:Tuple[None(Optional)] -> PY:Tuple[None(Optional)] -> ok", True, StructTupleOptional, yt_schema_tuple_optional, yt_data_tuple_optional_empty, py_data_tuple_optional_empty, None),
        ("runtime YT:Tuple[int(Optional)] -> PY:Tuple[int(Optional)] -> ok", True, StructTupleOptional, yt_schema_tuple_optional, yt_data_tuple_optional, py_data_tuple_optional, None),
        # "less" types
        ("runtime YT:int(Optional) -> PY:int -> ok", True, StructRequired, yt_schema_optional, yt_data_optional, py_data_required, None),
        ("runtime YT:None(Optional) -> PY:int -> !", True, StructRequired, yt_schema_optional, yt_data_optional_empty, None, "Skiff to Python conversion failed"),
        ("runtime YT:Tuple[Optional[int]] -> PY:Tuple[int] -> ok", True, StructTupleRequired, yt_schema_tuple_optional, yt_data_tuple_optional, py_data_tuple_required, None),
        ("runtime YT:Tuple[Optional[none]] -> PY:Tuple[int] -> !", True, StructTupleRequired, yt_schema_tuple_optional, yt_data_tuple_optional_empty, py_data_tuple_required, "Skiff to Python conversion failed"),  # noqa: E501
        # impossible case
        ("runtime YT_no_schema:int(Optional) -> PY:int(wrong int) -> ok", True, StructRequired, None, yt_data_optional, StructRequired(int_field=3160321), None),
        ("runtime YT_no_schema:int -> PY:int(Optional) -> !", True, StructOptional, None, yt_data_required, None, "Skiff to Python conversion failed"),
        ("runtime YT_no_schema:None(Optional) -> PY:int -> !", True, StructRequired, None, yt_data_optional_empty, None, "Skiff to Python conversion failed"),
    ])
    @authors("denvr")
    def test_validation_required_optional_on_reading(self, test_name, validate_runtime, py_struct, yt_struct, yt_data, yt_data_value, exception_str):
        with set_config_option("runtime_type_validation", validate_runtime):
            py_schema = _SchemaRuntimeCtx().set_validation_mode_from_config(get_config(client=None)).create_row_py_schema(py_struct, yt_struct)

            if exception_str:
                with pytest.raises(yt.YtError) as ex:
                    format = StructuredSkiffFormat([py_schema], for_reading=True)
                    record = format.load_rows(io.BytesIO(yt_data)).__next__()
                assert exception_str in str(ex.value)
            else:
                format = StructuredSkiffFormat([py_schema], for_reading=True)
                record = format.load_rows(io.BytesIO(yt_data)).__next__()
                assert record
                assert type(record) == py_struct
                assert record == yt_data_value

    @pytest.mark.parametrize("test_name, validate_runtime, py_struct, yt_struct, yt_data, py_data_value, exception_str", [
        # validate on RUNTIME
        # same or "greater" types
        ("runtime PY:int -> YT:int -> ok", True, StructRequired, yt_schema_required, yt_data_required, py_data_required, None),
        ("runtime PY:int -> YT:int(Optional) -> ok", True, StructRequired, yt_schema_optional, yt_data_optional, py_data_required, None),
        ("runtime PY:int(Optional) -> YT:int(Optional) -> ok", True, StructOptional, yt_schema_optional, yt_data_optional, py_data_optional, None),
        ("runtime PY:None(Optional) -> YT:None(Optional) -> ok", True, StructOptional, yt_schema_optional, yt_data_optional_empty, py_data_optional_empty, None),
        ("runtime PY_no_schema:int -> YT:int -> ok", True, StructRequired, None, yt_data_required, py_data_required, None),
        ("runtime PY_no_schema:int(Optional) -> YT:int(Optional), True -> ok", True, StructOptional, None, yt_data_optional, py_data_optional, None),
        ("runtime PY_no_schema:None(Optional) -> YT:None(Optional), True -> ok", True, StructOptional, None, yt_data_optional_empty, py_data_optional_empty, None),

        # "less" types
        ("runtime PY:int(Optional) -> YT:int -> ok", True, StructOptional, yt_schema_required, yt_data_required, py_data_optional, None),
        ("runtime PY:None(Optional) -> YT:int -> !", True, StructOptional, yt_schema_required, yt_data_required, py_data_optional_empty, "None in required for field \"TestDefaultOptionalNotOptional.StructOptional.int_field.<optional-element>\""),  # noqa: E501

    ])
    @authors("denvr")
    def test_validation_required_optional_on_writing(self, test_name, validate_runtime, py_struct, yt_struct, yt_data, py_data_value, exception_str):
        with set_config_option("runtime_type_validation", validate_runtime):
            py_schema = _SchemaRuntimeCtx().set_validation_mode_from_config(get_config(client=None)).create_row_py_schema(py_struct, yt_struct)

            if exception_str:
                with pytest.raises(yt.YtError) as ex:
                    format = StructuredSkiffFormat([py_schema], for_reading=False)
                    stream = io.BytesIO()
                    format.dump_rows([py_data_value], stream)
                assert exception_str in str(ex.value)
            else:
                format = StructuredSkiffFormat([py_schema], for_reading=False)
                stream = io.BytesIO()
                format.dump_rows([py_data_value], stream)
                assert stream.getvalue() == yt_data

    @pytest.mark.parametrize("test_name, validate_runtime, py_struct, yt_struct, yt_data, yt_data_value, exception_str", [
        # validate on INIT
        # same or "greater" types
        ("init YT:int -> PY:int -> ok", False, StructRequired, yt_schema_required, yt_data_required, py_data_required, None),
        ("init YT:int -> PY:int(Optional) -> ok", False, StructOptional, yt_schema_required, yt_data_required, py_data_optional, None),
        ("init YT:int(Optional) -> PY:int(Optional) -> ok", False, StructOptional, yt_schema_optional, yt_data_optional, py_data_optional, None),
        # "less" types
        ("init YT:int(Optional) -> PY:int -> !", False, StructRequired, yt_schema_optional, yt_data_optional, py_data_required, "Schema and yt_dataclass mismatch"),

        # validate on RUNTIME
        # same or "greater" types
        ("runtime YT:int -> PY:int -> ok", True, StructRequired, yt_schema_required, yt_data_required, py_data_required, None),
        ("runtime YT:int -> PY:int(Optional) -> ok", True, StructOptional, yt_schema_required, yt_data_required, py_data_optional, None),
        ("runtime YT:int(Optional) -> PY:int(Optional) -> ok", True, StructOptional, yt_schema_optional, yt_data_optional, py_data_optional, None),
        # "less" types
        ("runtime YT:int(Optional) -> PY:int -> !", True, StructRequired, yt_schema_optional, yt_data_optional, py_data_required, "Schema and yt_dataclass mismatch"),
        # impossible case
        ("runtime YT_no_schema:int(Optional) -> PY:int(wrong int) -> ok", True, StructRequired, None, yt_data_optional, StructRequired(int_field=3160321), None),
        ("runtime YT_no_schema:int -> PY:int(Optional) -> !", True, StructOptional, None, yt_data_required, None, "Skiff to Python conversion failed"),
        ("runtime YT_no_schema:None(Optional) -> PY:int -> !", True, StructRequired, None, yt_data_optional_empty, None, "Skiff to Python conversion failed"),
    ])
    @authors("denvr")
    def test_validation_oldwrapper_required_optional_on_reading(self, test_name, validate_runtime, py_struct, yt_struct, yt_data, yt_data_value, exception_str):
        py_schema = _create_row_py_schema(py_struct, yt_struct)

        with set_config_option("runtime_type_validation", validate_runtime):
            if exception_str:
                with pytest.raises(yt.YtError) as ex:
                    format = StructuredSkiffFormat([py_schema], for_reading=True)
                    del py_schema._schema_runtime_context
                    record = format.load_rows(io.BytesIO(yt_data)).__next__()
                assert exception_str in str(ex.value)
            else:
                format = StructuredSkiffFormat([py_schema], for_reading=True)
                del py_schema._schema_runtime_context
                record = format.load_rows(io.BytesIO(yt_data)).__next__()
                assert record
                assert type(record) == py_struct
                assert record == yt_data_value
