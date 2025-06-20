import numpy as np
import pandas as pd
from pandas.api.extensions import ExtensionDtype
import yt.wrapper as yt
import yt.type_info.typing as ti

from typing import Mapping

# pd dtype <-> yt type_info type
# https://pandas.pydata.org/docs/user_guide/basics.html#basics-dtypes
# https://numpy.org/doc/stable/reference/arrays.dtypes.html
# https://yt.yandex-team.ru/docs/description/storage/data_types
_TYPE_MATCH = [
    # Signed types
    *[(np.dtype(f"int{i}"), ti.deserialize_yson(f"int{i}")) for i in (8, 16, 32, 64)],
    (pd.Int8Dtype(), ti.Optional[ti.Int8]),
    (pd.Int16Dtype(), ti.Optional[ti.Int16]),
    (pd.Int32Dtype(), ti.Optional[ti.Int32]),
    (pd.Int64Dtype(), ti.Optional[ti.Int64]),
    # Unsigned types
    *[(np.dtype(f"uint{i}"), ti.deserialize_yson(f"uint{i}")) for i in (8, 16, 32, 64)],
    (pd.UInt8Dtype(), ti.Optional[ti.Uint8]),
    (pd.UInt16Dtype(), ti.Optional[ti.Uint16]),
    (pd.UInt32Dtype(), ti.Optional[ti.Uint32]),
    (pd.UInt64Dtype(), ti.Optional[ti.Uint64]),
    # Float point types
    (np.dtype("float64"), ti.Double),
    (pd.Float64Dtype(), ti.Optional[ti.Double]),
    (np.dtype("float32"), ti.Float),
    (pd.Float32Dtype(), ti.Optional[ti.Float]),
    # Boolean types
    (np.dtype("bool"), ti.Bool),
    (pd.BooleanDtype(), ti.Optional[ti.Bool]),
    # String types
    (pd.StringDtype(), ti.String),
    (pd.StringDtype(), ti.Optional[ti.String]),
    # TODO(YT-23828): Support datetime and timedelta when they will be supported in yt
    (np.dtype("datetime64[s]"), ti.Timestamp),
    (np.dtype("datetime64[ms]"), ti.Timestamp),
    (np.dtype("datetime64[ns]"), ti.Timestamp),
    # ("datetime64[D]", "date"),
    # ("timedelta64[ms]", "interval")
]
_PD_DTYPE_TO_TI_TYPE = {dtype: ti_type for dtype, ti_type in _TYPE_MATCH}
_YT_TYPE_TO_PD_DTYPE = {ti_type: dtype for dtype, ti_type in _TYPE_MATCH}


def extract_table_schema_from_dataframe(df: pd.DataFrame) -> yt.schema.TableSchema:
    pd_schema: Mapping[str, np.dtype | ExtensionDtype] = df.dtypes
    table_schema = yt.schema.TableSchema()
    for name, dtype in pd_schema.items():
        if dtype == np.object_:
            raise ValueError(
                f"Column {name!r} has unsupported dtype 'object'.\n"
                "It is recommended to use df.convert_dtypes() to automatically infer column types. "
                f"If this column contains strings - you may convert it to string dtype explicitly: df = df.astype({({name: 'string'})!r}).\n"
                "You may want to enable automatic string conversion via `pd.options.future.infer_string = True`\n"
                "If this is not a string column that contains None - you may convert it to nullable dtype explicitly, "
                "see https://pandas.pydata.org/docs/user_guide/missing_data.html.\n"
            )
        ti_type = _PD_DTYPE_TO_TI_TYPE.get(dtype)
        if ti_type is None:
            raise ValueError(f"Column {name!r} has unsupported dtype {dtype}.")
        table_schema.add_column(name, ti_type)
    return table_schema


def table_schema_to_dtype_mapping(table_schema: yt.schema.TableSchema) -> Mapping[str, np.dtype | ExtensionDtype]:
    if not table_schema.strict:
        raise ValueError(
            "Table must have strict schema. "
            "You can explicitly select list of columns from schema via yt.TablePath(path, columns=...)"
        )
    dtype_mapping = {}
    for column in table_schema.columns:
        dtype = _YT_TYPE_TO_PD_DTYPE.get(column.type)
        if dtype is None:
            raise NotImplementedError(f"Type {column.type!s} of column {column.name!r} is not supported yet")
        dtype_mapping[column.name] = dtype
    return dtype_mapping


def table_schema_to_arrow_pandas_metadata(table_schema: yt.schema.TableSchema) -> dict:
    metadata = {
        "index_columns": [],
        "columns": [],
    }
    columns_metadata = metadata["columns"]
    dtype_mapping = table_schema_to_dtype_mapping(table_schema)
    for column_name, dtype in dtype_mapping.items():
        # NOTE: Current pandas_type is just a string and is not consistent with pyarrow-generated pandas_type
        # It doesn't seem to be an issue - pandas_type seems to be almost completely ignored, but the key is expected:
        # https://github.com/apache/arrow/blob/main/python/pyarrow/pandas_compat.py#L1141
        columns_metadata.append({"name": column_name, "pandas_type": dtype.name, "numpy_type": dtype.name})
    return metadata


def read_table_schema(client: yt.YtClient, table_path: str | yt.TablePath) -> yt.schema.TableSchema:
    table_path = yt.TablePath(table_path, client=client)
    schema = client.get_table_schema(table_path)
    if (table_columns := table_path.columns) is not None:
        schema.columns = [c for c in schema.columns if c.name in table_path.columns]
        if len(table_columns) != len(schema.columns):
            missing_columns = sorted(set(table_columns) - {c.name for c in schema.columns})
            raise ValueError(f"Columns {missing_columns} are missing from schema, unable to determine their type")
        schema.strict = True
    return schema
