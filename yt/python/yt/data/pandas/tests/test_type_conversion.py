import pytest

import functools
import numpy as np
import pandas as pd


import yt.data.pandas
import yt.wrapper
from yt.wrapper.testlib.conftest_helpers import authors
import yt.type_info.typing as ti


@functools.cache
def make_test_data() -> pd.DataFrame:
    int_types = ["int8", "int16", "int32", "int64"] + ["uint8", "uint16", "uint32", "uint64"]
    int_type_columns = {name: [np.iinfo(np.dtype(name)).min, np.iinfo(np.dtype(name)).max] for name in int_types}

    float_types = ["float32", "float64"]
    float_type_columns = {name: [np.finfo(np.dtype(name)).min, np.finfo(np.dtype(name)).max] for name in float_types}

    bool_type = ["bool"]
    bool_type_column = {"bool": [True, False]}

    simple_types = int_types + float_types + bool_type
    simple_type_columns: dict[str, list] = {**int_type_columns, **float_type_columns, **bool_type_column}

    extra_types_columns = {
        # "str": ["a", "b"],
        "string": ["a", "b"],
        # "bytes": [b"a", b"b"],
        "date": pd.Timestamp("2023-01-01"),
        "datetime": pd.Timestamp("2023-01-01 09:00:00"),
        "timestamp": pd.Timestamp(1513393355.5, unit="s"),
        # TODO(YT-23828): Support datetime and timedelta when they will be supported in yt
        # "tz_timestamp": pd.Timestamp(1513393355, unit="s", tz="US/Pacific"),
        # "timedelta": pd.Timedelta(days=1)
    }
    df = pd.DataFrame({**simple_type_columns, **extra_types_columns})
    df = df.astype({name: np.dtype(name) for name in simple_types})
    df = df.astype({"string": "string"})
    return df


@authors("abodrov")
def test_conversion_round_trip():
    test_data = make_test_data()
    test_data = test_data.astype({c: "datetime64[ns]" for c in "date datetime timestamp".split()})

    yt.data.pandas.write_table(test_data, "//tmp/test.arrow")
    read_data = yt.data.pandas.read_table("//tmp/test.arrow")

    assert read_data["datetime"].equals(test_data["datetime"])


@authors("abodrov")
@pytest.mark.parametrize("df_column_name, df_column_type, yt_type", [
    ("int8", np.int8, ti.Int8),
    ("int16", np.int16, ti.Int16),
    ("int32", np.int32, ti.Int32),
    ("int64", np.int64, ti.Int64),
    ("uint8", np.uint8, ti.Uint8),
    ("uint16", np.uint16, ti.Uint16),
    ("uint32", np.uint32, ti.Uint32),
    ("uint64", np.uint64, ti.Uint64),
    ("float32", np.float32, ti.Float),
    ("float64", np.float64, ti.Double),
    ("bool", np.bool_, ti.Bool),
    ("string", pd.StringDtype(), ti.Optional[ti.String])
])
def test_simple_type_conversion(df_column_name, df_column_type, yt_type):
    df = make_test_data()[[df_column_name]]
    assert df.dtypes[df_column_name] == df_column_type
    table_name = f"//tmp/test.{df_column_name}.arrow"

    yt.data.pandas.write_table(df, table_name)
    schema = yt.wrapper.get_table_schema(table_name)
    assert schema.columns[0].type == yt_type

    read_df = yt.data.pandas.read_table(table_name)
    assert read_df.dtypes[df_column_name] == df_column_type


@authors("abodrov")
@pytest.mark.parametrize("df_column_name, df_column_type, yt_type", [
    ("int8", pd.Int8Dtype(), ti.Int8),
    ("int16", pd.Int16Dtype(), ti.Int16),
    ("int32", pd.Int32Dtype(), ti.Int32),
    ("int64", pd.Int64Dtype(), ti.Int64),
    ("uint8", pd.UInt8Dtype(), ti.Uint8),
    ("uint16", pd.UInt16Dtype(), ti.Uint16),
    ("uint32", pd.UInt32Dtype(), ti.Uint32),
    ("uint64", pd.UInt64Dtype(), ti.Uint64),
    ("float32", pd.Float32Dtype(), ti.Float),
    ("float64", pd.Float64Dtype(), ti.Double),
    ("bool", pd.BooleanDtype(), ti.Bool),
    ("string", pd.StringDtype(), ti.String)
])
def test_optional_type_conversion(df_column_name, df_column_type, yt_type):
    df = make_test_data()[[df_column_name]].astype({df_column_name: df_column_type})
    df[df_column_name][1] = None
    table_name = f"//tmp/test.optional_{df_column_name}.arrow"

    yt.data.pandas.write_table(df, table_name)
    schema = yt.wrapper.get_table_schema(table_name)
    assert schema.columns[0].type == ti.Optional[yt_type]

    read_df = yt.data.pandas.read_table(table_name)
    assert read_df.dtypes[df_column_name] == df_column_type
