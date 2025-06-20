import pytest

import numpy as np
import pandas as pd


import yt.data.pandas as yt_pd
import yt.wrapper as yt
from yt.wrapper.schema import TableSchema
from yt.wrapper.testlib.conftest_helpers import authors
import yt.type_info.typing as ti


@authors("abodrov")
def test_read_table_with_simple_strict_schema():
    table_name = "//tmp/test_read_table_with_simple_strict_schema"
    yt.create_table(table_name, attributes={"schema": TableSchema().add_column('a', ti.Int16)})
    # NOTE(YT-23898): Empty tables are not read as empty df :(
    yt.write_table(table_name, [{'a': 1}])
    df = yt_pd.read_table(table_name)
    assert df.dtypes['a'] == np.dtype('int16')


@authors("abodrov")
def test_read_table_with_non_strict_schema_columns_specified():
    table_name = "//tmp/test_read_table_with_non_strict_schema_columns_specified"
    yt.create_table(table_name, attributes={"schema": TableSchema(strict=False).add_column('a', ti.Int16)})
    # NOTE(YT-23898): Empty tables are not read as empty df :(
    yt.write_table(table_name, [{'a': 1, 'b': 'not strict'}])
    df = yt_pd.read_table(yt.TablePath(table_name, columns=['a']))
    assert df.dtypes['a'] == np.dtype('int16')


@authors("abodrov")
def test_read_non_strict_schema_raises():
    table_name = "//tmp/test_read_non_strict_schema_raises"
    yt.create_table(table_name, attributes={"schema": TableSchema(strict=False).add_column('a', ti.Int16)})
    with pytest.raises(ValueError):
        yt_pd.read_table(table_name)


@authors("abodrov")
def test_read_missing_columns_raises():
    table_name = "//tmp/test_read_missing_columns_raises"
    yt.create_table(table_name, attributes={"schema": TableSchema().add_column('a', ti.Int16)})
    with pytest.raises(ValueError):
        yt_pd.read_table(yt.TablePath(table_name, columns=['b']))


@authors("abodrov")
def test_write_simple():
    table_name = "//tmp/test_write_simple"
    df = pd.DataFrame({'a': [1, 2, 3]})
    yt_pd.write_table(df, table_name)
    assert yt.exists(table_name)
    assert yt.get_table_schema(table_name) == TableSchema().add_column('a', ti.Int64)


@authors("abodrov")
def test_write_schema():
    table_name = "//tmp/test_write_schema"
    df = pd.DataFrame({'a': [1, 2, 3]})
    table_schema = TableSchema().add_column('a', ti.Int32)
    yt_pd.write_table(df, yt.TablePath(table_name, schema=table_schema))
    assert yt.exists(table_name)
    assert yt.get_table_schema(table_name) == table_schema


@authors("abodrov")
def test_write_object_fails():
    table_name = "//tmp/test_write_object_fails"
    df = pd.DataFrame({'str': ["a", "b", "c"]})
    assert df.dtypes['str'] == np.dtype('O')
    with pytest.raises(ValueError, match="Column 'str' has unsupported dtype 'object'."):
        yt_pd.write_table(df, table_name)


@authors("abodrov")
def test_write_with_schema_type_conversion():
    table_name = "//tmp/test_write_with_schema_type_conversion"
    df = pd.DataFrame({'str': ["a", "b", "c"]})
    assert df.dtypes['str'] == np.dtype('O')
    table_schema = TableSchema().add_column('str', ti.String)
    yt_pd.write_table(df, yt.TablePath(table_name, schema=table_schema))
    assert yt.exists(table_name)
    assert yt.get_table_schema(table_name) == table_schema


@authors("abodrov")
def test_write_with_append_ignores_schema():
    table_name = "//tmp/test_write_with_append_ignores_schema"
    yt.create_table(table_name)
    df = pd.DataFrame({'a': [1, 2, 3]})
    yt_pd.write_table(df, yt.TablePath(table_name, append=True))
    assert yt.exists(table_name)
    assert list(yt.read_table(table_name)) == [{'a': 1}, {'a': 2}, {'a': 3}]
