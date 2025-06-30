import json

import yt.wrapper as yt
import pyarrow as pa
import pandas as pd

from ._schema import (
    extract_table_schema_from_dataframe,
    read_table_schema,
    table_schema_to_arrow_pandas_metadata,
    table_schema_to_dtype_mapping,
)


def write_dataframe_to_table_via_arrow(client: yt.YtClient, df: pd.DataFrame, table_path: str | yt.TablePath):
    table_path = yt.TablePath(table_path, client=client)
    if "schema" not in table_path.attributes:
        if "append" not in table_path.attributes:
            # NOTE: Schema is not compatible with append, but it's totally fine for YT to write arrow without schema.
            table_path.attributes["schema"] = extract_table_schema_from_dataframe(df)
    else:
        schema = table_path.attributes["schema"]
        dtypes = table_schema_to_dtype_mapping(schema)
        df = df.astype(dtypes)
    array_table = pa.Table.from_pandas(df)
    client.write_table(table_path, _to_pybytes(array_table), format=yt.format.ArrowFormat(raw=True), raw=True)


def _to_pybytes(arrow_table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    writer = pa.ipc.RecordBatchStreamWriter(sink, arrow_table.schema)
    writer.write(arrow_table)
    writer.close()
    return sink.getvalue().to_pybytes()


def read_table_to_dataframe_via_arrow(client: yt.YtClient, table_path: str | yt.TablePath) -> pd.DataFrame:
    schema = read_table_schema(client, table_path)
    pandas_metadata = table_schema_to_arrow_pandas_metadata(schema)
    pandas_metadata_bytes = json.dumps(pandas_metadata).encode("utf-8")
    stream = client.read_table(table_path, format=yt.format.ArrowFormat(raw=True), raw=True)
    arrow_table = pa.ipc.open_stream(stream).read_all().replace_schema_metadata({b"pandas": pandas_metadata_bytes})
    return arrow_table.to_pandas()
