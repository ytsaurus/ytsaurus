from __future__ import annotations
import typing
import yt.wrapper as yt

if typing.TYPE_CHECKING:
    import pandas as pd


def read_table(table: str | yt.TablePath, *, client: yt.YtClient | None = None) -> pd.DataFrame:
    from ._arrow import read_table_to_dataframe_via_arrow

    if client is None:
        client = yt

    return read_table_to_dataframe_via_arrow(client, table)


def write_table(df: pd.DataFrame, table: str | yt.TablePath, *, client: yt.YtClient | None = None) -> None:
    from ._arrow import write_dataframe_to_table_via_arrow

    if client is None:
        client = yt

    write_dataframe_to_table_via_arrow(client, df, table)
