from .helpers import YTToolBase

import yt.wrapper as yt

from typing import Annotated


class ReadStaticTable(YTToolBase):
    def on_handle_request(
        self,
        *,
        cluster: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Cluster",
            )
        ],
        table: Annotated[
            str,
            YTToolBase.ToolInputField(
                description="Table path",
            )
        ],
        limit: Annotated[
            int,
            YTToolBase.ToolInputField(
                description="Limit",
                default=1,
            )
        ] = 1,
        offset: Annotated[
            int,
            YTToolBase.ToolInputField(
                description="Offset",
                default=0,
            )
        ] = 0,
        request_context,
        **kwargs,
    ):
        "A tool for getting static table content. " \
            "Returns 'total_rows' (total rows in table), 'limit' (current limit), 'offset' (current offset), 'table_content' (rows from table). " \
            "Data size can by large - use limit 1 for first query."
        yt_client = self.runner.helper_get_yt_client(cluster, request_context)

        table = yt.YPath(table)
        if table.attributes and "ranges" not in table.attributes:
            table.attributes["ranges"] = [
                {"lower_limit": {"row_index": offset}, "upper_limit": {"row_index": offset + limit}}
            ]

        try:
            table_attributes = yt_client.get(table, attributes=["row_count"]).attributes
            table_content = yt_client.read_table(
                table,
            )
        except Exception as e:
            self.helper_process_common_exception(e)

        return {
            "total_rows": table_attributes["row_count"],
            "limit": limit,
            "offset": offset,
            "table_content": list(table_content)
        }
