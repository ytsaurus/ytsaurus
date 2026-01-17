import json
import logging
import os
import typing

from mcp.server.fastmcp import FastMCP, Context
from mcp.types import TextContent
from mcp.server.fastmcp.utilities.func_metadata import ArgModelBase
from pydantic import Field, create_model
from pydantic.fields import _Unset
from typing import Annotated, Optional, Union, List, Dict, Any

import yt.wrapper as yt

import yt.mcp as yt_mcp


class YTToolRunnerMCP:
    _PUBLIC_CLUSTERS = ()

    def helper_get_public_clusters(self, delimeter: Optional[str] = None, template: Optional[str] = None) -> Union[List[str], str]:
        if delimeter:
            if template:
                return delimeter.join(map(lambda s: template.format(s), self._PUBLIC_CLUSTERS))
            else:
                return delimeter.join(self._PUBLIC_CLUSTERS)
        elif template:
            return ", ".join(map(lambda s: template.format(s), self._PUBLIC_CLUSTERS))
        else:
            return self._PUBLIC_CLUSTERS

    def helper_get_yt_client(self, cluster, request_context: Context) -> yt.YtClient:
        if self._yt_token:
            yt_client = yt.YtClient(cluster, token=self._yt_token)
        else:
            yt_client = yt.YtClient(cluster)
        return yt_client

    def __init__(self, name=None):
        self._name = name or "YTMCPServer"
        self._tools: List["yt_mcp.lib.tools.helpers.YTToolBase"] = []
        self._logger = logging.getLogger(__name__)
        self._yt_token = None

    def attach_tools(self, tools: List["yt_mcp.lib.tools.helpers.YTToolBase"], variants: List[Dict[str, Any]] = None):
        for tool in tools:
            tool.set_runner(self)

            cloned_tools = tool._clone_by_variants()
            if variants:
                for variant in variants:
                    if variant:
                        tool_description, _ = tool._get_tool_description()
                        cloned_tools.extend(tool._clone_by_dict(variant["tools"][tool_description.name]))

            if cloned_tools:
                self._tools.extend(cloned_tools)
            else:
                self._tools.append(tool)

    def configure_yt(self, token_file=None):
        if os.environ.get("MCP_YT_TOKEN"):
            self._yt_token = os.environ["MCP_YT_TOKEN"]
            self._logger.debug("Use YT token from env MCP_YT_TOKEN")
        elif token_file:
            with open(token_file, "r") as fh:
                self._yt_token = fh.read().strip()
                self._logger.debug(f"Use YT token from file {token_file}")
        else:
            self._logger.debug("Use YT token from YT client (default)")

    def configure_logging(self, level, file_name):
        logging.basicConfig(
            filename=file_name,
            level=level,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )

    def return_text(self, text):
        return TextContent(type="text", text=text)

    def _simplify_structured(self, data):
        if isinstance(data, yt.yson.yson_types.YsonList):
            data = [{"item_value": row["$value"], "item_attributes": row["$attributes"]} if "$value" in row else row for row in yt.yson.convert.yson_to_json(data)]
        elif isinstance(data, typing.Generator):
            return self._simplify_structured(list(data))

        try:
            json.dump(data)
        except TypeError:
            # TODO: convert to dict
            data = repr(data)

        return data

    def return_structured(self, data):
        if isinstance(data, yt.format.RowsIterator):
            data = list(data)
        return TextContent(type="text", text=json.dumps(self._simplify_structured(data)))

    def _add_tool_to_mcp(self, mcp: FastMCP, tool: "yt.mcp.lib.tools.helpers.YTToolBase"):
        tool_description, tool_params = tool._get_tool_description()

        def handler(context: Context, *args, **kwargs):
            return tool.on_handle_request(request_context=context, *args, **kwargs)

        mcp.add_tool(
            fn=handler,
            name=tool_description.name,
            description=tool_description.description,
        )

        args_fields = {}
        for param in tool_params:
            field = Field(
                default=param.default,
                name=param.name,
                description=param.description or _Unset,
                examples=param.examples or _Unset,
            )
            args_fields[param.name] = Annotated[param.field_type or str, field]

        args_model = create_model(
            f"Tool{tool_description.name.capitalize()}Arguments",
            __base__=ArgModelBase,
            **args_fields
        )

        mcp._tool_manager.get_tool(tool_description.name).fn_metadata.arg_model = args_model
        mcp._tool_manager.get_tool(tool_description.name).parameters = args_model.model_json_schema()

    def start(self, transport="stdio"):
        mcp = FastMCP(self._name, log_level="ERROR")

        for tool in self._tools:
            self._add_tool_to_mcp(mcp, tool)

        mcp.run(transport)
