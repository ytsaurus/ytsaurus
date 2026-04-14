import json
import logging
import os
import typing
import weakref

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, asdict
from pydantic_core import PydanticUndefined
from typing import Any, Optional, List, Tuple, TypedDict

from yt.common import _pretty_format_messages
from yt.mcp.lib.tool_runner_mcp import YTToolRunnerMCP

import yt.wrapper as yt


logger = logging.getLogger(__name__)


class ToolTemplateType(TypedDict):
    class ToolTemplateInputType(TypedDict):
        name: str
        description: str

    name: str
    description: str
    input: List[ToolTemplateInputType]


class ExternalConfigType(TypedDict):
    class ExternalConfigToolType(TypedDict):
        str: List[ToolTemplateType]

    tools: ExternalConfigToolType


class YTToolBase:
    @dataclass
    class ToolName:
        name: str
        description: str

    @dataclass
    class ToolInputField:
        name: str
        description: Optional[str] = None
        examples: Optional[List[str]] = None
        field_type: Optional[type] = None
        default: Optional[Any] = PydanticUndefined

    def __init__(self):
        self.runner: weakref.ref[YTToolRunnerMCP] = None
        self._tool_description: Optional[Tuple[self.ToolName, List[self.ToolInputField]]] = None

    def set_runner(self, runner: YTToolRunnerMCP):
        self.runner: YTToolRunnerMCP = weakref.proxy(runner)

    def _get_tool_description(self):
        if not self._tool_description:
            self._tool_description = self.get_tool_description()
        return self._tool_description

    def get_tool_description(self) -> Tuple[ToolName, List[ToolInputField]]:
        raise RuntimeError("Not Implemented")

    def on_handle_request(self, **kwargs):
        raise RuntimeError("Not Implemented")

    def get_tool_variants(self) -> Optional[List[ToolTemplateType]]:
        pass

    def helper_process_common_exception(self, ex: Exception, raise_exception=True):
        self.runner._logger.error(type(ex))
        self.runner._logger.error(ex)
        result = None
        if isinstance(ex, yt.errors.YtResponseError):
            if isinstance(ex, yt.errors.YtResolveError):
                result = "Given path does not exists on cluster"
            elif isinstance(ex, yt.errors.YtAuthenticationError):
                result = "Authentication error"
            elif ex.is_access_denied():
                result = "Access denied"
            else:
                result = _pretty_format_messages(ex)
        elif isinstance(ex, yt.errors.YtTokenError):
            result = "Authentication error (wrong token)"

        if raise_exception:
            if result:
                raise RuntimeError(result)
            else:
                raise ex
        return result

    def _clone_by_variants(self):
        variants = self.get_tool_variants()
        if variants:
            return self._clone_by_dict(variants)
        else:
            return []

    def _clone_by_dict(self, template: List[ToolTemplateType]) -> List["YTToolBase"]:
        """
            Template example:

            [
                {
                    "name": "<tool_1_subname>",
                    "descripion": "<tool_1_full_description>",  # emplty or absent means - copy from original
                    "input": [
                        {
                            "name": "<tool_input_name>",  # absent name means - no input parameter
                            "description": "<tool_input_new_description>",  # empty or absent means - copy from original
                        }
                    ]
                },
                {}  # emplty means - add origianl tool (instead of only modifications)
            ]
        """

        if not self._tool_description:
            self._tool_description = self.get_tool_description()

        def _append_attrs(src: typing.Mapping, dst: "YTToolBase", fields: typing.List[str]):
            for i, f in enumerate(fields):
                if f in src:
                    setattr(dst, f, f"{getattr(dst, f)}_{src[f]}")
                else:
                    setattr(dst, f, f"{getattr(dst, f)}_{i}")

        def _replace_attrs(src: typing.Mapping, dst: "YTToolBase", fields: typing.List[str]):
            for f in fields:
                if f in src:
                    setattr(dst, f, src[f])

        res = []
        for tool_template in template:
            if not tool_template and isinstance(tool_template, dict):
                res.append(self)
                continue
            new_tool: "YTToolBase" = deepcopy(self)
            new_description, new_inputs = new_tool._tool_description
            _append_attrs(tool_template, new_description, ("name", ))
            _replace_attrs(tool_template, new_description, ("description", ))
            self.runner._logger.info(f"Clone tool {self._tool_description[0].name} to {new_description.name}")

            if "input" in tool_template:
                orig_inputs_idx = dict((field.name, field) for field in new_inputs)
                new_inputs.clear()
                for field_template in tool_template["input"]:
                    orig_input_field = orig_inputs_idx.get(field_template["name"], None)
                    new_field = YTToolBase.ToolInputField(
                        **{
                            **(asdict(orig_input_field) if orig_input_field else {}),
                            **field_template,
                        }
                    )
                    if isinstance(new_field.field_type, str):
                        new_field.field_type = {
                            "str": str,
                            "int": int,
                            "float": float,
                        }.get(new_field.field_type, str)

                    new_inputs.append(new_field)

            res.append(new_tool)
        return res


def get_external_configs(config_file: str, config_yt_cluster: str, config_yt_path: str) -> Tuple[dict[str, Any], dict[str, Any]]:
    """
        config example:
        {
            "tools": {
                "<original_tool_name>": [
                    {
                        "name": "<tool_1_subname>",
                        "descripion": "<tool_1_full_description>",  # emplty or absent means - copy from original
                        "input": [
                            {
                                "name": "<tool_input_name>",  # absent name means - no input parameter
                                "description": "<tool_input_new_description>",  # empty or absent means - copy from original
                            }
                        ]
                    },
                    {}  # emplty means - add origianl tool (instead of only modifications)
                ]
            }
        }
    """
    config_from_file = None
    config_from_yt = None
    if config_file and os.path.exists(config_file):
        with open(config_file, "r") as fh:
            logger.info("Read config from file {CONFIG_FILE}")
            config_from_file = json.load(fh)

    if config_yt_cluster and config_yt_path:
        yt_client = yt.YtClient(config_yt_cluster, config={"proxy": {"retries": {"enable": False}}})
        try:
            if yt_client.exists(config_yt_path):
                logger.info(f"Read config from yt {config_yt_cluster}:{config_yt_path}")
                config_from_yt = {
                    "tools": defaultdict(list)
                }

                @yt.yt_dataclass
                class ToolConfig:
                    base_name: str
                    name: typing.Optional[str]
                    description: typing.Optional[str]
                    input: typing.Optional[bytes]

                tools: typing.List[ToolConfig] = yt_client.read_table_structured(config_yt_path, ToolConfig)
                tool: ToolConfig
                for tool in tools:
                    if tool.name:
                        config_from_yt["tools"][tool.base_name].append(
                            {
                                "name": tool.name,
                                "description": tool.description,
                                "input": yt.yson.loads(tool.input),
                            }
                        )
                    else:
                        config_from_yt["tools"][tool.base_name].append({})
        except Exception:
            logger.error(f"Can not load tool variations from YT {config_yt_cluster}:{config_yt_path}")
            pass

    return config_from_file, config_from_yt
