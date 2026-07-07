import asyncio
import itertools
import typing
from unittest.mock import MagicMock

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import Context
from pydantic_core import PydanticUndefined

from yt.mcp.lib.tool_runner_mcp import YTToolRunnerMCP
from yt.mcp.lib.server import get_tools_groups


_TYPE_STUBS = {str: "stub", int: 0, float: 0.0, bool: False, list: [], dict: {}}


def _build_runner_and_mcp():
    runner = YTToolRunnerMCP()
    runner.attach_tools(list(itertools.chain(*get_tools_groups().values())))

    mcp = FastMCP("test")
    for tool in runner._tools:
        runner._add_tool_to_mcp(mcp, tool)
    return runner, mcp


def test_all_tools_registered_in_mcp():
    runner, mcp = _build_runner_and_mcp()

    expected = {tool._get_tool_description()[0].name for tool in runner._tools}
    registered = {t.name for t in mcp._tool_manager.list_tools()}

    assert registered == expected, (
        f"Missing: {expected - registered}\n"
        f"Unexpected: {registered - expected}"
    )


def test_each_tool_has_description():
    _, mcp = _build_runner_and_mcp()
    for tool in mcp._tool_manager.list_tools():
        assert tool.description, f"Tool {tool.name!r} has no description"


def test_each_tool_has_parameters_schema():
    _, mcp = _build_runner_and_mcp()
    for tool in mcp._tool_manager.list_tools():
        assert isinstance(tool.parameters, dict), \
            f"Tool {tool.name!r} has no parameters schema"
        assert "properties" in tool.parameters, \
            f"Tool {tool.name!r} parameters schema has no 'properties'"


def test_tools_callable_through_mcp():
    runner, mcp = _build_runner_and_mcp()

    for tool in runner._tools:
        tool.on_handle_request = MagicMock(return_value="ok")

    context = Context(request_context=None, fastmcp=mcp)

    for registered_tool in mcp._tool_manager.list_tools():
        model = registered_tool.fn_metadata.arg_model

        # Build args: use field default if exists, otherwise pick a stub by type.
        args = {}
        for name, info in model.model_fields.items():
            if info.default is not PydanticUndefined:
                args[name] = info.default
            elif info.default_factory is not None:
                args[name] = info.default_factory()
            else:
                annotation = info.annotation
                if typing.get_origin(annotation) is typing.Union:
                    non_none = [a for a in typing.get_args(annotation) if a is not type(None)]
                    annotation = non_none[0] if non_none else None
                origin = typing.get_origin(annotation)
                args[name] = next(
                    (v for t, v in _TYPE_STUBS.items() if annotation is t or origin is t),
                    "stub",
                )

        # Some tools declare defaults with wrong types (e.g. default="table" for list[str]).
        # Re-validate and fix only the broken fields.
        try:
            model.model_validate(args)
        except Exception as e:
            for error in e.errors():
                broken_name = error["loc"][0]
                broken_info = model.model_fields[broken_name]
                origin = typing.get_origin(broken_info.annotation)
                args[broken_name] = next(
                    (v for t, v in _TYPE_STUBS.items() if broken_info.annotation is t or origin is t),
                    "stub",
                )

        result = asyncio.run(
            mcp._tool_manager.call_tool(registered_tool.name, args, context=context)
        )
        assert result is not None


def test_default_rw_mode_is_false():
    runner = YTToolRunnerMCP()
    assert runner._rw_mode is False


def test_configure_server_sets_rw_mode():
    runner = YTToolRunnerMCP()
    runner.configure_server(rw_mode=True)
    assert runner._rw_mode is True


def test_configure_server_readonly():
    runner = YTToolRunnerMCP()
    runner.configure_server(rw_mode=False)
    assert runner._rw_mode is False
