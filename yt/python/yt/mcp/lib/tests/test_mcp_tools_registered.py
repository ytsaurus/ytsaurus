import asyncio
import itertools
import typing
from unittest.mock import MagicMock

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import Context
from pydantic_core import PydanticUndefined

from yt.mcp.lib.tool_runner_mcp import YTToolRunnerMCP
from yt.mcp.lib.server import get_all_tool_names, get_tools_groups


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


# Identified by Python class name (works without attaching a runner: tools whose
# get_tool_description() references self.runner.helper_get_public_clusters
# would otherwise crash on a None runner).
_MUTATING_CLASSES = ("CreateNode", "CopyNode", "MoveNode", "RemoveNode", "SetAttribute")
_READONLY_CLASSES = ("ListDir", "Search", "ReadStaticTable")


def test_mutating_tools_present_in_contextual_groups():
    # Mutating tools (Cypress create/copy/move/remove/set) are interleaved
    # into their contextual groups (currently "common"), not into a separate
    # "edit" group; the bin filters them out unless --rw-mode is set.
    groups = get_tools_groups()
    assert "edit" not in groups
    common_classes = {type(t).__name__ for t in groups["common"]}
    for name in _MUTATING_CLASSES:
        assert name in common_classes, f"{name} must be present in the 'common' group"


def test_mutating_tools_marked_as_mutable():
    # Each mutating tool sets _MUTABLE = True so the bin can filter them out
    # in read-only mode (the default).
    groups = get_tools_groups()
    by_class = {type(t).__name__: t for t in groups["common"]}
    for name in _MUTATING_CLASSES:
        assert by_class[name]._MUTABLE is True, f"{name} must have _MUTABLE = True"
    for name in _READONLY_CLASSES:
        assert by_class[name]._MUTABLE is False, f"{name} must have _MUTABLE = False"


def test_mutating_tools_filtered_in_readonly_mode():
    # Same filtering logic the bin uses: drop tools where _MUTABLE is True.
    groups = get_tools_groups()
    all_tools = list(itertools.chain(*groups.values()))
    filtered = [t for t in all_tools if not getattr(t, "_MUTABLE", False)]
    classes = {type(t).__name__ for t in filtered}
    for name in _MUTATING_CLASSES:
        assert name not in classes, f"{name} must be filtered out in read-only mode"
    for name in _READONLY_CLASSES:
        assert name in classes, f"{name} must survive read-only filtering"


def test_get_all_tool_names_matches_readonly_binary():
    # get_all_tool_names() default must match the read-only server binaries
    # (mutating tools filtered out); rw_mode=True includes them.
    readonly_names = get_all_tool_names()
    for name in _MUTATING_CLASSES:
        assert name not in readonly_names, f"{name} must not be listed in read-only mode"
    rw_names = get_all_tool_names(rw_mode=True)
    for name in _MUTATING_CLASSES:
        assert name in rw_names, f"{name} must be listed in rw mode"
