from yt.mcp.lib.tools.helpers import YTToolBase
from yt.mcp.lib.tools.common_client import CommonCypress

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def test_common_client_get_operation_exposes_and_forwards_include_runtime():
    class FakeClient:
        def __init__(self):
            self.kwargs = None

        def get_operation(self, **kwargs):
            self.kwargs = kwargs
            return {"id": "1-2-3-4"}

    client = FakeClient()

    class FakeRunner:
        _logger = logger

        def helper_get_yt_client(self, cluster, request_context):
            return client

        def return_structured(self, data):
            return data

    tool = CommonCypress()
    runner = FakeRunner()
    tool.set_runner(runner)

    get_operation_variant = next(
        variant for variant in tool.get_tool_variants()
        if variant["name"] == "get_operation"
    )
    include_runtime_field = next(
        field for field in get_operation_variant["input"]
        if field["name"] == "include_runtime"
    )
    assert include_runtime_field["field_type"] == Optional[bool]
    assert include_runtime_field["default"] is False

    result = tool.on_handle_request(
        cluster="watt",
        method="get_operation",
        operation_id="1-2-3-4",
        attributes=["progress"],
        include_runtime=True,
        request_context=None,
    )

    assert result == {"id": "1-2-3-4"}
    assert client.kwargs == {
        "operation_id": "1-2-3-4",
        "attributes": ["progress"],
        "include_runtime": True,
    }


def test_dict_variation():
    class FakeRunner:
        _logger = logger

        def helper_get_yt_client(cluster, request_context):
            return None

    class TestTool(YTToolBase):
        def get_tool_description(self):
            return (
                self.ToolName(
                    name="tool_name",
                    description="tool descr",
                ),
                [
                    self.ToolInputField(
                        name="field_one",
                        description="Field one description",
                    ),
                    self.ToolInputField(
                        name="field_two",
                        description="Field two description",
                    ),
                    self.ToolInputField(
                        name="field_three",
                        field_type=int,
                        description="Field three description",
                    ),
                ]
            )

        def get_tool_variants(self):
            return [
                {
                    "name": "subname2",
                    "description": "tool descr2",
                    "input": [
                        {
                            "name": "field_two",
                        },
                        {
                            "field_type": str,
                            "name": "field_three",
                            "description": "New field three description",
                        },
                        {
                            "name": "field_four",
                            "description": "Field four description",
                        },
                    ]
                }
            ]

    test_tool = TestTool()
    runner = FakeRunner()
    test_tool.set_runner(runner)

    clones_by_variants = test_tool._clone_by_variants()

    assert len(clones_by_variants) == 1
    assert clones_by_variants[0]._tool_description[0].name == "tool_name_subname2"
    assert clones_by_variants[0]._tool_description[0].description == "tool descr2"
    assert "field_one" not in [field.name for field in clones_by_variants[0]._tool_description[1]], "Cut field"
    assert "field_two" in [field.name for field in clones_by_variants[0]._tool_description[1]], "Pass field as is"
    assert clones_by_variants[0]._tool_description[1][0].description == "Field two description"
    assert "field_three" in [field.name for field in clones_by_variants[0]._tool_description[1]]
    assert clones_by_variants[0]._tool_description[1][1].field_type == str, "Type field three chaned"
    assert clones_by_variants[0]._tool_description[1][1].description == "New field three description"
    assert "field_four" in [field.name for field in clones_by_variants[0]._tool_description[1]], "New field added"

    clones_by_dict = test_tool._clone_by_dict(
        [
            {
                "name": "subname2",
                "description": "tool descr2",
                "input": [
                    {
                        "name": "field_two",
                    },
                    {
                        "field_type": str,
                        "name": "field_three",
                        "description": "New field three description",
                    },
                    {
                        "name": "field_four",
                        "description": "Field four description",
                    },
                ]
            }
        ]
    )

    assert len(clones_by_dict) == len(clones_by_variants)
    for k in ('name', 'description'):
        assert clones_by_dict[0]._tool_description[0].__dict__[k] == clones_by_variants[0]._tool_description[0].__dict__[k]
    assert str(clones_by_dict[0]._tool_description[1]) == str(clones_by_variants[0]._tool_description[1])
