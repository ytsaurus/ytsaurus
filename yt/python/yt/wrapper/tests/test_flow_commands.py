from yt.wrapper import flow_commands
from yt.common import YtError
import yt.wrapper.yson as yson
from yt.cli import yt_binary

import pytest


@pytest.mark.authors(["timoninmaxim"])
@pytest.mark.parametrize(
    "options,argument",
    [
        ({}, {}),
        (
            {"spec_path": "/computations/reader", "expected_version": 7, "force": True},
            {"path": "/computations/reader", "expected_version": 7, "force": True},
        ),
        ({"spec_path": "", "expected_version": 0, "force": False}, {"path": "", "expected_version": 0, "force": False}),
    ],
)
def test_static_spec_remove_uses_flow_execute(monkeypatch, options, argument):
    calls = []
    client = object()
    monkeypatch.setattr(
        flow_commands,
        "flow_execute",
        lambda pipeline_path, **kwargs: calls.append((pipeline_path, kwargs)) or {"version": 8},
    )
    assert flow_commands.remove_pipeline_spec("//pipeline", client=client, **options) == {"version": 8}
    assert calls == [
        (
            "//pipeline",
            {
                "flow_command": "remove-pipeline-spec",
                "flow_argument": argument,
                "client": client,
            },
        )
    ]


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_remove_is_atomic(monkeypatch):
    calls = []

    def execute(pipeline_path, **kwargs):
        calls.append((pipeline_path, kwargs))
        return {"version": 8}

    monkeypatch.setattr(flow_commands, "flow_execute", execute)
    result = flow_commands.remove_pipeline_dynamic_spec("//pipeline", spec_path="/missing", expected_version=7)
    assert result == {"version": 8}
    assert calls == [
        (
            "//pipeline",
            {
                "flow_command": "remove-pipeline-dynamic-spec",
                "flow_argument": {"path": "/missing", "expected_version": 7},
                "client": None,
            },
        )
    ]


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_patch_create_and_listing(monkeypatch):
    calls = []
    monkeypatch.setattr(
        flow_commands,
        "flow_execute",
        lambda pipeline_path, **kwargs: calls.append((pipeline_path, kwargs)) or {"version": 8, "patch_id": 8},
    )
    result = flow_commands.patch_pipeline_dynamic_spec(
        "//pipeline", set=("/limits/max", 120), comment="Keep across releases", expected_version=7
    )
    assert result == {"version": 8, "patch_id": 8}
    assert calls[0][1]["flow_command"] == "patch-pipeline-dynamic-spec"
    assert calls[0][1]["flow_argument"] == {
        "set_path": "/limits/max",
        "set_value": 120,
        "comment": "Keep across releases",
        "expected_version": 7,
    }

    flow_commands.get_pipeline_dynamic_spec_patches("//pipeline", patch=8)
    assert calls[1][1]["flow_command"] == "get-pipeline-dynamic-spec-patches"
    assert calls[1][1]["flow_argument"] == {"patch": 8}

    flow_commands.patch_pipeline_dynamic_spec("//pipeline", patch=8, comment="New reason", expected_version=8)
    assert calls[2][1]["flow_argument"] == {
        "patch": 8,
        "comment": "New reason",
        "expected_version": 8,
    }

    flow_commands.patch_pipeline_dynamic_spec(
        "//pipeline", set_changes=[("/a", 1), ("/b", {"c": 2})], comment="Update both", expected_version=8
    )
    assert calls[3][1]["flow_argument"] == {
        "set_changes": [{"path": "/a", "value": 1}, {"path": "/b", "value": {"c": 2}}],
        "comment": "Update both",
        "expected_version": 8,
    }


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_get_view_and_blame(monkeypatch):
    calls = []
    monkeypatch.setattr(flow_commands, "flow_execute", lambda pipeline_path, **kwargs: calls.append(kwargs))
    flow_commands.get_pipeline_dynamic_spec("//pipeline", view="base", blame=True)
    client = object()
    flow_commands.get_pipeline_dynamic_spec("//pipeline", client=client)
    assert calls == [
        {
            "flow_command": "get-pipeline-dynamic-spec",
            "flow_argument": {"view": "base", "blame": True},
            "output_format": None,
            "client": None,
        },
        {
            "flow_command": "get-pipeline-dynamic-spec",
            "flow_argument": {},
            "output_format": None,
            "client": client,
        },
    ]


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_patch_cli_parses_set_value(monkeypatch):
    calls = []
    output = []
    monkeypatch.setattr(
        flow_commands,
        "flow_execute",
        lambda pipeline_path, **kwargs: calls.append(kwargs) or {"version": 8, "patch_id": 8},
    )
    monkeypatch.setattr(yt_binary, "print_to_output", output.append)
    args = yt_binary._prepare_parser().parse_args(
        [
            "flow",
            "patch-pipeline-dynamic-spec",
            "//pipeline",
            "--set",
            "/limits",
            '{"max": 120}',
            "--format",
            "json",
            "--comment",
            "Raise limit",
            "--expected-version",
            "7",
        ]
    )
    args.func(
        pipeline_path=args.pipeline_path,
        set=args.set,
        patch=args.patch,
        comment=args.comment,
        expected_version=args.expected_version,
        format=args.format,
    )
    assert calls[0]["flow_command"] == "patch-pipeline-dynamic-spec"
    assert calls[0]["flow_argument"] == {
        "set_path": "/limits",
        "set_value": {"max": 120},
        "comment": "Raise limit",
        "expected_version": 7,
    }
    assert yson.loads(output[0].encode("utf-8")) == {"version": 8, "patch_id": 8}


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_patch_cli_multiple_sets(monkeypatch):
    calls = []
    monkeypatch.setattr(
        flow_commands,
        "flow_execute",
        lambda pipeline_path, **kwargs: calls.append(kwargs) or {"version": 8, "patch_id": 8},
    )
    monkeypatch.setattr(yt_binary, "print_to_output", lambda output: None)
    args = yt_binary._prepare_parser().parse_args(
        [
            "flow",
            "patch-pipeline-dynamic-spec",
            "//pipeline",
            "--set",
            "/a",
            "1",
            "--set",
            "/b",
            '{"c": 2}',
            "--format",
            "json",
            "--comment",
            "Update both",
            "--expected-version",
            "7",
        ]
    )
    args.func(
        pipeline_path=args.pipeline_path,
        set=args.set,
        patch=args.patch,
        comment=args.comment,
        expected_version=args.expected_version,
        format=args.format,
    )
    assert calls[0]["flow_argument"] == {
        "set_changes": [{"path": "/a", "value": 1}, {"path": "/b", "value": {"c": 2}}],
        "comment": "Update both",
        "expected_version": 7,
    }


@pytest.mark.authors(["timoninmaxim"])
@pytest.mark.parametrize(
    "function,arguments,command,payload",
    [
        (
            flow_commands.cancel_pipeline_dynamic_spec_patch,
            {"patch": 4},
            "cancel-pipeline-dynamic-spec-patch",
            {"patch": 4, "expected_version": 7},
        ),
        (
            flow_commands.reset_pipeline_dynamic_spec,
            {},
            "reset-pipeline-dynamic-spec",
            {"expected_version": 7},
        ),
    ],
)
def test_dynamic_spec_cancellation_cas(monkeypatch, function, arguments, command, payload):
    calls = []
    monkeypatch.setattr(
        flow_commands, "flow_execute", lambda pipeline_path, **kwargs: calls.append((pipeline_path, kwargs))
    )
    function("//pipeline", expected_version=7, **arguments)
    assert calls == [("//pipeline", {"flow_command": command, "flow_argument": payload, "client": None})]


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_legacy_positional_client(monkeypatch):
    client = object()
    calls = []
    monkeypatch.setattr(
        flow_commands, "flow_execute", lambda pipeline_path, **kwargs: calls.append((pipeline_path, kwargs))
    )
    flow_commands.set_pipeline_dynamic_spec("//pipeline", 8, "/parallelism", 7, None, client)
    flow_commands.remove_pipeline_dynamic_spec("//pipeline", "/parallelism", 7, client)
    assert [kwargs["client"] for _, kwargs in calls] == [client, client]
    assert calls[0][1]["flow_argument"] == {
        "spec": 8,
        "path": "/parallelism",
        "expected_version": 7,
    }
    assert calls[1][1]["flow_argument"] == {
        "path": "/parallelism",
        "expected_version": 7,
    }


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_dynamic_client_method_preserves_client_slot(monkeypatch):
    from yt.wrapper.client_helpers import create_class_method

    client = object()
    calls = []
    monkeypatch.setattr(
        flow_commands, "flow_execute", lambda pipeline_path, **kwargs: calls.append((pipeline_path, kwargs))
    )
    method = create_class_method(flow_commands.set_pipeline_dynamic_spec)
    method(client, "//pipeline", 8, "/parallelism", 7, None)
    assert calls[0][1]["client"] is client
    assert calls[0][1]["flow_argument"]["spec"] == 8


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_bound_client_preserves_legacy_arguments(monkeypatch):
    from yt.wrapper import YtClient

    client = YtClient(proxy="localhost")
    calls = []
    monkeypatch.setattr(
        flow_commands, "flow_execute", lambda pipeline_path, **kwargs: calls.append((pipeline_path, kwargs))
    )
    client.set_pipeline_dynamic_spec("//pipeline", 8, "/parallelism", 7, None)
    client.remove_pipeline_dynamic_spec("//pipeline", "/parallelism", 7)
    assert [kwargs["client"] for _, kwargs in calls] == [client, client]
    assert calls[0][1]["flow_argument"]["spec"] == 8
    assert calls[1][1]["flow_argument"]["path"] == "/parallelism"


@pytest.mark.authors(["timoninmaxim"])
@pytest.mark.parametrize("dynamic_method", [False, True])
def test_non_flow_client_preserves_positional_arguments(monkeypatch, dynamic_method):
    from yt.wrapper import YtClient, cypress_commands
    from yt.wrapper.client_helpers import create_class_method

    client = YtClient(proxy="localhost")
    calls = []
    monkeypatch.setattr(cypress_commands, "get_api_version", lambda client: "v4")

    def request(command, params, **kwargs):
        calls.append((command, params, kwargs))
        return {"value": True}

    monkeypatch.setattr(cypress_commands, "make_formatted_request", request)
    if dynamic_method:
        method = create_class_method(cypress_commands.exists)
        assert method(client, "//node", "cache", 4, True)
    else:
        assert client.exists("//node", "cache", 4, True)
    command, params, kwargs = calls[0]
    assert command == "exists"
    assert str(params["path"]) == "//node"
    assert params["read_from"] == "cache"
    assert params["cache_sticky_group_size"] == 4
    assert params["suppress_transaction_coordinator_sync"] is True
    assert kwargs["client"] is client


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_set_preserves_large_version(monkeypatch):
    calls = []
    monkeypatch.setattr(
        flow_commands, "flow_execute", lambda pipeline_path, **kwargs: calls.append((pipeline_path, kwargs))
    )
    flow_commands.set_pipeline_dynamic_spec(
        "//pipeline",
        [1, 2],
        spec_path="/resources/text/file_providers",
        expected_version=9007199254740993,
    )
    assert calls == [
        (
            "//pipeline",
            {
                "flow_command": "set-pipeline-dynamic-spec",
                "flow_argument": {
                    "spec": [1, 2],
                    "path": "/resources/text/file_providers",
                    "expected_version": 9007199254740993,
                },
                "client": None,
            },
        )
    ]


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_set_cli_is_silent(monkeypatch):
    output = []
    calls = []
    monkeypatch.setattr(
        flow_commands, "flow_execute", lambda *args, **kwargs: calls.append((args, kwargs)) or {"version": 8}
    )
    monkeypatch.setattr(yt_binary, "print_to_output", output.append)
    args = yt_binary._prepare_parser().parse_args(
        ["flow", "set-pipeline-dynamic-spec", "//pipeline", "--value", "{x=1;}", "--expected-version", "7"]
    )
    args.func(
        pipeline_path=args.pipeline_path,
        value=args.value,
        spec_path=args.spec_path,
        expected_version=args.expected_version,
        format=args.format,
    )
    assert output == []
    assert calls[0][1]["flow_argument"]["expected_version"] == 7


@pytest.mark.authors(["timoninmaxim"])
@pytest.mark.parametrize("comment", [None, ""])
@pytest.mark.parametrize("bound", [False, True])
def test_dynamic_spec_patch_requires_nonempty_comment(monkeypatch, comment, bound):
    from yt.wrapper import YtClient

    calls = []
    monkeypatch.setattr(flow_commands, "flow_execute", lambda *args, **kwargs: calls.append(kwargs))
    target = YtClient(proxy="localhost") if bound else flow_commands
    with pytest.raises(YtError, match="nonempty comment"):
        target.patch_pipeline_dynamic_spec("//pipeline", set=("/x", 8), expected_version=7, comment=comment)
    assert calls == []


@pytest.mark.authors(["timoninmaxim"])
@pytest.mark.parametrize("comment", [None, ""])
def test_dynamic_spec_patch_cli_requires_nonempty_comment(monkeypatch, comment):
    calls = []
    monkeypatch.setattr(flow_commands, "flow_execute", lambda *args, **kwargs: calls.append(kwargs))
    arguments = ["flow", "patch-pipeline-dynamic-spec", "//pipeline", "--set", "/x", "8", "--expected-version", "7"]
    if comment is not None:
        arguments.extend(["--comment", comment])
    args = yt_binary._prepare_parser().parse_args(arguments)
    with pytest.raises(YtError, match="nonempty comment"):
        args.func(
            pipeline_path=args.pipeline_path,
            set=args.set,
            patch=args.patch,
            comment=args.comment,
            expected_version=args.expected_version,
            format=args.format,
        )
    assert calls == []


@pytest.mark.authors(["timoninmaxim"])
@pytest.mark.parametrize(
    "comment", ["  Причина 😀  ", "\u200b", "\u001c\u001d\u001e\u001f", "\u0085 Причина 😀 \u200b"]
)
def test_dynamic_spec_patch_comment_preserves_unicode_and_surrounding_spaces(monkeypatch, comment):
    calls = []
    monkeypatch.setattr(flow_commands, "flow_execute", lambda *args, **kwargs: calls.append(kwargs))
    flow_commands.patch_pipeline_dynamic_spec("//pipeline", set=("/x", 8), expected_version=7, comment=comment)
    assert calls[0]["flow_argument"]["comment"] == comment


@pytest.mark.authors(["timoninmaxim"])
def test_dynamic_spec_cli_rejects_removed_view():
    with pytest.raises(SystemExit):
        yt_binary._prepare_parser().parse_args(
            ["flow", "get-pipeline-dynamic-spec", "//pipeline", "--view", "override"]
        )
