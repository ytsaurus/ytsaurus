import os

import pytest

from yt.wrapper import yson
from yt.yt.flow.library.python import runner


@pytest.mark.authors(["sergeypozdeev"])
@pytest.mark.parametrize(
    "worker, expected_port_count",
    [
        ({}, 3),
        ({"port_count": None}, 3),
        ({"port_count": -1}, 3),
        ({"port_count": 0}, 3),
        ({"port_count": 1}, 3),
        ({"port_count": 2}, 3),
        ({"port_count": 3}, 3),
        ({"port_count": 4}, 4),
        ({"port_count": 7}, 7),
    ],
)
def test_launch_normalizes_companion_ports(monkeypatch, tmp_path, worker, expected_port_count):
    config = {
        "vanilla": {"enable": True, "worker": worker},
        "spec": {
            "resources": {
                "companion": {"resource_class_name": "NYT::NFlow::NCompanion::TCompanionManager"},
            },
        },
    }
    config_path = tmp_path / "pipeline.yson"
    config_path.write_bytes(yson.dumps(config))
    monkeypatch.setattr(runner.tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(runner.sys, "argv", [str(tmp_path / "pipeline")])
    calls = []
    monkeypatch.setattr(runner.os, "execv", lambda executable, args: calls.append((executable, args)))

    runner.launch(str(config_path), "./flow_server")

    assert len(calls) == 1
    executable, args = calls[0]
    assert executable == os.path.abspath("./flow_server")
    assert args[:2] == [executable, "--config"]
    with open(args[2], "rb") as source:
        generated = yson.load(source)
    assert generated["vanilla"]["worker"]["port_count"] == expected_port_count
    assert generated["vanilla"]["worker"]["local_files"]["py_companion"] == str(tmp_path / "pipeline")
    assert generated["spec"]["resources"]["companion"]["parameters"]["entrypoint"] == {
        "executable": "./py_companion",
    }


@pytest.mark.authors(["sergeypozdeev"])
def test_launch_preserves_disabled_vanilla(monkeypatch, tmp_path):
    config = {"vanilla": {"enable": False, "worker": {"port_count": 0}}}
    config_path = tmp_path / "pipeline.yson"
    config_path.write_bytes(yson.dumps(config))
    monkeypatch.setattr(runner.tempfile, "tempdir", str(tmp_path))
    calls = []
    monkeypatch.setattr(runner.os, "execv", lambda executable, args: calls.append(args))

    runner.launch(str(config_path), "./flow_server")

    with open(calls[0][2], "rb") as source:
        assert yson.load(source) == config


def _launch_with_flow_bin(monkeypatch, tmp_path, flow_bin):
    config_path = tmp_path / "pipeline.yson"
    config_path.write_bytes(yson.dumps({}))
    monkeypatch.setattr(runner.tempfile, "tempdir", str(tmp_path))
    calls = []
    monkeypatch.setattr(runner.os, "execv", lambda executable, args: calls.append(executable))
    runner.launch(str(config_path), flow_bin)
    return calls[0]


@pytest.mark.authors(["timoninmaxim"])
def test_launch_explicit_flow_bin_wins(monkeypatch, tmp_path):
    monkeypatch.setenv("YT_FLOW_BIN", "/env/flow_server")

    assert _launch_with_flow_bin(monkeypatch, tmp_path, "/flag/flow_server") == "/flag/flow_server"


@pytest.mark.authors(["timoninmaxim"])
def test_launch_takes_flow_bin_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("YT_FLOW_BIN", "/env/flow_server")

    assert _launch_with_flow_bin(monkeypatch, tmp_path, None) == "/env/flow_server"


@pytest.mark.authors(["timoninmaxim"])
@pytest.mark.parametrize("env_value", [None, ""])
def test_launch_fails_without_flow_bin(monkeypatch, tmp_path, env_value):
    if env_value is None:
        monkeypatch.delenv("YT_FLOW_BIN", raising=False)
    else:
        monkeypatch.setenv("YT_FLOW_BIN", env_value)

    with pytest.raises(RuntimeError, match="--flow-bin.*YT_FLOW_BIN"):
        _launch_with_flow_bin(monkeypatch, tmp_path, None)
