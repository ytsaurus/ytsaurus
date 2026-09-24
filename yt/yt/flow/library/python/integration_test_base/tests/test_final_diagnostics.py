from unittest.mock import Mock

import pytest

from yt.yt.flow.library.python.integration_test_base import flow_process, yt_flow_base


@pytest.mark.authors(["mikari"])
@pytest.mark.parametrize("use_vanilla_jobs", [False, True])
@pytest.mark.parametrize("early_dump", [False, True])
@pytest.mark.parametrize("diagnostics_fail", [False, True])
def test_final_diagnostics(monkeypatch, tmp_path, use_vanilla_jobs, early_dump, diagnostics_fail):
    events = []
    controller_alive = True

    def dump(name):
        events.append(name)
        assert controller_alive
        if diagnostics_fail and name != "sensors":
            raise RuntimeError("Diagnostics unavailable")
        return {"diagnostic": name}

    def stop():
        nonlocal controller_alive
        events.append("stop")
        controller_alive = False

    def make_process(**kwargs):
        return Mock(
            start=Mock(),
            stop=Mock(side_effect=stop),
            try_dump_process_state=Mock(side_effect=lambda **kwargs: dump("sensors")),
        )

    monkeypatch.setattr(flow_process, "FlowSimpleProcess", make_process)
    monkeypatch.setattr(flow_process, "BulliedProcess", make_process)
    monkeypatch.setattr(yt_flow_base, "MONITORING_STACK_ENABLED", False)

    base = yt_flow_base.FlowTestBase()
    base.FLOW_BINARY_PATH = "/tmp/flow_server"
    base.path_to_flow_logs = str(tmp_path)
    base.pipeline_path = "//pipeline"
    base.port_manager = Mock()
    base.client = Mock()
    base.client.get_flow_view.side_effect = lambda *args, **kwargs: dump("flow_view")
    base.client.flow_execute.side_effect = lambda *args, **kwargs: dump("description")
    base.client.list_operations.return_value = {"operations": []}
    base.wait_pipeline_state = Mock()
    base._inject_vanilla_block = Mock()

    failure = RuntimeError("Original test failure")
    with pytest.raises(RuntimeError) as error:
        with base.start_flow_process_federation(
            patch_node_config=False,
            use_vanilla_jobs=use_vanilla_jobs,
        ) as federation:
            if early_dump:
                federation.try_dump_final_state()
                events.append("abort")
                controller_alive = False
            raise failure

    assert error.value is failure
    base.client.get_flow_view.assert_called_once_with("//pipeline", cache=False)
    base.client.flow_execute.assert_called_once_with("//pipeline", flow_command="describe-pipeline")
    expected_dumps = ["flow_view", "description"] + ([] if use_vanilla_jobs else ["sensors", "sensors"])
    assert events == expected_dumps + (["abort"] if early_dump else []) + ["stop"] * (1 if use_vanilla_jobs else 3)
    for name in ("final_flow_view.yson", "final_description.yson"):
        assert (tmp_path / name).exists() is not diagnostics_fail
