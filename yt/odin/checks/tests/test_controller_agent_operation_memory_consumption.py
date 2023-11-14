from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, wait


def test_controller_operation_memory_consumption(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    agents = yt_client.get("//sys/controller_agents/instances")
    assert len(agents) > 0

    checks_path = make_check_dir("controller_agent_operation_memory_consumption")
    with configure_odin(proxy_url, checks_path) as odin:
        storage = odin.create_db_client()

        def get_states():
            return storage.get_service_states("controller_agent_operation_memory_consumption")

        run_checks(odin)
        wait(lambda: len(get_states()) == 1)
        assert abs(get_states()[-1] - FULLY_AVAILABLE_STATE) <= 0.001

        yt_client.write_table("//tmp/input", [{"key": "value"}])
        op = yt_client.run_map("sleep 1000", "//tmp/input", "//tmp/output", format="json", sync=False)
        wait(lambda: op.get_state() == "running")

        run_checks(odin)
        wait(lambda: len(get_states()) == 2)
        assert abs(get_states()[-1] - FULLY_AVAILABLE_STATE) <= 0.001

        op.abort()


def test_controller_operation_memory_consumption_fail(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    agents = yt_client.get("//sys/controller_agents/instances")
    assert len(agents) > 0

    check_options = dict(
        memory_threshold=10
    )
    checks_path = make_check_dir("controller_agent_operation_memory_consumption", check_options)
    with configure_odin(proxy_url, checks_path) as odin:
        storage = odin.create_db_client()

        def get_states():
            return storage.get_service_states("controller_agent_operation_memory_consumption")

        run_checks(odin)
        wait(lambda: len(get_states()) == 1)
        assert abs(get_states()[-1] - FULLY_AVAILABLE_STATE) <= 0.001

        yt_client.write_table("//tmp/input", [{"key": "value"}])

        spec = {
            "testing": {
                "allocation_size": 200 * 1024 * 1024,
            }
        }
        op = yt_client.run_map("sleep 5000", "//tmp/input", "//tmp/output", format="json", sync=False, spec=spec)
        wait(lambda: op.get_state() == "running")

        controller_agents = yt_client.list("//sys/controller_agents/instances")
        assert len(controller_agents) == 1

        agent = controller_agents[0]
        orchid_path = "//sys/controller_agents/instances/{}/orchid/controller_agent/tagged_memory_statistics".format(agent)
        wait(lambda: len(yt_client.get(orchid_path)) > 0)

        state_count = 1

        def is_unavailable():
            run_checks(odin)
            nonlocal state_count
            state_count += 1
            wait(lambda: len(get_states()) == state_count)
            return abs(get_states()[-1] - UNAVAILABLE_STATE) <= 0.001

        wait(is_unavailable)

        op.abort()
