from .conftest import (
    Cli,
    create_nodes,
    create_pod_with_boilerplate,
)

import yt.yson as yson

import pytest


class ScriptWrapper(Cli):
    def __init__(self, name):
        binary_name = "yp-" + name.replace("_", "-")
        super(ScriptWrapper, self).__init__("yp/scripts/{}/{}".format(name, binary_name))
        self.set_config(dict(enable_ssl=False))

    def set_config(self, config):
        self._config = yson.dumps(config, yson_format="text")

    def get_args(self, args):
        return super(ScriptWrapper, self).get_args(args) + ["--config", self._config]


@pytest.mark.usefixtures("yp_env")
class TestScripts(object):
    def test_touch_pod_master_spec_timestamp(self, yp_env, tmpdir):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        node_ids = create_nodes(yp_client, 10)
        pod_ids = [
            create_pod_with_boilerplate(yp_client, pod_set_id, {"node_id": node_id})
            for node_id in node_ids
        ]

        def get_timestamps():
            return [
                t[0]
                for t in yp_client.get_objects(
                    "pod", pod_ids, selectors=["/status/master_spec_timestamp"]
                )
            ]

        timestamps1 = get_timestamps()

        grpc_address = yp_env.yp_instance.yp_client_grpc_address
        script_wrapper = ScriptWrapper("touch_pod_master_spec_timestamps")

        def run_script(filter=None, node=None, node_list=None, batch_size=None, dry_run=False):
            if node_list:
                file = tmpdir.join("test_nodes")
                file.write("\n".join(node_list))
                node_list = str(file)

            script_wrapper.check_output(
                ["--cluster", grpc_address]
                + (["--dry-run"] if dry_run else [])
                + (["--filter", filter] if filter else [])
                + (["--node", node] if node else [])
                + (["--batch-size", batch_size] if batch_size else [])
                + (["--node-list", node_list] if node_list else [])
            )

        run_script()
        timestamps2 = get_timestamps()
        assert all(timestamps2[i] > timestamps1[i] for i in range(10))
        timestamps1 = timestamps2

        run_script(batch_size="1")
        timestamps2 = get_timestamps()
        assert all(timestamps2[i] > timestamps1[i] for i in range(10))
        timestamps1 = timestamps2

        run_script(batch_size="9")
        timestamps2 = get_timestamps()
        assert all(timestamps2[i] > timestamps1[i] for i in range(10))
        timestamps1 = timestamps2

        run_script(batch_size="500")
        timestamps2 = get_timestamps()
        assert all(timestamps2[i] > timestamps1[i] for i in range(10))
        timestamps1 = timestamps2

        run_script(dry_run=True)
        timestamps2 = get_timestamps()
        assert timestamps1 == timestamps2
        timestamps1 = timestamps2

        run_script(node=node_ids[0])
        timestamps2 = get_timestamps()
        assert timestamps2[0] > timestamps1[0] and timestamps1[1:] == timestamps2[1:]
        timestamps1 = timestamps2

        run_script(filter='[/meta/id]="{}"'.format(pod_ids[0]))
        timestamps2 = get_timestamps()
        assert timestamps2[0] > timestamps1[0] and timestamps1[1:] == timestamps2[1:]
        timestamps1 = timestamps2

        run_script(node_list=node_ids[:5])
        timestamps2 = get_timestamps()
        assert (
            all(timestamps2[i] > timestamps1[i] for i in range(5))
            and timestamps1[5:] == timestamps2[5:]
        )
        timestamps1 = timestamps2
