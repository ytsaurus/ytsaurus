from yt_env_setup import YTEnvSetup

from yt_helpers import profiler_factory

from yt_commands import (
    authors,
    wait,
    ls,
    get,
    set,
    create,
    write_table,
    remove,
    print_debug,
)

import yt.packages.requests as requests
import yt.yson as yson

import pytest

##################################################################


@pytest.mark.enabled_multidaemon
class MetricsTestBase(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 2

    DELTA_PROXY_CONFIG = {
        "coordinator": {
            "heartbeat_interval": 100,
            "death_age": 500,
            "cypress_timeout": 50,
        },
        "api": {
            "force_tracing": True,
        },
        "access_checker": {
            "enabled": True,
            "cache": {
                "expire_after_access_time": 100,
            },
        },
    }

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _get_build_snapshot_url(self):
        return self._get_proxy_address() + "/api/v4/build_snapshot"

    def _get_master_address(self):
        return ls("//sys/primary_masters")[0]

    def _get_hydra_monitoring(self, master=None):
        if master is None:
            master = self._get_master_address()
        return get(
            "//sys/primary_masters/{}/orchid/monitoring/hydra".format(master),
            suppress_transaction_coordinator_sync=True,
            default={},
        )


class TestPortoMetrics(MetricsTestBase):
    ENABLE_MULTIDAEMON = False  # Metrics test.
    SUSPENDING_TABLE = "//tmp/suspending_table"
    DELAY_BEFORE_COMMAND = 10 * 1000
    KEEP_ALIVE_PERIOD = 1 * 1000
    DELTA_PROXY_CONFIG = {
        "enable_porto_resource_tracker": True,
        "api": {
            "testing": {
                "delay_before_command": {
                    "get": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/path",
                        "substring": SUSPENDING_TABLE,
                    },
                    "get_table_columnar_statistics": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/paths/0",
                        "substring": SUSPENDING_TABLE,
                    },
                    "read_table": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/path",
                        "substring": SUSPENDING_TABLE,
                    },
                }
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "enable_porto_resource_tracker": True,
    }

    FRAME_TAG_TO_NAME = {
        0x01: "data",
        0x02: "keep_alive",
    }

    @authors("don-dron")
    def test_porto_metrics(self):
        proxies = ls("//sys/http_proxies")
        proxy = proxies[0]

        def get_yson(url):
            return yson.loads(requests.get(url).content)

        assert get_yson(self._get_proxy_address() + "/hosts") == [proxy]
        assert get_yson(self._get_proxy_address() + "/hosts?role=data") == [proxy]
        assert get_yson(self._get_proxy_address() + "/hosts?role=control") == []

        set("//sys/http_proxies/" + proxy + "/@role", "control")

        def check_role_updated():
            return get_yson(self._get_proxy_address() + "/hosts") == [] and \
                get_yson(self._get_proxy_address() + "/hosts?role=data") == [] and \
                get_yson(self._get_proxy_address() + "/hosts?role=control") == [proxy]

        # Wait until the proxy entry will be updated on the coordinator.
        wait(check_role_updated)

        gauges = [
            "porto/cpu/burst",
            "porto/cpu/user",
            "porto/cpu/total",
            "porto/cpu/system",
            "porto/cpu/wait",
            "porto/cpu/throttled",
            "porto/cpu/cfs_throttled",
            "porto/cpu/guarantee",
            "porto/cpu/limit",
            "porto/cpu/thread_count",
            "porto/cpu/context_switches",

            "porto/memory/minor_page_faults",
            "porto/memory/major_page_faults",
            "porto/memory/file_cache_usage",
            "porto/memory/anon_usage",
            "porto/memory/anon_limit",
            "porto/memory/memory_usage",
            "porto/memory/memory_guarantee",
            "porto/memory/memory_limit",

            "porto/io/read_bytes",
            "porto/io/write_bytes",
            "porto/io/bytes_limit",

            "porto/io/read_ops",
            "porto/io/write_ops",
            "porto/io/ops",
            "porto/io/ops_limit",
            "porto/io/total",

            "porto/network/rx_bytes",
            "porto/network/rx_drops",
            "porto/network/rx_packets",
            "porto/network/rx_limit",
            "porto/network/tx_bytes",
            "porto/network/tx_drops",
            "porto/network/tx_packets",
            "porto/network/tx_limit",

            "porto/volume/count",

            "porto/layer/count",
        ]

        may_be_empty = [
            "porto/cpu/burst",
            "porto/cpu/wait",
            "porto/cpu/throttled",
            "porto/cpu/cfs_throttled",
            "porto/cpu/guarantee",
            "porto/memory/major_page_faults",
            "porto/memory/memory_guarantee",
            "porto/io/ops_limit",
            "porto/io/read_ops",
            "porto/io/write_ops",
            "porto/io/wait",
            "porto/io/bytes_limit",
            "porto/network/rx_bytes",
            "porto/network/rx_drops",
            "porto/network/rx_packets",
            "porto/network/rx_limit",
            "porto/network/tx_bytes",
            "porto/network/tx_drops",
            "porto/network/tx_packets",
            "porto/network/tx_limit",

            "porto/volume/count",

            "porto/layer/count",
        ]

        def check_node_sensors(node, container_category, node_sensors):
            node_profiler = profiler_factory().at_http_proxy(node)
            for sensor_name in node_sensors:
                sensor = node_profiler.gauge(name=sensor_name, fixed_tags={"container_category": container_category})
                value = sensor.get()
                if ((value is None) or (sensor.get() < 0)) and (sensor.name not in may_be_empty):
                    print("Sensor {0} not found".format(sensor.name))
                    return False
            return True

        wait(lambda: any(check_node_sensors(node, "daemon", gauges) for node in proxies))
        wait(lambda: any(check_node_sensors(node, "pod", gauges) for node in proxies))
        wait(lambda: any(not check_node_sensors(node, "", gauges) for node in proxies))

    @authors("vvshlyaga")
    def test_check_tagged_io_sensors(self):
        taggedSensors = [
            "porto/io/write_bytes",
            "porto/io/read_bytes",
            "porto/io/bytes_limit",

            "porto/io/read_ops",
            "porto/io/write_ops",
            "porto/io/ops",
            "porto/io/ops_limit",
            "porto/io/total",
        ]

        mayBeWithoutTag = [
            "porto/io/read_bytes",
            "porto/io/bytes_limit",

            "porto/io/read_ops",
            "porto/io/write_ops",
            "porto/io/ops",
            "porto/io/ops_limit",
            "porto/io/total",
        ]

        nodes = ls("//sys/cluster_nodes")

        create("table", "//tmp/t_trash", attributes={
            "schema": [{"name": "k", "type": "int64", "sort_order": "ascending"}],
            "replication_factor": 5
        })

        def check_sensors():
            flag = True
            rows = [{"k": i} for i in range(10)]
            write_table("//tmp/t_trash", rows)
            for node in nodes:
                for taggedSensor in taggedSensors:
                    try:
                        value = get(f"//sys/cluster_nodes/{node}/orchid/sensors/yt/{taggedSensor}")
                        print_debug(f"value of sensor {taggedSensor} on {node} = {value}")
                        flag = flag and value is not None and ('device_name' in str(value) or taggedSensor in mayBeWithoutTag)
                    except Exception as err:
                        print(f"Unexpected {err=}, {type(err)=}")
                        return False
            return flag

        wait(check_sensors)
        remove("//tmp/t_trash")
