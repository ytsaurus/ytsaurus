from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_helpers import profiler_factory

from yt_commands import (
    authors, wait, ls, set, get, map, update_nodes_dynamic_config, create,
    write_file, write_table, merge, create_domestic_medium, exists,
    set_account_disk_space_limit, get_account_disk_space_limit, remove)

import yt_error_codes

import os
import time


class TestLocationMisconfigured(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    STORE_LOCATION_COUNT = 2

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 2
        config["data_node"]["store_locations"][1]["medium_name"] = "test"

    @authors("don-dron")
    def test_location_medium_misconfigured(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1

        with Restarter(self.Env, NODES_SERVICE):
            pass

        get("//sys/cluster_nodes/{}/@".format(nodes[0]))

        def check_alerts():
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(nodes[0]))
            return len(alerts) == 1 and alerts[0]["code"] == yt_error_codes.LocationMediumIsMisconfigured

        wait(lambda: check_alerts())


class TestCacheLocation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    STORE_LOCATION_COUNT = 1

    @authors("don-dron")
    def test_disable_cache_location(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        chunk_cache = self.Env.configs["node"][0]["data_node"]["cache_locations"][0]["path"]

        assert not os.path.exists("{}/disabled".format(chunk_cache))

        create("file", "//tmp/file.py")

        create("table", "//tmp/t_input", attributes={"schema": [{"name": "k", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/t_output", attributes={"schema": [{"name": "k", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/table", attributes={"schema": [{"name": "k", "type": "int64", "sort_order": "ascending"}]})
        set("//tmp/t_input/@replication_factor", 1)
        set("//tmp/t_output/@replication_factor", 1)
        set("//tmp/table/@replication_factor", 1)
        set("//tmp/file.py/@replication_factor", 1)

        set("//tmp/t_input/@compression_codec", "none")
        set("//tmp/t_output/@compression_codec", "none")
        set("//tmp/table/@compression_codec", "none")
        set("//tmp/file.py/@compression_codec", "none")

        write_file(
            "//tmp/file.py",
            b"""
import sys

for line in sys.stdin:
    print(line)
        """, file_writer={"upload_replication_factor": 1}
        )
        rows = [{"k": i} for i in range(10)]
        write_table("//tmp/t_input", rows)
        write_table("//tmp/t_output", rows)

        merge(in_=["//tmp/t_input[2:6,4:8]", "//tmp/t_output[1:3,5:7]"],
              out="//tmp/table",
              mode="sorted",
              spec={"data_weight_per_job": 1})

        assert get("//tmp/table/@chunk_count") > 1

        update_nodes_dynamic_config({
            "exec_node": {
                "chunk_cache": {
                    "test_cache_location_disabling": True,
                }
            }
        })
        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="python3 file.py",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                    "file_paths": ["//tmp/file.py", "<format=json>//tmp/table"],
                },
            },
            track=False
        )

        wait(lambda: os.path.exists("{}/disabled".format(chunk_cache)))

        assert os.path.exists("{}/disabled".format(chunk_cache))

        os.remove("{}/disabled".format(chunk_cache))

        update_nodes_dynamic_config({
            "exec_node": {
                "chunk_cache": {
                    "test_cache_location_disabling": False,
                }
            }
        })

        with Restarter(self.Env, NODES_SERVICE):
            pass

        op.track()


class TestPerLocationFullHeartbeats(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    STORE_LOCATION_COUNT = 10

    @classmethod
    def setup_class(cls):
        super().setup_class()

        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        for i in range(cls.STORE_LOCATION_COUNT):
            set_account_disk_space_limit("tmp", disk_space_limit, medium=f"hdd{i}")

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == cls.STORE_LOCATION_COUNT

        for i in range(cls.STORE_LOCATION_COUNT):
            config["data_node"]["store_locations"][i]["medium_name"] = f"hdd{i}"

    @classmethod
    def on_masters_started(cls):
        for i in range(cls.STORE_LOCATION_COUNT):
            create_domestic_medium(f"hdd{i}")

    @authors("danilalexeev")
    def test_interrupt_full_heartbeat_session(self):
        # Create chunk on every medium.
        for i in range(self.STORE_LOCATION_COUNT):
            table_path = f"//tmp/t{i}"
            medium_name = f"hdd{i}"
            create("table", table_path, recursive=True, attributes={
                "replication_factor": 1,
                "primary_medium": medium_name,
            })
            assert exists(f"{table_path}/@media/{medium_name}")
            write_table(table_path, [{"key": "value"}])

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        update_nodes_dynamic_config({
            "data_node": {
                "master_connector": {
                    "full_heartbeat_session_sleep_duration": 1000,
                },
            }
        })
        # Now a data node full heartbeat session will take not less than 10s.

        with Restarter(self.Env, NODES_SERVICE, sync=False):
            pass

        time.sleep(3)

        # Full heartbeat is in session.
        assert get(f"//sys/cluster_nodes/{node}/@state") == "registered"

        set("//sys/@config/chunk_manager/data_node_tracker/enable_per_location_full_heartbeats", False)

        wait(lambda: get(f"//sys/cluster_nodes/{node}/@state") == "online")


class TestAsyncTrashLoad(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    STORE_LOCATION_COUNT = 1

    MEDIUM_NAME = "test_async_trash_load"

    PATCHED_NODE_CONFIGS = []

    @classmethod
    def setup_class(cls):
        super().setup_class()

        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        set_account_disk_space_limit("tmp", disk_space_limit, medium=cls.MEDIUM_NAME)

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium(cls.MEDIUM_NAME)

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        cls.PATCHED_NODE_CONFIGS.append(config)

        assert len(config["data_node"]["store_locations"]) == cls.STORE_LOCATION_COUNT

        for i in range(cls.STORE_LOCATION_COUNT):
            config["data_node"]["store_locations"][i]["trash_check_period"] = 10000
            config["data_node"]["store_locations"][i]["medium_name"] = cls.MEDIUM_NAME

    @authors("vvshlyaga")
    def test_async_trash_load(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1

        node = nodes[0]

        def get_sensor(sensor_name):
            return profiler_factory().at_node(node).gauge(name=sensor_name).get()

        def check_sensor_is_not_zero(sensor_name):
            sensor = get_sensor(sensor_name)
            return sensor is not None and sensor != 0

        create("table", "//tmp/t_trash", attributes={
            "schema": [{"name": "k", "type": "int64", "sort_order": "ascending"}],
            "replication_factor": 1,
            "primary_medium": self.MEDIUM_NAME
        })

        rows = [{"k": i} for i in range(10)]
        write_table("//tmp/t_trash", rows)

        remove("//tmp/t_trash")

        wait(lambda: check_sensor_is_not_zero('location/trash_space'))
        wait(lambda: check_sensor_is_not_zero('location/trash_chunk_count'))

        update_nodes_dynamic_config({
            "data_node": {
                "testing_options": {
                    "enable_trash_scanning_barrier": True
                }
            }
        })

        with Restarter(self.Env, NODES_SERVICE):
            for node_config in self.PATCHED_NODE_CONFIGS:
                for i in range(self.STORE_LOCATION_COUNT):
                    node_config["data_node"]["enable_trash_scanning_barrier"] = True
            self.Env.rewrite_node_configs()

        time.sleep(10)

        wait(lambda: get(f"//sys/cluster_nodes/{node}/@state") == "online")

        def check_sensor_is_zero(sensor_name):
            sensor = get_sensor(sensor_name)
            return sensor is not None and sensor == 0

        wait(lambda: check_sensor_is_zero('location/trash_space'))
        wait(lambda: check_sensor_is_zero('location/trash_chunk_count'))

        trash_space = get_sensor('location/trash_space')
        trash_chunk_count = get_sensor('location/trash_chunk_count')

        update_nodes_dynamic_config({
            "data_node": {
                "testing_options": {
                    "enable_trash_scanning_barrier": False
                }
            }
        })

        def wait_sensor_change(sensor_name, prev_value):
            sensor = get_sensor(sensor_name)
            return sensor is not None and sensor != prev_value

        wait(lambda: wait_sensor_change('location/trash_space', trash_space))
        wait(lambda: wait_sensor_change('location/trash_chunk_count', trash_chunk_count))
