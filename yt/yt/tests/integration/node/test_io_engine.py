from yt_env_setup import (YTEnvSetup)

from yt_helpers import profiler_factory

import re
import pytest
import platform
import os.path
import threading
import yt

from yt_commands import (authors, wait, read_table, get, ls, create, write_table, set, update_nodes_dynamic_config, get_applied_node_dynamic_config, print_debug)
from yt.common import YtError


@authors("capone212")
class TestIoEngine(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 0
    NODE_IO_ENGINE_TYPE = "thread_pool"

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            "memory_limits": {
                "lookup_rows_cache": {
                    "type": "static",
                    "value": 0
                },
                "block_cache": {
                    "type": "static",
                    "value": 0
                }
            },
            "tablet_static": {
                "type": "static",
                "value": 0
            }
        },
        "data_node": {
            "p2p": {
                "enabled": False,
            },
            "block_cache": {
                "compressed_data": {
                    "capacity": 0,
                },
                "uncompressed_data": {
                    "capacity": 0,
                },
            },
        }
    }

    def get_write_sensors(self, node):
        node_profiler = profiler_factory().at_node(node)
        return [
            node_profiler.counter(name="location/written_bytes", tags={"location_type": "store"}),
            node_profiler.counter(name="location/write/request_count", tags={"location_type": "store"}),
            node_profiler.counter(name="location/write/total_time", tags={"location_type": "store"}),
        ]

    def get_read_sensors(self, node):
        node_profiler = profiler_factory().at_node(node)
        return [
            node_profiler.counter(name="location/read_bytes", tags={"location_type": "store"}),
            node_profiler.counter(name="location/read/request_count", tags={"location_type": "store"}),
            node_profiler.counter(name="location/read/total_time", tags={"location_type": "store"}),
        ]

    def get_pending_read_memory(self, node):
        return get("//sys/cluster_nodes/{}/@statistics/memory/pending_disk_read/used".format(node))

    def get_pending_write_memory(self, node):
        return get("//sys/cluster_nodes/{}/@statistics/memory/pending_disk_write/used".format(node))

    def get_session_summary_allocation(self, node):
        sessions = get("//sys/cluster_nodes/{}/orchid/data_node/session_manager/sessions".format(node))

        sum = 0

        for session in sessions:
            sum += session['heap_usage']

        return sum

    def check_node_sensors(self, node_sensors):
        for sensor in node_sensors:
            value = sensor.get()
            if (value is None) or (sensor.get() == 0):
                return False
        return True

    @authors("capone212")
    def test_disk_statistics(self):
        create("table", "//tmp/t")
        REPLICATION_FACTOR = 3
        set("//tmp/t/@replication_factor", REPLICATION_FACTOR)
        nodes = ls("//sys/cluster_nodes")
        write_sensors = [
            self.get_write_sensors(node) for node in nodes
        ]
        write_table("//tmp/t", [{"a": i} for i in range(100)])
        wait(lambda: sum(1 for node_sensor in write_sensors if self.check_node_sensors(node_sensor)) >= REPLICATION_FACTOR)
        # check read stats
        read_sensors = [
            self.get_read_sensors(node) for node in nodes
        ]
        assert len(read_table("//tmp/t")) != 0
        # we should receive read stats from at least one node
        wait(lambda: any(self.check_node_sensors(node_sensor) for node_sensor in read_sensors))

    @authors("don-dron")
    def test_pending_read_write_memory_tracking(self):
        REPLICATION_FACTOR = 2

        update_nodes_dynamic_config({
            "data_node": {
                "testing_options": {
                    "delay_before_blob_session_block_free": 100000,
                },
            },
        })

        nodes = ls("//sys/cluster_nodes")
        create(
            "table",
            "//tmp/test",
            attributes={
                "replication_factor": REPLICATION_FACTOR,
            })

        ys = [{"key": "x" * (8 * 1024)} for i in range(1024)]
        write_table("//tmp/test", ys)

        wait(lambda: any(self.get_pending_write_memory(node) > 1024 for node in nodes))

        read_table("//tmp/test")

        wait(lambda: any(self.get_pending_read_memory(node) > 1024 for node in nodes))

    @authors("don-dron")
    def test_io_engine_request_limit(self):
        path = "//tmp/table"
        create("table", path)
        write_table(path, [{"a": 1}])
        update_nodes_dynamic_config({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        "io_config": {
                            "read_request_limit": 0,
                            "write_request_limit": 10000,
                        }
                    }
                }
            }
        })

        nodes = ls("//sys/cluster_nodes")

        def seed_counter(node, path):
            return profiler_factory().at_node(node).counter(path)

        counters = [seed_counter(node, "location/throttled_reads") for node in nodes]
        read_res = read_table(path, return_response=True, table_reader={"probe_peer_count": 1})
        wait(lambda: any(counter.get_delta() > 0 for counter in counters))

        update_nodes_dynamic_config({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        "io_config": {
                            "read_request_limit": 10000,
                            "write_request_limit": 0,
                        }
                    }
                }
            }
        })

        counters = [seed_counter(node, "location/throttled_writes") for node in nodes]
        write_res = write_table(path, [{"a": 1}], return_response=True)
        wait(lambda: any(counter.get_delta() > 0 for counter in counters))

        update_nodes_dynamic_config({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        "io_config": {
                            "read_request_limit": 10000,
                            "write_request_limit": 10000,
                        }
                    }
                }
            }
        })

        read_res.wait()
        write_res.wait()

    @authors("don-dron")
    def test_rpc_server_queue_size_limit(self):
        REPLICATION_FACTOR = 1

        update_nodes_dynamic_config({
            "rpc_server": {
                "services": {
                    "DataNodeService": {
                        "methods": {
                            "PutBlocks": {
                                "max_queue_size": -1
                            }
                        }
                    }
                }
            }
        })

        create(
            "table",
            "//tmp/test",
            attributes={
                "replication_factor": REPLICATION_FACTOR,
            })

        ys = [{"key": "x"}]

        with pytest.raises(YtError, match="Request queue size limit exceeded"):
            write_table("//tmp/test", ys)

        # Reset nodes dynamic config to defaults.
        set("//sys/cluster_nodes/@config", {})

        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get_applied_node_dynamic_config(node) == {})

        write_table("//tmp/test", ys)

    def _run_throttled(self, delta, is_read, need_throttle):
        nodes = ls("//sys/cluster_nodes")

        def seed_counter(node, path):
            return profiler_factory().at_node(node).counter(path)

        update_nodes_dynamic_config(delta)
        counters = [seed_counter(node, "location/throttled_reads" if is_read else "location/throttled_writes") for node in nodes]

        first_response = read_table("//tmp/test", return_response=True, table_reader={"probe_peer_count": 1}) if is_read else write_table("//tmp/test", [{"key": "x"}], return_response=True)
        second_response = read_table("//tmp/test", return_response=True, table_reader={"probe_peer_count": 1}) if is_read else write_table("//tmp/test", [{"key": "x"}], return_response=True)

        if need_throttle:
            wait(lambda: any(counter.get_delta() > 0 for counter in counters))
        else:
            wait(lambda: all(counter.get_delta() == 0 for counter in counters))

        update_nodes_dynamic_config({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        "read_memory_limit": 10000000,
                        "write_memory_limit": 10000000,
                        "pending_io_read_limit": 10000000,
                        "pending_io_write_limit": 10000000,
                        "session_count_limit": 10000000,
                    }
                }
            }
        })
        first_response.wait()
        second_response.wait()

    @authors("don-dron")
    def test_location_limits(self):
        REPLICATION_FACTOR = self.NUM_NODES

        create(
            "table",
            "//tmp/test",
            attributes={
                "primary_medium": "default",
                "replication_factor": REPLICATION_FACTOR,
            })

        self._run_throttled({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        'write_memory_limit': -1,
                    }
                }
            }
        }, False, True)

        self._run_throttled({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        "pending_io_write_limit": -1,
                    }
                }
            }
        }, False, True)

        self._run_throttled({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {}
                }
            }
        }, False, False)

        self._run_throttled({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        'read_memory_limit': -1,
                    }
                }
            }
        }, True, True)

        self._run_throttled({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        'pending_io_read_limit': -1,
                    }
                }
            }
        }, True, True)

        self._run_throttled({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {}
                }
            }
        }, True, False)

    @authors("yuryalekseev")
    def test_rpc_server_queue_bytes_size_limit(self):
        REPLICATION_FACTOR = 1

        update_nodes_dynamic_config({
            "rpc_server": {
                "services": {
                    "DataNodeService": {
                        "methods": {
                            "PutBlocks": {
                                "max_queue_byte_size": -1
                            }
                        }
                    }
                }
            }
        })

        create(
            "table",
            "//tmp/test",
            attributes={
                "replication_factor": REPLICATION_FACTOR,
            })

        ys = [{"key": "x"}]

        with pytest.raises(YtError, match="Request queue bytes size limit exceeded"):
            write_table("//tmp/test", ys)

        # Reset nodes dynamic config to defaults.
        set("//sys/cluster_nodes/@config", {})

        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get_applied_node_dynamic_config(node) == {})

        write_table("//tmp/test", ys)

    @authors("don-dron")
    @pytest.mark.skip(reason="The tcmalloc's patch 'user_data.patch' does NOT process user_data in StackTrace's hash")
    def test_read_write_session_allocation_tracking(self):
        REPLICATION_FACTOR = 1

        update_nodes_dynamic_config({
            "data_node": {
                "testing_options": {
                    "delay_before_blob_session_block_free": 100000,
                },
            },
        })

        nodes = ls("//sys/cluster_nodes")
        create(
            "table",
            "//tmp/test",
            attributes={
                "replication_factor": REPLICATION_FACTOR,
            })

        ys = [{"key": "x" * (8 * 1024)} for i in range(1024)]
        write_table("//tmp/test", ys)

        wait(lambda: any(self.get_session_summary_allocation(node) > 1024 for node in nodes))

        read_table("//tmp/test")

        wait(lambda: any(self.get_session_summary_allocation(node) > 1024 for node in nodes))

    @authors("prime")
    def test_dynamic_sick_detector(self):
        create("table", "//tmp/sick")
        write_table("//tmp/sick", [{"a": i} for i in range(100)])

        def get_sick_count():
            return sum(
                profiler_factory().at_node(node).gauge(name="location/sick", fixed_tags={"location_type": "store"}).get()
                for node in ls("//sys/cluster_nodes")
            )

        assert get_sick_count() == 0

        update_nodes_dynamic_config({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        "io_config": {
                            "sick_write_time_threshold": 0,
                            "sick_write_time_window": 0,
                            "sickness_expiration_timeout": 1000000,
                        }
                    }
                }
            }
        })

        write_table("//tmp/sick", [{"a": i} for i in range(100)])
        wait(lambda: get_sick_count() > 0)

    @authors("prime")
    def test_dynamic_io_engine(self):
        create("table", "//tmp/table")

        def use_engine(name):
            update_nodes_dynamic_config({
                "data_node": {
                    "store_location_config_per_medium": {
                        "default": {
                            "io_engine_type": name
                        }
                    }
                }
            })

            def enabled_engines():
                return sum(
                    profiler_factory().at_node(node).gauge(name="location/engine_enabled", fixed_tags={"location_type": "store", "engine_type": name}).get()
                    for node in ls("//sys/cluster_nodes")
                )

            wait(lambda: enabled_engines() == self.NUM_NODES)

            write_table("//tmp/table", [{"a": i} for i in range(100)])

        use_engine("thread_pool")
        use_engine("fair_share_thread_pool")

        if not is_uring_supported() or is_uring_disabled():
            return

        # uring is broken in CI
        # use_engine("uring")


def parse_version(vstring):
    pattern = r'^(\d+)\.(\d+)\.(\d+).*'
    match = re.match(pattern, vstring)
    if not match:
        raise ValueError("invalid version number '%s'" % vstring)
    (major, minor, patch) = match.group(1, 2, 3)
    return tuple(map(int, [major, minor, patch]))


def is_uring_supported():
    if platform.system() != "Linux":
        return False
    try:
        return parse_version(platform.release()) >= (5, 4, 0)
    except Exception:
        pass

    return False


def is_uring_disabled():
    proc_file = "/proc/sys/kernel/io_uring_perm"
    if not os.path.exists(proc_file):
        return False
    with open(proc_file, "r") as myfile:
        return myfile.read() == '0'


@authors("capone212")
@pytest.mark.skip("YT-15905 io_uring is broken in CI")
@pytest.mark.skipif(not is_uring_supported() or is_uring_disabled(), reason="io_uring is not available on this host")
class TestIoEngineUringStats(TestIoEngine):
    NODE_IO_ENGINE_TYPE = "uring"


@authors("don-dron")
class BaseTestRpcMemoryTracking(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 0

    @classmethod
    def get_rpc_memory_used(self, node):
        return get("//sys/cluster_nodes/{}/@statistics/memory/rpc/used".format(node))

    @classmethod
    def get_server_connections_sensor(self, node):
        node_profiler = profiler_factory().at_node(node)
        return node_profiler.counter(name="bus/server_connections")

    @classmethod
    def get_client_connections_sensor(self, node):
        node_profiler = profiler_factory().at_node(node)
        return node_profiler.counter(name="bus/client_connections")

    @classmethod
    def check_node(self, node, value=32768):
        # 32768 = 16384 + 16384 = ReadBufferSize + WriteBufferSize, see yt/core/bus/tcp/connection.cpp
        server_connections_sensor = self.get_server_connections_sensor(node)
        server_connections = server_connections_sensor.get()

        client_connections_sensor = self.get_client_connections_sensor(node)
        client_connections = client_connections_sensor.get()

        if server_connections is None or server_connections == 0:
            print_debug("No server connections")
            return False

        total_connections = server_connections + client_connections
        rpc_used = self.get_rpc_memory_used(node)
        expected_used = value * total_connections
        print_debug((
            f"rpc memory used: {rpc_used}, server connections: {server_connections}, "
            f"client connections: {client_connections}, expected memory used: {expected_used}"
        ))

        return self.get_rpc_memory_used(node) == expected_used


@authors("don-dron")
class TestRpcBuffersMemoryTracking(BaseTestRpcMemoryTracking):
    DELTA_NODE_CONFIG = {
        "bus_server": {
            "connection_start_delay": 10000
        }
    }

    @authors("don-dron")
    def test_tcp_socket_buffers_tracking(self):
        REPLICATION_FACTOR = 1

        nodes = ls("//sys/cluster_nodes")
        create(
            "table",
            "//tmp/test",
            attributes={
                "replication_factor": REPLICATION_FACTOR,
            })

        ys = [{"key": "x"} for i in range(16)]
        response = write_table("//tmp/test", ys, return_response=True)

        wait(lambda: any(self.check_node(node) for node in nodes))

        response.wait()


@authors("don-dron")
class TestRpcDecoderMemoryTracking(BaseTestRpcMemoryTracking):
    DELTA_NODE_CONFIG = {
        "bus_server": {
            "packet_decoder_delay": 1000
        }
    }

    @authors("don-dron")
    def test_tcp_socket_decoder_tracking(self):
        REPLICATION_FACTOR = 1

        nodes = ls("//sys/cluster_nodes")
        for i in range(4):
            create(
                "table",
                "//tmp/test{}".format(i),
                attributes={
                    "replication_factor": REPLICATION_FACTOR,
                })

        ys = [{"key": "x" * (1024 * 1024)}]
        node = nodes[0]

        responses = []

        def write(index):
            yw = yt.wrapper.YtClient(proxy=self.Env.get_proxy_address())
            for _ in range(4):
                yw.write_table("//tmp/test{}".format(index), ys)

        for i in range(4):
            responses.append(threading.Thread(target=write, args=i))

        for response in responses:
            response.start()

        def check_node_with_rct(node, value=32768):
            # 32768 = 16384 + 16384 = ReadBufferSize + WriteBufferSize, see yt/core/bus/tcp/connection.cpp
            sensor = self.get_server_connections_sensor(node)
            connections = sensor.get()
            if connections is None or connections == 0:
                return False
            stat = get("//sys/cluster_nodes/{}/orchid/monitoring/ref_counted/statistics".format(node))

            if stat is None:
                return False

            def find_in_rct(tag):
                for i in stat:
                    if i['name'] == tag:
                        return i

                return None

            rct_write = find_in_rct("NYT::NBus::TTcpServerConnectionReadBufferTag")
            rct_read = find_in_rct("NYT::NBus::TTcpServerConnectionWriteBufferTag")
            rct_decoder = find_in_rct("NYT::NBus::TPacketDecoderTag")

            rct_write_bytes_alive = 0 if rct_write is None else rct_write['bytes_alive']
            rct_read_bytes_alive = 0 if rct_read is None else rct_read['bytes_alive']
            rct_decoder_bytes_alive = 0 if rct_decoder is None else rct_read['bytes_alive']

            used = self.get_rpc_memory_used(node)

            print_debug("Used: {}, RefCountedTrackerReadBufferTag: {}, RefCountedTrackerWriteBufferTag: {}, RefCountedTrackerDecoderTag: {}".format(
                used, rct_read_bytes_alive, rct_write_bytes_alive, rct_decoder_bytes_alive))

            return connections * value == rct_read_bytes_alive + rct_write_bytes_alive and used >= rct_write_bytes_alive + rct_read_bytes_alive

        wait(lambda: check_node_with_rct(node))

        for response in responses:
            response.join()
