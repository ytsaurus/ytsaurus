from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, wait,
    ls, get, set, create, map,
    read_table, write_table, write_file)

import os.path

#################################################################


def trim(filepath):
    with open(filepath, "wb"):
        pass


class TestChunkCache(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {"data_node": {"background_artifact_validation_delay": 0}}

    selected_node = None
    prev_obj = None

    def select_node(self):
        if self.selected_node is None:
            self.selected_node = ls("//sys/cluster_nodes")[0]
        return self.selected_node

    def config_nodes(self):
        node = self.select_node()
        set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["run_here"])

    def damage_chunks(self):
        path = "//sys/cluster_nodes/{0}/orchid/exec_node/cached_chunks".format(self.select_node())
        cached_chunks = ls(path)

        for chunk in cached_chunks:
            location = get(os.path.join(path, chunk, "location"))
            # If chunk store layout is changed, check this line
            file_loc = os.path.join(location, chunk[-2:], chunk)
            assert os.path.isfile(file_loc)
            trim(file_loc)
            assert os.path.getsize(file_loc) == 0

    def get_online_node_count(self):
        count = 0
        nodes = get("//sys/scheduler/orchid/scheduler/nodes")
        for node in nodes.values():
            if node["state"] == "online":
                count += 1
        return count

    def restart(self):
        with Restarter(self.Env, NODES_SERVICE):
            wait(
                lambda: self.get_online_node_count() == 0,
                "Scheduler doesn't know that nodes are dead",
                sleep_backoff=1,
            )

        wait(
            lambda: self.get_online_node_count() == self.NUM_NODES,
            "Scheduler doesn't know that nodes are alive",
            sleep_backoff=1,
        )

    def run_map(self):
        return map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="python3 file.py",
            spec={
                "scheduling_tag_filter": "run_here",
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                    "file_paths": ["//tmp/file.py"],
                },
            },
        )

    @authors("ogorod")
    def test_lazy_chunk_length_validation(self):
        self.config_nodes()

        create("file", "//tmp/file.py")
        write_file(
            "//tmp/file.py",
            b"""
import sys

for line in sys.stdin:
    print(line)
        """,
        )

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        write_table("//tmp/t1", {"foo": "bar"})
        self.run_map()
        assert read_table("//tmp/t2") == [{"foo": "bar"}]

        self.damage_chunks()
        self.restart()

        self.run_map()
        assert read_table("//tmp/t2") == [{"foo": "bar"}]
