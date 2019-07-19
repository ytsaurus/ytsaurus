from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE
from yt_commands import *

import sys
import os.path

#################################################################

def trim(filepath):
    with open(filepath, "w"):
        pass

class TestChunkCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    selected_node = None

    def select_node(self):
        if self.selected_node is None:
            self.selected_node = ls("//sys/cluster_nodes")[0]
        return self.selected_node

    def config_nodes(self):
        node = self.select_node()
        set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["run_here"])

    def damage_chunks(self):
        path = "//sys/cluster_nodes/{0}/orchid/cached_chunks".format(self.select_node())
        cached_chunks = ls(path)

        for chunk in cached_chunks:
            location = get(os.path.join(path, chunk, "location"))
            # If chunk location method is changed, check this line
            file_loc = os.path.join(location, chunk[-2:], chunk)
            assert os.path.isfile(file_loc)
            trim(file_loc)
            assert os.path.getsize(file_loc) == 0

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
                    "file_paths": ["//tmp/file.py"]
                }
            }
        )

    def test_lazy_chunk_length_validation(self):
        self.config_nodes()

        create("file", "//tmp/file.py")
        write_file("//tmp/file.py", """
import sys

for line in sys.stdin:
    print(line)
        """)

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        write_table("//tmp/t1", {"foo": "bar"})
        self.run_map()
        assert read_table("//tmp/t2") == [{"foo": "bar"}]

        self.damage_chunks()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        self.run_map()
        assert read_table("//tmp/t2") == [{"foo": "bar"}]

