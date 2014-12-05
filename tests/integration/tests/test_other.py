from yt_env_setup import YTEnvSetup
from yt_commands import *

import time
import os

##################################################################

class TestResourceLeak(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    DELTA_NODE_CONFIG = {"data_node" : {"session_timeout": 100}}

    def _check_no_temp_file(self, chunk_store):
        for root, dirs, files in os.walk(chunk_store):
            for file in files:
                assert not file.endswith("~") or file == "health_check~", "Found temporary file: " + file

    # should be called on empty nodes
    def test_canceled_upload(self):
        class InputStream(object):
            def read(self):
                time.sleep(1)
                raise Exception("xxx")

        tx = start_transaction(timeout=2000)

        # uploading from empty stream will fail
        create("file", "//tmp/file")

        try:
            command("upload", parameters={"path": "//tmp/file", "tx": tx}, input_stream=InputStream())
        except YtError:
            time.sleep(1)
            # now check that there are no temp files
            for i in xrange(self.NUM_NODES):
                # TODO(panin): refactor
                node_config = self.Env.configs["node"][i]
                chunk_store_path = node_config["data_node"]["store_locations"][0]["path"]
                self._check_no_temp_file(chunk_store_path)
