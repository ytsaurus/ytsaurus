from yt_env_setup import YTEnvSetup

from yt_helpers import profiler_factory

from yt_commands import (
    authors, concatenate, create, get, ls, map, read_file, read_table, set,
    update_nodes_dynamic_config, wait, write_file, write_table)

import hashlib
import io
import pytest

import zstandard as zstd

from os import listdir
from os.path import join, isfile


class TestParallelFileArtifactDownload(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    def _make_multichunk_file(self, chunk_count):
        chunk_paths = []
        expected = b""
        for index in range(chunk_count):
            path = "//tmp/part{}".format(index)
            create("file", path)
            set(path + "/@replication_factor", 1)
            data = (str(index) * 32).encode("ascii") * (1000 + 137 * index)
            write_file(path, data, file_writer={"upload_replication_factor": 1})
            chunk_paths.append(path)
            expected += data

        expected_md5 = hashlib.md5(expected).hexdigest()

        create("file", "//tmp/multichunk")
        set("//tmp/multichunk/@replication_factor", 1)
        concatenate(chunk_paths, "//tmp/multichunk")
        assert get("//tmp/multichunk/@chunk_count") == chunk_count

        # Sanity check: the multi-chunk file must be readable and equal to the concatenation of its chunks.
        assert read_file("//tmp/multichunk") == expected

        return expected_md5

    def _artifact_cache_size(self, node):
        total = 0
        for segment in ("younger", "older"):
            value = profiler_factory().at_node(node).gauge(
                name="exec_node/artifact_cache/size",
                fixed_tags={"segment": segment},
            ).get()
            if value is not None:
                total += value
        return total

    def _parallel_download_logged(self):
        logs_path = join(self.path_to_run, "logs")
        node_files = [
            join(logs_path, file_name)
            for file_name in listdir(logs_path)
            if "node" in file_name and ".log.zst" in file_name and isfile(join(logs_path, file_name))]
        for file_path in node_files:
            with open(file_path, "rb") as log_file:
                decompressor = zstd.ZstdDecompressor()
                text_stream = io.TextIOWrapper(decompressor.stream_reader(log_file, read_size=8192), encoding="utf-8")
                if any("Downloading file artifact in parallel" in line for line in text_stream):
                    return True
        return False

    def _run_hash_operation(self, expected_md5):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        set("//tmp/t_in/@replication_factor", 1)
        set("//tmp/t_out/@replication_factor", 1)
        write_table("//tmp/t_in", [{"x": 1}], table_writer={"upload_replication_factor": 1})

        command = "md5sum multichunk | awk '{print \"{\\\"hash\\\": \\\"\" $1 \"\\\"}\"}'"

        # The first operation downloads the artifact into the exec node cache.
        map(
            in_="//tmp/t_in",
            out="<append=true>//tmp/t_out",
            command=command,
            spec={"mapper": {
                "input_format": "json",
                "output_format": "json",
                "file_paths": ["//tmp/multichunk"],
            }},
        )

        # The second operation must reuse the cached artifact.
        map(
            in_="//tmp/t_in",
            out="<append=true>//tmp/t_out",
            command=command,
            spec={"mapper": {
                "input_format": "json",
                "output_format": "json",
                "file_paths": ["//tmp/multichunk"],
            }},
        )

        hashes = [row["hash"] for row in read_table("//tmp/t_out")]
        assert len(hashes) == 2
        assert all(h == expected_md5 for h in hashes)

    @authors("yuryalekseev")
    @pytest.mark.parametrize("max_parallel_download_chunks", [1, 4])
    def test_multichunk_file_assembled_correctly(self, max_parallel_download_chunks):
        expected_md5 = self._make_multichunk_file(chunk_count=4)

        update_nodes_dynamic_config({
            "data_node": {
                "artifact_cache_reader": {
                    "max_parallel_download_chunks": max_parallel_download_chunks,
                },
            },
        })

        node = ls("//sys/cluster_nodes")[0]
        initial_cache_size = self._artifact_cache_size(node)

        self._run_hash_operation(expected_md5)

        # The artifact must have been cached.
        wait(lambda: self._artifact_cache_size(node) > initial_cache_size)

        # Make sure the parallel download path was taken.
        if max_parallel_download_chunks > 1:
            wait(self._parallel_download_logged)
