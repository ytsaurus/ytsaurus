from yt.local import LocalYt

from yt.packages.six.moves import xrange

import os
import sys
import subprocess
import pytest

WORKER_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../worker.py")
SANDBOX_PATH = os.path.join(os.path.dirname(__file__), "../tests.sandbox")

@pytest.mark.parametrize("mode", ["sequential", "parallel", "batch"])
def test_worker_script(mode):
    with LocalYt(path=SANDBOX_PATH, node_count=16) as client:
        tasks_and_checks = []

        task1_table = "//tmp/test_to_none_brotli_8"
        client.write_table(task1_table, [{"a": "b", "c": "d"}])
        task1 = {
            "table": task1_table,
            "erasure_codec": "none",
            "compression_codec": "brotli_8",
            "pool": "abc"
        }
        def task1_check():
            erasure_statistics = client.get(task1_table + "/@erasure_statistics")
            compression_statistics = client.get(task1_table + "/@compression_statistics")
            assert len(erasure_statistics) == 1
            assert len(compression_statistics) == 1
            assert erasure_statistics["none"]["chunk_count"] == 1
            assert compression_statistics["brotli_8"]["chunk_count"] == 1
            assert list(client.read_table(task1_table)) == [{"a": "b", "c": "d"}]
        tasks_and_checks.append((task1, task1_check))


        task2_table = "//tmp/test_to_brotli_8_and_lrc_12_2_2"
        client.write_table(task2_table, [{"a": "b", "c": "d"}])
        task2 = {
            "table": task2_table,
            "erasure_codec": "lrc_12_2_2",
            "compression_codec": "brotli_8",
            "pool": "abc"
        }
        def task2_check():
            erasure_statistics = client.get(task2_table + "/@erasure_statistics")
            compression_statistics = client.get(task2_table + "/@compression_statistics")
            assert len(erasure_statistics) == 1
            assert len(compression_statistics) == 1
            assert erasure_statistics["lrc_12_2_2"]["chunk_count"] == 1
            assert compression_statistics["brotli_8"]["chunk_count"] == 1
            assert list(client.read_table(task2_table)) == [{"a": "b", "c": "d"}]
        tasks_and_checks.append((task2, task2_check))


        task3_table = "//tmp/test_table_with_strong_schema"
        schema = [{"name": "a", "type": "string"}, {"name": "x", "type": "string"}]
        client.create("table", task3_table, attributes={"schema": schema})
        client.write_table(task3_table, [{"a": "b", "x": "y"}])
        task3 = {
            "table": task3_table,
            "compression_codec": "zlib_6",
            "erasure_codec": "none",
            "pool": "abc"
        }
        def task3_check():
            erasure_statistics = client.get(task3_table + "/@erasure_statistics")
            compression_statistics = client.get(task3_table + "/@compression_statistics")
            assert len(erasure_statistics) == 1
            assert len(compression_statistics) == 1
            assert erasure_statistics["none"]["chunk_count"] == 1
            assert compression_statistics["zlib_6"]["chunk_count"] == 1
            assert list(client.read_table(task3_table)) == [{"a": "b", "x": "y"}]
            schema = client.get(task3_table + "/@schema")
            assert schema[0]["name"] == "a"
            assert schema[0]["type"] == "string"
            assert schema[1]["name"] == "x"
            assert schema[1]["type"] == "string"
        tasks_and_checks.append((task3, task3_check))


        task4_table = "//tmp/test_sorted_table"
        client.write_table(task4_table, [{"a": 1, "b": 2}])
        client.run_sort(task4_table, sort_by=["a"])
        task4 = {
            "table": task4_table,
            "erasure_codec": "lrc_12_2_2",
            "compression_codec": "zlib_9",
            "pool": "abc"
        }
        def task4_check():
            erasure_statistics = client.get(task4_table + "/@erasure_statistics")
            compression_statistics = client.get(task4_table + "/@compression_statistics")
            assert len(erasure_statistics) == 1
            assert len(compression_statistics) == 1
            assert erasure_statistics["lrc_12_2_2"]["chunk_count"] == 1
            assert compression_statistics["zlib_9"]["chunk_count"] == 1
            assert list(client.read_table(task4_table)) == [{"a": 1, "b": 2}]
            assert client.get(task4_table + "/@sorted_by") == ["a"]
        tasks_and_checks.append((task4, task4_check))


        task5_table = "//tmp/test_transform_with_none"
        client.write_table(task5_table, [{"a": "b"}])
        task5 = {
            "table": task5_table,
            "erasure_codec": "none",
            "compression_codec": "none",
            "pool": "abc"
        }
        def task5_check():
            erasure_statistics = client.get(task5_table + "/@erasure_statistics")
            compression_statistics = client.get(task5_table + "/@compression_statistics")
            assert len(erasure_statistics) == 1
            assert len(compression_statistics) == 1
            assert erasure_statistics["none"]["chunk_count"] == 1
            assert compression_statistics["none"]["chunk_count"] == 1
            assert list(client.read_table(task5_table)) == [{"a": "b"}]
        tasks_and_checks.append((task5, task5_check))


        task6_table = "//tmp/test_empty_table"
        client.create_table(task6_table, attributes={"abcdef": "123456"})
        task6_table_modification_time = client.get(task6_table + "/@modification_time")
        task6 = {
            "table": task6_table,
            "compression_codec": "none",
            "erasure_codec": "none",
            "pool": "abc"
        }
        def task6_check():
            erasure_statistics = client.get(task6_table + "/@erasure_statistics")
            compression_statistics = client.get(task6_table + "/@compression_statistics")
            assert not erasure_statistics
            assert not compression_statistics
            assert client.get(task6_table + "/@modification_time") == task6_table_modification_time
            assert client.get(task6_table + "/@abcdef") == "123456"
        tasks_and_checks.append((task6, task6_check))


        task7_table = "//tmp/test_to_erasure_with_none_compression"
        client.write_table(task7_table, [{"a": "b"}])
        task7 = {
            "table": task7_table,
            "erasure_codec": "lrc_12_2_2",
            "compression_codec": "none",
            "pool": "abc"
        }
        def task7_check():
            erasure_statistics = client.get(task7_table + "/@erasure_statistics")
            compression_statistics = client.get(task7_table + "/@compression_statistics")
            assert len(erasure_statistics) == 1
            assert len(compression_statistics) == 1
            assert erasure_statistics["lrc_12_2_2"]["chunk_count"] == 1
            assert compression_statistics["none"]["chunk_count"] == 1
            assert list(client.read_table(task7_table)) == [{"a": "b"}]
            assert client.get(task7_table + "/@compression_codec") == "none"
            assert client.get(task7_table + "/@erasure_codec") == "lrc_12_2_2"
        tasks_and_checks.append((task7, task7_check))


        task8_table = "//tmp/test_save_user_attributes"
        client.write_table(task8_table, [{"a": "b"}])
        client.set(task8_table + "/@my_attr", "value")
        task8 = {
            "table": task8_table,
            "erasure_codec": "lrc_12_2_2",
            "compression_codec": "gzip_best_compression",
            "pool": "abc"
        }
        def task8_check():
            erasure_statistics = client.get(task8_table + "/@erasure_statistics")
            compression_statistics = client.get(task8_table + "/@compression_statistics")
            assert len(erasure_statistics) == 1
            assert len(compression_statistics) == 1
            assert erasure_statistics["lrc_12_2_2"]["chunk_count"] == 1
            assert compression_statistics["zlib_9"]["chunk_count"] == 1
            assert list(client.read_table(task8_table)) == [{"a": "b"}]
            assert client.get(task8_table + "/@my_attr") == "value"
        tasks_and_checks.append((task8, task8_check))


        def run_worker(tasks_root, wait=True):
            env = {
                "PYTHONPATH": os.environ["PYTHONPATH"],
                "YT_PROXY": client.config["proxy"]["url"]
            }

            worker = subprocess.Popen([sys.executable, WORKER_SCRIPT_PATH, "--tasks-path", tasks_root,
                                       "--id", "abc", "--process-tasks-and-exit"], env=env)
            if wait:
                assert worker.wait() == 0

            return worker

        if mode == "batch":
            client.create("list_node", "//tmp/compression_tasks")
            for task, _ in tasks_and_checks:
                client.set("//tmp/compression_tasks/end", task)

            run_worker("//tmp/compression_tasks")

            for _, check in tasks_and_checks:
                check()
        elif mode == "sequential":
            client.create("list_node", "//tmp/compression_tasks")
            for task, check in tasks_and_checks:
                client.set("//tmp/compression_tasks/end", task)
                run_worker("//tmp/compression_tasks")
                check()
        else:  # mode == "parallel"
            for i, task_and_check in enumerate(tasks_and_checks):
                client.create("list_node", "//tmp/compression_tasks_" + str(i))
                client.set("//tmp/compression_tasks_{0}/end".format(i), task_and_check[0])

            workers = []
            for i in xrange(len(tasks_and_checks)):
                workers.append(run_worker("//tmp/compression_tasks_" + str(i), wait=False))

            for worker in workers:
                assert worker.wait() == 0

            for _, check in tasks_and_checks:
                check()
