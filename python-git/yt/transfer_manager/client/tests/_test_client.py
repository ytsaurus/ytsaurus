from yt.transfer_manager.client import TransferManager
from yt.wrapper.client import Yt
import yt.logger as logger

import time
import logging

logger.LOGGER.setLevel(logging.INFO)

"""Test file name starts with _ because we do not want to run it in Teamcity."""

def _wait_task(task_id, client):
    while True:
        info = client.get_task_info(task_id)
        assert info["state"] not in ["failed", "aborted"]
        if info["state"] in ["completed", "skipped"]:
            break

def test_copy_between_clusters(backend_url):
    client = TransferManager(url=backend_url)

    plato_client = Yt(proxy="plato")
    table = plato_client.create_temp_table()
    plato_client.write_table(table, ["a\tb\n", "c\td\n", "e\tf\n"], format="yamr", raw=True)

    client.add_task("plato", table, "quine", "//tmp/test_table", sync=True)

    task_id = client.add_task("plato", table, "sakura", "tmp/yt/client_test_table",
                              params={"mr_user": "userdata"})
    time.sleep(0.5)
    assert task_id in [task["id"] for task in client.get_tasks()]
    _wait_task(task_id, client)

    task_id = client.add_task("sakura", "tmp/yt/client_test_table", "plato", "//tmp/test_table",
                              sync=True, poll_period=10, params={"pool": "ignat"})

    assert client.get_task_info(task_id)["state"] == "completed"
    assert client.get_task_info(task_id)["pool"] == "ignat"
    assert plato_client.read_table("//tmp/test_table", format="yamr", raw=True).read() == "a\tb\nc\td\ne\tf\n"

    # Abort/restart
    task_id = client.add_task("sakura", "tmp/yt/client_test_table", "plato", "//tmp/test_table")
    client.abort_task(task_id)
    time.sleep(0.5)
    assert client.get_task_info(task_id)["state"] == "aborted"
    client.restart_task(task_id)
    assert client.get_task_info(task_id)["state"] != "aborted"
    _wait_task(task_id, client)

def test_copy_no_retries(backend_url):
    client = TransferManager(url=backend_url, enable_retries=False)

    plato_client = Yt(proxy="plato")
    table = plato_client.create_temp_table()
    client.add_task("plato", table, "quine", "//tmp/test_table", sync=True)

def test_copy_dir(backend_url):
    client = TransferManager(url=backend_url, enable_retries=False)

    plato_client = Yt(proxy="plato")
    plato_client.mkdir("//tmp/test_dir_tm", recursive=True)
    for i in xrange(10):
        plato_client.create("table", "//tmp/test_dir_tm/" + str(i), ignore_existing=True)

    quine_client = Yt(proxy="quine")
    quine_client.remove("//tmp/test_dir_tm", recursive=True, force=True)

    client.add_tasks("plato", "//tmp/test_dir_tm", "quine", "//tmp/test_dir_tm", sync=True, running_tasks_limit=2)

    assert 10 == len(quine_client.list("//tmp/test_dir_tm"))
