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
        if info["state"] == "completed":
            break

def test_copy_between_clusters(backend_url):
    client = TransferManager(url=backend_url)

    smith_client = Yt(proxy="smith")
    table = smith_client.create_temp_table()
    smith_client.write_table(table, ["a\tb\n", "c\td\n", "e\tf\n"], format="yamr")

    client.add_task("smith", table, "sakura", "tmp/yt/test_table", sync=True)

    task_id = client.add_task("sakura", "tmp/yt/test_table", "redwood", "tmp/yt/test_table",
                              params={"mr_user": "userdata"})
    time.sleep(0.5)
    assert task_id in [task["id"] for task in client.get_tasks()]
    _wait_task(task_id, client)

    task_id = client.add_task("redwood", "tmp/yt/test_table", "plato", "//tmp/test_table",
                              sync=True, poll_period=10, params={"pool": "ignat"})

    assert client.get_task_info(task_id)["state"] == "completed"
    assert client.get_task_info(task_id)["pool"] == "ignat"
    assert Yt(proxy="plato").read_table("//tmp/test_table").read() == "a\tb\nc\td\ne\tf\n"

    # Abort/restart
    task_id = client.add_task("redwood", "tmp/yt/test_table", "plato", "//tmp/test_table")
    client.abort_task(task_id)
    time.sleep(0.5)
    assert client.get_task_info(task_id)["state"] == "aborted"
    client.restart_task(task_id)
    assert client.get_task_info(task_id)["state"] != "aborted"
    _wait_task(task_id, client)

