import random
import time

from yt.wrapper import YtError
import yt.yson as yson
from yt.wrapper.driver import make_request

from .log import logger
from .common import create_client
from .runner import ThreadWrapper


class Disturbance:
    period: int = 5
    callback: callable = None
    locks: list[str] = None

    def __init__(self, callback, period=60, locks=[]):
        self.callback = callback
        self.period = period
        self.locks = locks


def freeze_unfreeze(table, proxy=None, **kwargs):
    def callback():
        client = create_client(proxy=proxy)
        logger.debug(f"Freezing {table}")
        client.freeze_table(table, sync=True)
        time.sleep(1)
        logger.debug(f"Unfreezing {table}")
        client.unfreeze_table(table, sync=True)
        logger.debug(f"Unfroze {table}")

    return Disturbance(callback, **kwargs)


def unmount_mount(table, proxy=None, **kwargs):
    def callback():
        client = create_client(proxy=proxy)
        logger.debug(f"Unmounting {table}")
        client.unmount_table(table, sync=True)
        logger.debug(f"Unmounted {table}")
        time.sleep(1)
        logger.debug(f"Mounting {table}")
        client.mount_table(table, sync=True)
        logger.debug(f"Mounted {table}")

    return Disturbance(callback, **kwargs)


def build_cell_snapshot(proxy=None, **kwargs):
    def callback():
        client = create_client(proxy=proxy)
        cell_ids = client.list("//sys/tablet_cells")
        cell_id = random.choice(cell_ids)
        logger.debug(f"Building snapshot of cell {cell_id}")
        format = yson.to_yson_type("yson", attributes={"format": "text"})
        rsp = make_request(
            "build_snapshot",
            params={
                "cell_id": cell_id,
                "output_format": format,
            },
            client=client)
        rsp = yson.loads(rsp)
        logger.debug(f"Built snapshot of cell {cell_id}, id = {rsp['snapshot_id']}")

    return Disturbance(callback, **kwargs)


def restart_cell(proxy=None, **kwargs):
    def callback():
        client = create_client(proxy=proxy)
        cell_ids = client.list("//sys/tablet_cells")
        cell_id = random.choice(cell_ids)
        cell = client.get(f"#{cell_id}/@", attributes=["peers", "health"])
        if cell["health"] != "good":
            logger.debug(f"Will not restart cell {cell_id}, it is not good")
            return
        logger.debug(f"Restarting cell {cell_id}")
        node = cell["peers"][0]["address"]
        client.set(f"//sys/cluster_nodes/{node}/@disable_tablet_cells", True)
        while True:
            new_peers = client.get(f"#{cell_id}/@peers")
            if any(node == p.get("address", None) for p in new_peers):
                time.sleep(0.1)
                continue
            break
        client.set(f"//sys/cluster_nodes/{node}/@disable_tablet_cells", False)
        while client.get(f"#{cell_id}/@health") != "good":
            logger.debug(f"Waiting for health of cell {cell_id}")
            time.sleep(1)
        logger.debug(f"Cell {cell_id} is good, finished restarting")

    return Disturbance(callback, **kwargs)


def alter_replica(table_path, **kwargs):
    def callback():
        client = create_client()
        replica_id = client.get(f"{table_path}/@upstream_replica_id")
        new_mode = random.choice(["sync"] * 3 + ["async"])
        logger.debug(f"Altering replica of table {table_path} to mode {new_mode}")
        client.alter_table_replica(replica_id, mode=new_mode)

    # period = 10
    return Disturbance(callback, **kwargs)


class Disturber(ThreadWrapper):
    def __init__(self, disturbances: list[Disturbance]):
        self.disturbances = disturbances
        super().__init__()

    def disturb(self):
        logger.debug("Picking disturbances")
        candidates = []
        for d in self.disturbances:
            if random.random() * d.period < 1:
                candidates.append(d)
        if not candidates:
            return
        logger.debug(f"Candidate count = {len(candidates)}")
        d = random.choice(candidates)
        d.callback()

    def do_run(self):
        while not self.quit_event.is_set():
            start_time = time.time()
            try:
                self.disturb()
            except YtError as e:
                logger.debug("Disturbance failed", exc_info=True)
                logger.info(f"Disturbance failed: {repr(e)}")
            delta = time.time() - start_time
            if delta < 1:
                time.sleep(1 - delta)
