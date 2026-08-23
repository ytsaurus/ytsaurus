import os

from yt.yt.tests.integration.stress.dyn_read_write.lib.runner import MultiRunner
from yt.yt.tests.integration.stress.dyn_read_write.lib.common import create_client, make_client_factory
from yt.yt.tests.integration.stress.dyn_read_write.lib.inserter import Inserter
from yt.yt.tests.integration.stress.dyn_read_write.lib.selecter import Selecter
from yt.yt.tests.integration.stress.dyn_read_write.lib.smooth_move import SmoothMovementRunner, SmoothMovementHelper
from yt.yt.tests.integration.stress.dyn_read_write.lib.create import create_sorted_table
from yt.yt.tests.integration.stress.dyn_read_write.lib.setup import setup_clusters
from yt.yt.tests.integration.stress.dyn_read_write.lib.log import update_file_handler
from yt.yt.tests.integration.stress.dyn_read_write.lib.disturbance import Disturber, freeze_unfreeze, unmount_mount, build_cell_snapshot, restart_cell

import yatest.common


def init_globals(proxy):
    # Initialize abc stuff in the main thread.
    try:
        SmoothMovementHelper("garbage")
    except Exception:
        pass

    # Initialize rpc driver in the main thread.
    create_client("rpc", proxy=proxy).list("/")


class TestStressReadWriteToDynamicTables():
    def test_simple(self):
        proxy = os.getenv("YT_PROXY_FIRST")
        client = create_client(proxy=proxy)

        init_globals(proxy)

        setup_clusters("default", proxy, None, 3, 0)

        update_file_handler(yatest.common.test_output_path("app.log"))

        tables = [
            "//tmp/t1",
            "//tmp/t2",
        ]

        schema_attributes = {
            "with_hash": True,
            "with_hunks": False,
        }
        reshard_args = {
            "tablet_count": 10,
            "uniform": True,
        }

        create_table = create_sorted_table
        for table in tables:
            create_table(
                table, schema_attributes=schema_attributes,
                ignore_existing=True, mount=True, reshard_args=reshard_args,
                force=True, client=client)

        runner = MultiRunner()

        for i, table in enumerate(tables):
            inserter = Inserter(table, period=0, client_factory=make_client_factory(backend="http", proxy=proxy))
            inserter.initialize()

            # 5 rows per insertion.
            runner.start_thread(inserter, 5, name=f"Ins{i}")

            # Select from the main table.
            for j in range(3):
                runner.start_thread(
                    Selecter(inserter, period=0, client_factory=make_client_factory(backend="rpc", proxy=proxy)),
                    name=f"Sel{i}/{j}")

        SMOOTH_MOVEMENT_PERIOD = 2

        for j in range(8):
            runner.start_thread(SmoothMovementRunner(
                tables, period=SMOOTH_MOVEMENT_PERIOD, client=client),
                name=f"Mov{i}")

        disturbances = []

        disturbances.append(build_cell_snapshot(proxy=proxy, period=40))
        disturbances.append(restart_cell(proxy=proxy, period=60))

        for path in tables:
            disturbances.append(unmount_mount(path, period=150, proxy=proxy))
            disturbances.append(freeze_unfreeze(path, period=200, proxy=proxy))

        for i in range(2):
            runner.start_thread(Disturber(disturbances), name=f"Dis{i}")

        assert runner.wait(300)
