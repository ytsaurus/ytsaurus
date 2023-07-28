from __future__ import print_function

from .conftest import authors
from .helpers import TEST_DIR, check_rows_equality, get_tests_sandbox, dumps_yt_config, set_config_option

from yt.wrapper.spec_builders import MapSpecBuilder

import yt.logger as logger

from yt.local import start, stop

import yt.wrapper as yt

import logging
import os
import pytest
import uuid


def create_spec_builder(binary, source_table, destination_table):
    return MapSpecBuilder() \
        .input_table_paths(source_table) \
        .output_table_paths(destination_table) \
        .begin_mapper() \
        .command(binary) \
        .end_mapper()


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestOperationsTracker(object):
    NUM_TEST_PARTITIONS = 4

    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    @authors("max42")
    def test_reduce_with_foreign_tables_and_disabled_key_guarantee(self):
        table1 = TEST_DIR + "/table1"
        table2 = TEST_DIR + "/table2"
        table = TEST_DIR + "/table"
        yt.write_table("<sorted_by=[x;y]>" + table1, [{"x": 1, "y": 1}])
        yt.write_table("<sorted_by=[x]>" + table2, [{"x": 1}])

        def func(_, rows):
            for row in rows:
                yield row

        op = yt.run_reduce(func, [table1, "<foreign=true>" + table2], table,
                           join_by=["x"], enable_key_guarantee=False)
        assert "enable_key_guarantee" in op.get_attributes()["spec"]
        check_rows_equality([{"x": 1, "y": 1}, {"x": 1}], yt.read_table(table))

    @authors("renadeen")
    def test_operations_tracker_multiple_instances(self):
        tracker = yt.OperationsTracker()

        test_name = "TrackOperationsFromMultipleClusters"
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None
        try:
            instance = start(path=dir, id=id, node_count=3, enable_debug_logging=True, fqdn="localhost", cell_tag=2)
            clientA = yt
            clientB = instance.create_client()
            clientB.config["tabular_data_format"] = yt.format.JsonFormat()

            table = TEST_DIR + "/table"
            clientA.write_table(table, [{"x": 1, "y": 1}])

            clientB.create("table", table, recursive=True, ignore_existing=True)
            clientB.write_table(table, [{"x": 1, "y": 1}])

            op1 = clientA.run_map("sleep 30; cat", table, TEST_DIR + "/out1", sync=False)
            op2 = clientB.run_map("sleep 30; cat", table, TEST_DIR + "/out2", sync=False)

            tracker.add(op1)
            tracker.add(op2)
            tracker.abort_all()

            assert op1.get_state() == "aborted"
            assert op2.get_state() == "aborted"

            op1 = clientA.run_map("sleep 2; cat", table, TEST_DIR + "/out1", sync=False)
            op2 = clientB.run_map("sleep 2; cat", table, TEST_DIR + "/out2", sync=False)
            tracker.add_by_id(op1.id)
            tracker.add_by_id(op2.id, client=clientB)
            tracker.wait_all()

            assert op1.get_state().is_finished()
            assert op2.get_state().is_finished()

            with pytest.raises(ZeroDivisionError):
                with yt.OperationsTracker() as another_tracker:
                    op1 = clientA.run_map("sleep 100; cat", table, TEST_DIR + "/out", sync=False)
                    another_tracker.add(op1)
                    op2 = clientB.run_map("sleep 100; cat", table, TEST_DIR + "/out", sync=False)
                    another_tracker.add(op2)
                    raise ZeroDivisionError

            assert op2.get_state() == "aborted"
            assert op1.get_state() == "aborted"

        finally:
            if instance is not None:
                stop(instance.id, path=dir, remove_runtime_data=True)

    @authors("ignat")
    def test_operations_tracker(self):
        tracker = yt.OperationsTracker()

        # To enable progress printing
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1, "y": 1}])

            op1 = yt.run_map("sleep 30; cat", table, TEST_DIR + "/out1", sync=False)
            op2 = yt.run_map("sleep 30; cat", table, TEST_DIR + "/out2", sync=False)

            tracker.add(op1)
            tracker.add(op2)
            tracker.abort_all()

            assert op1.get_state() == "aborted"
            assert op2.get_state() == "aborted"

            op1 = yt.run_map("sleep 2; cat", table, TEST_DIR + "/out1", sync=False)
            op2 = yt.run_map("sleep 2; cat", table, TEST_DIR + "/out2", sync=False)
            tracker.add_by_id(op1.id)
            tracker.add_by_id(op2.id)
            tracker.wait_all()

            assert op1.get_state().is_finished()
            assert op2.get_state().is_finished()

            tracker.add(yt.run_map("false", table, TEST_DIR + "/out", sync=False))
            with pytest.raises(yt.YtError):
                tracker.wait_all(check_result=True)

            assert not tracker.operations

            op = yt.run_map("cat", table, TEST_DIR + "/out", sync=False)
            tracker.add(op)
            tracker.wait_all(keep_finished=True)
            assert op.id in tracker.operations

            tracker.wait_all(keep_finished=True)
            tracker.abort_all()

            with yt.OperationsTracker() as another_tracker:
                op = yt.run_map("sleep 2; true", table, TEST_DIR + "/out", sync=False)
                another_tracker.add(op)
            assert op.get_state() == "completed"

            with pytest.raises(ZeroDivisionError):
                with yt.OperationsTracker() as another_tracker:
                    op = yt.run_map("sleep 100; cat", table, TEST_DIR + "/out", sync=False)
                    another_tracker.add(op)
                    raise ZeroDivisionError

            assert op.get_state() == "aborted"
        finally:
            logger.LOGGER.setLevel(old_level)

    @authors("levysotsky")
    def test_operations_tracker_with_envelope_transaction(self):
        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(input_table, [{"x": 1, "y": 1}])

        with set_config_option("detached", False):
            with yt.Transaction():
                with yt.OperationsTracker() as tracker:
                    op = yt.run_map("cat", input_table, output_table, sync=False)
                    tracker.add(op)

        assert list(yt.read_table(output_table)) == [{"x": 1, "y": 1}]

    @authors("ignat")
    def test_pool_tracker_multiple_instances(self):
        tracker = yt.OperationsTrackerPool(pool_size=1)

        test_name = "TrackPoolOperationsFromMultipleClusters"
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None

        # To enable progress printing
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            instance = start(path=dir, id=id, node_count=3, enable_debug_logging=True, fqdn="localhost", cell_tag=3)
            clientA = None
            clientB = instance.create_client()
            clientB.config["tabular_data_format"] = yt.format.JsonFormat()

            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1, "y": 1}])
            clientB.create("table", table, recursive=True, ignore_existing=True)
            clientB.write_table(table, [{"x": 1, "y": 1}])

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out2")

            tracker.add(spec_builder1, client=clientA)
            tracker.add(spec_builder2, client=clientB)

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out2")

            tracker.map([spec_builder1, spec_builder2], client=clientA)

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            tracker.map([spec_builder1, spec_builder2], client=clientB)

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 2; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 2; cat", table, TEST_DIR + "/out2")
            tracker.map([spec_builder1, spec_builder2], client=clientA)
            tracker.map([spec_builder1, spec_builder2], client=clientB)

            assert tracker.get_operation_count() == 4

            tracker.wait_all()

            assert tracker.get_operation_count() == 0

            tracker.map([create_spec_builder("false", table, TEST_DIR + "/out")], client=clientB)
            with pytest.raises(yt.YtError):
                tracker.wait_all(check_result=True)

            tracker.map([create_spec_builder("false", table, TEST_DIR + "/out")], client=clientA)
            with pytest.raises(yt.YtError):
                tracker.wait_all(check_result=True)

        finally:
            logger.LOGGER.setLevel(old_level)
            if instance is not None:
                stop(instance.id, path=dir, remove_runtime_data=True)

    @authors("asaitgalin")
    def test_pool_tracker(self):
        tracker = yt.OperationsTrackerPool(pool_size=1)

        # To enable progress printing
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1, "y": 1}])

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out2")

            tracker.add(spec_builder1)
            tracker.add(spec_builder2)

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out2")

            tracker.map([spec_builder1, spec_builder2])

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 2; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 2; cat", table, TEST_DIR + "/out2")
            tracker.map([spec_builder1, spec_builder2])

            assert tracker.get_operation_count() == 2

            tracker.wait_all()

            assert tracker.get_operation_count() == 0

            tracker.map([create_spec_builder("false", table, TEST_DIR + "/out")])
            with pytest.raises(yt.YtError):
                tracker.wait_all(check_result=True)

            spec_builder = create_spec_builder("cat", table, TEST_DIR + "/out")
            tracker.map([spec_builder])
            tracker.wait_all(keep_finished=True)
            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            with tracker:
                spec_builder = create_spec_builder("sleep 2; true", table, TEST_DIR + "/out")
                tracker.map([spec_builder])

                assert tracker.get_operation_count() == 1

            assert tracker.get_operation_count() == 0

        finally:
            logger.LOGGER.setLevel(old_level)

    @authors("aleexfi")
    def test_pool_tracker_transaction_multiple_instances(self):
        test_name = "TrackPoolOperationsWithTransactionFromMultipleClusters"
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None

        # To enable progress printing
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            instance = start(path=dir, id=id, node_count=3, enable_debug_logging=True, fqdn="localhost", cell_tag=3)
            clientA = None
            clientB = instance.create_client()
            clientB.config["tabular_data_format"] = yt.format.JsonFormat()

            with yt.Transaction(client=clientA) as transactionA, yt.Transaction(client=clientB) as transactionB, \
                    yt.OperationsTrackerPool(pool_size=4) as tracker:
                tableA = TEST_DIR + "/tableA"
                yt.write_table(tableA, [{"x": 1, "y": 1}])
                clientB.create("table", tableA, recursive=True, ignore_existing=True)
                clientB.write_table(tableA, [{"x": 1, "y": 1}])

                spec_builder1 = create_spec_builder("sleep 30; cat", tableA, TEST_DIR + "/out1")
                spec_builder2 = create_spec_builder("sleep 30; cat", tableA, TEST_DIR + "/out2")

                tracker.map([spec_builder1, spec_builder2], client=clientA)
                tracker.map([spec_builder1, spec_builder2], client=clientB)

                client_transactions = set([transactionA.transaction_id, transactionB.transaction_id])
                tracker._tracking_thread._check_operations()
                with tracker._tracking_thread._thread_lock:
                    assert len(tracker._tracking_thread._operations_to_track) == 4
                    op_transactions = set([
                        o.get_attributes()['user_transaction_id'] for o in tracker._tracking_thread._operations_to_track
                    ])
                assert client_transactions == op_transactions

                tracker.abort_all()

            with yt.OperationsTrackerPool(pool_size=4) as tracker:
                tableB = TEST_DIR + "/tableB"
                yt.write_table(tableB, [{"x": 1, "y": 1}])
                clientB.create("table", tableB, recursive=True, ignore_existing=True)
                clientB.write_table(tableB, [{"x": 1, "y": 1}])

                spec_builder1 = create_spec_builder("sleep 30; cat", tableB, TEST_DIR + "/out1")
                spec_builder2 = create_spec_builder("sleep 30; cat", tableB, TEST_DIR + "/out2")

                tracker.map([spec_builder1, spec_builder2], client=clientA)
                tracker.map([spec_builder1, spec_builder2], client=clientB)

                client_transactions = set([yt.get_current_transaction_id(clientA), yt.get_current_transaction_id(clientB)])
                tracker._tracking_thread._check_operations()
                with tracker._tracking_thread._thread_lock:
                    assert len(tracker._tracking_thread._operations_to_track) == 4
                    op_transactions = set([
                        o.get_attributes()['user_transaction_id'] for o in tracker._tracking_thread._operations_to_track
                    ])
                assert client_transactions == op_transactions

                tracker.abort_all()

        finally:
            logger.LOGGER.setLevel(old_level)
            if instance is not None:
                stop(instance.id, path=dir, remove_runtime_data=True)

    @authors("aleexfi")
    def test_pool_tracker_transaction_single_instances(self):
        # To enable progress printing
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            with yt.Transaction() as transaction, yt.OperationsTrackerPool(pool_size=2) as tracker:
                tableA = TEST_DIR + "/tableA"
                yt.write_table(tableA, [{"x": 1, "y": 1}])

                spec_builder1 = create_spec_builder("sleep 30; cat", tableA, TEST_DIR + "/out1")
                spec_builder2 = create_spec_builder("sleep 30; cat", tableA, TEST_DIR + "/out2")

                tracker.add(spec_builder1)
                tracker.add(spec_builder2)

                client_transactions = set([transaction.transaction_id])
                tracker._tracking_thread._check_operations()
                with tracker._tracking_thread._thread_lock:
                    assert len(tracker._tracking_thread._operations_to_track) == 2
                    op_transactions = set([
                        o.get_attributes()['user_transaction_id'] for o in tracker._tracking_thread._operations_to_track
                    ])
                assert client_transactions == op_transactions

                tracker.abort_all()

            with yt.OperationsTrackerPool(pool_size=2) as tracker:
                tableB = TEST_DIR + "/tableB"
                yt.write_table(tableB, [{"x": 1, "y": 1}])

                spec_builder1 = create_spec_builder("sleep 30; cat", tableB, TEST_DIR + "/out1")
                spec_builder2 = create_spec_builder("sleep 30; cat", tableB, TEST_DIR + "/out2")

                tracker.add(spec_builder1)
                tracker.add(spec_builder2)

                client_transactions = set([yt.get_current_transaction_id()])
                tracker._tracking_thread._check_operations()
                with tracker._tracking_thread._thread_lock:
                    assert len(tracker._tracking_thread._operations_to_track) == 2
                    op_transactions = set([
                        o.get_attributes()['user_transaction_id'] for o in tracker._tracking_thread._operations_to_track
                    ])
                assert client_transactions == op_transactions

                tracker.abort_all()

        finally:
            logger.LOGGER.setLevel(old_level)
