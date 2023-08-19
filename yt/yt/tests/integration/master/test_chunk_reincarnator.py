from yt_env_setup import YTEnvSetup

from yt_commands import (
    alter_table, concatenate, get, ls, copy, remove,
    authors, get_singular_chunk_id, print_debug, wait,
    create, write_table, set, read_table,
    read_file, write_file, update_nodes_dynamic_config,
    get_active_primary_master_leader_address,
    get_active_primary_master_follower_address,
    is_active_primary_master_leader,
    is_active_primary_master_follower,
    switch_leader,
    make_batch_request, execute_batch, get_batch_output)

from yt.common import YtError

from yt_helpers import profiler_factory
from yt_type_helpers import make_schema

from datetime import datetime, timedelta
import pytest
import builtins


class ReincarnatorStatistic:
    def __init__(self, counter):
        factory = profiler_factory()
        path = f"chunk_server/chunk_reincarnator/{counter}"
        self.counters = {"primary": []}
        for address in ls("//sys/primary_masters"):
            self.counters["primary"].append(factory.at_primary_master(address).counter(path))
        for cell_tag in ls("//sys/secondary_masters"):
            self.counters[cell_tag] = []
            for address in ls(f"//sys/secondary_masters/{cell_tag}"):
                self.counters[cell_tag].append(factory.at_secondary_master(cell_tag, address).counter(path))
        self.old_values = {cell: 0 for cell in self.counters}
        self.reset()

    def get_delta(self, cell=None):
        if cell is None:
            return sum(map(self.get_delta, self.counters))

        return sum(counter.get_delta() for counter in self.counters[cell]) - self.old_values[cell]

    def reset(self, cell=None):
        if cell is None:
            for cell in self.counters:
                self.reset(cell)
            return

        self.old_values[cell] += self.get_delta(cell)


##################################################################


class TestChunkReincarnatorBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True,
        }
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "chunk_reincarnator": {
                "chunk_scan_period": 600,
            }
        },
        "cell_master": {
            "mutation_time_commit_period": 150,
        }
    }

    ERASURE_CODEC = None

    def _create_table(self, path, *, attributes=None):
        print_debug("create table with erasure codec:", self.ERASURE_CODEC)
        attributes = attributes or {}
        if self.ERASURE_CODEC and "erasure_codec" not in attributes:
            attributes["erasure_codec"] = self.ERASURE_CODEC
        create("table", path, attributes=attributes)

    def _wait_for_chunk_obsolescence(self, chunk_id):
        wait(lambda: datetime.strptime(get(f"//sys/estimated_creation_time/{chunk_id}/max"), "%Y-%m-%dT%H:%M:%S.%fZ")
             < datetime.utcnow() - timedelta(seconds=1))

    def _wait_for_reincarnation(
            self,
            table,
            min_allowed_creation_time=None,
            whole_table_reincarnation=True,
            interesting_chunks=None,
            inject_leader_switch=False):
        chunk_ids = get(f"{table}/@chunk_ids")
        if interesting_chunks:
            chunk_ids = list(filter(lambda chunk: chunk in interesting_chunks, chunk_ids))

        if min_allowed_creation_time is not None:
            print_debug("setting min_allowed_creation_time:", min_allowed_creation_time)
            set("//sys/@config/chunk_manager/chunk_reincarnator/min_allowed_creation_time",
                str(min_allowed_creation_time))

        set("//sys/@config/chunk_manager/chunk_reincarnator/enable", True)

        def chunks_reincarnated():
            new_chunk_ids = get(f"{table}/@chunk_ids")
            if not whole_table_reincarnation:
                print_debug(f"old_chunks: {chunk_ids}")
                print_debug(f"new_chunks: {new_chunk_ids}")
                return (builtins.set(chunk_ids) & builtins.set(new_chunk_ids)) != builtins.set(chunk_ids)

            if builtins.set(new_chunk_ids) & builtins.set(chunk_ids):
                return False

            old_chunk_ref_counters = execute_batch([
                make_batch_request("get", path=f"#{chunk_id}/@ref_counter")
                for chunk_id in chunk_ids])

            for old_chunk_ref_counter, chunk_id in zip(old_chunk_ref_counters, chunk_ids):
                try:
                    if get_batch_output(old_chunk_ref_counter) > 0:
                        print_debug(f"Chunk {chunk_id} is still alive")
                        return False
                except YtError:
                    pass

            return True

        if inject_leader_switch:
            self._switch_leader(get("//sys/@cell_id"))

        wait(chunks_reincarnated)
        set("//sys/@config/chunk_manager/chunk_reincarnator/enable", False)

    def _get_chunk_info(self, chunk_id):
        attrs = get(f"#{chunk_id}/@")

        transient_attrs = [
            "id",
            "ref_counter",
            "ephemeral_ref_counter",
            "weak_ref_counter",
            "estimated_creation_time",
            "stored_replicas",
            "last_seen_replicas",
            "replication_status",
            "min_timestamp",
            "max_timestamp",
            "scan_flags",
            "creation_time",
            "local_requisition_index",
            "creation_time",
            "disk_space",
            "meta_size",
            "master_meta_size",
            "approved_replica_count",
            "external_requisitions",
            "external_requisition_indexes",
            "staging_account",
            "staging_transaction_id",
            "shard_index",
            "chunk_replicator_address",
        ]

        # COMPAT(h0pless): Remove this when reincarnator will learn how to set chunk schemas
        if "schema" in attrs:
            attrs.pop("schema")
        if "schema_id" in attrs:
            attrs.pop("schema_id")

        for attr in transient_attrs:
            if attr in attrs:
                attrs.pop(attr)

        for requisition in attrs["requisition"]:
            requisition.pop("committed")
        for requisition in attrs["local_requisition"]:
            requisition.pop("committed")

        return attrs

    def _save_tables(self, *tables):
        result = {}
        for table in tables:
            result[table] = {
                "content": read_table(table),
                "chunks": [
                    self._get_chunk_info(chunk)
                    for chunk in get(f"{table}/@chunk_ids")
                ],
            }
        return result

    def _check_tables(self, tables):
        for table, info in tables.items():
            assert info["content"] == read_table(table)
            for chunk_id, chunk_info in zip(get(f"{table}/@chunk_ids"), info["chunks"]):
                assert self._get_chunk_info(chunk_id) == chunk_info

    def _switch_leader(self, cell_id):
        old_leader = get_active_primary_master_leader_address(self)
        new_leader = get_active_primary_master_follower_address(self)
        switch_leader(cell_id, new_leader)
        wait(lambda: is_active_primary_master_follower(old_leader))
        wait(lambda: is_active_primary_master_leader(new_leader))

    def setup_method(self, method):
        super(TestChunkReincarnatorBase, self).setup_method(method)
        set("//sys/@config/chunk_manager/chunk_reincarnator/enable", False)

    def teardown_method(self, method):
        set("//sys/@config/chunk_manager/chunk_reincarnator/enable", False)
        super(TestChunkReincarnatorBase, self).teardown_method(method)


##################################################################


class TestChunkReincarnatorSingleCell(TestChunkReincarnatorBase):
    @authors("kvk1920")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_single_chunk(self, optimize_for):
        self._create_table("//tmp/t", attributes={"optimize_for": optimize_for})
        write_table("//tmp/t", [
            {"a": "b"},
            {"a": "c"},
            {"a": "d"},
        ], table_writer={"upload_replication_factor": 1})

        statistics = ReincarnatorStatistic("successful_reincarnations")
        tables = self._save_tables("//tmp/t")
        self._wait_for_chunk_obsolescence(get_singular_chunk_id("//tmp/t"))
        self._wait_for_reincarnation("//tmp/t", datetime.utcnow())
        self._check_tables(tables)
        wait(lambda: statistics.get_delta() == 1)

    @authors("kvk1920")
    def test_shared_chunk(self):
        self._create_table("//tmp/t1")
        content = [
            {"a": "b"},
            {"a": "c"},
            {"a": "d"},
        ]
        write_table("//tmp/t1", content)
        statistic = ReincarnatorStatistic("successful_reincarnations")
        chunk1 = get_singular_chunk_id("//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")

        self._wait_for_chunk_obsolescence(chunk1)
        ts = datetime.utcnow()

        write_table("<append=true>//tmp/t2", content)

        t2_chunks = get("//tmp/t2/@chunk_ids")
        assert len(t2_chunks) == 2 and chunk1 == t2_chunks[0]
        chunk2 = t2_chunks[1]

        tables = self._save_tables("//tmp/t1", "//tmp/t2")

        self._wait_for_reincarnation("//tmp/t1", ts)

        new_chunk1 = get_singular_chunk_id("//tmp/t1")
        assert chunk1 != new_chunk1
        assert [new_chunk1, chunk2] == get("//tmp/t2/@chunk_ids")
        self._check_tables(tables)
        wait(lambda: statistic.get_delta() == 1)

    @authors("kvk1920")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_reincarnation_many_chunks(self, optimize_for):
        self._create_table("//tmp/t", attributes={"optimize_for": optimize_for})
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})
        write_table("<append=true>//tmp/t", {"a": "e"})

        statistic = ReincarnatorStatistic("successful_reincarnations")

        tables = self._save_tables("//tmp/t")

        for chunk in get("//tmp/t/@chunk_ids"):
            self._wait_for_chunk_obsolescence(chunk)
        self._wait_for_reincarnation("//tmp/t", datetime.utcnow())

        wait(lambda: statistic.get_delta() == 4)
        self._check_tables(tables)

    @authors("kvk1920")
    def test_max_visited_chunk_lists_count(self):
        self._create_table("//tmp/t1")
        write_table("//tmp/t1", {"a": "a"})

        reincarnation_counter = ReincarnatorStatistic("successful_reincarnations")
        too_many_ancestors_counter = ReincarnatorStatistic("permanent_failures/too_many_ancestors")

        for i in range(2, 10):
            copy(f"//tmp/t{i - 1}", f"//tmp/t{i}")
            write_table(f"<append=true>//tmp/t{i}", {"a": "b"})

        chunks = get("//tmp/t9/@chunk_ids")
        assert len(chunks) == 9
        chunk1, chunk2 = chunks[0], chunks[8]

        # NB: chunk2 has about 1 ancestors.
        # NB: chunk1 has many ancestors.
        set("//sys/@config/chunk_manager/chunk_reincarnator/max_visited_chunk_lists_per_scan", 2)

        tables = self._save_tables(*[f"//tmp/t{i}" for i in range(1, 10)])

        self._wait_for_chunk_obsolescence(chunk2)
        reincarnation_time = datetime.utcnow()
        self._wait_for_reincarnation("//tmp/t9", reincarnation_time, whole_table_reincarnation=False)

        new_chunks = get("//tmp/t9/@chunk_ids")
        assert len(new_chunks) == 9

        # NB: This chunk should not be reincarnated due to large amount of parents.
        assert new_chunks[0] == chunk1
        wait(lambda: too_many_ancestors_counter.get_delta() >= 1)

        self._check_tables(tables)

        set("//sys/@config/chunk_manager/chunk_reincarnator/max_visited_chunk_lists_per_scan", 220)
        too_many_ancestors_counter.reset()

        self._wait_for_chunk_obsolescence(chunk1)
        assert chunk1 == get("//tmp/t9/@chunk_ids")[0]
        self._wait_for_reincarnation("//tmp/t1", reincarnation_time, whole_table_reincarnation=False, interesting_chunks=[chunk1])
        too_many_ancestors_counter.reset()
        assert too_many_ancestors_counter.get_delta() == 0
        wait(lambda: reincarnation_counter.get_delta() >= 1)
        self._check_tables(tables)

    @authors("kvk1920")
    def test_ignore_dynamic_tables(self):
        schema = make_schema([
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ])
        self._create_table("//tmp/static", attributes={"schema": schema})
        write_table("//tmp/static", {"key": 2, "value": "abc"})
        copy("//tmp/static", "//tmp/dynamic")
        alter_table("//tmp/dynamic", dynamic=True, schema=schema)
        write_table("<append=true>//tmp/static", {"key": 3, "value": "bcd"})

        dynamic_table_chunks = ReincarnatorStatistic("permanent_failures/dynamic_table_chunk")

        chunk1, chunk2 = get("//tmp/static/@chunk_ids")
        assert chunk1 == get_singular_chunk_id("//tmp/dynamic")

        tables = self._save_tables("//tmp/static", "//tmp/dynamic")
        self._wait_for_chunk_obsolescence(get("//tmp/static/@chunk_ids/-1"))
        self._wait_for_reincarnation("//tmp/static", datetime.utcnow(), whole_table_reincarnation=False)

        # NB: This chunk is reachable from dynamic table.
        assert chunk1 == get("//tmp/static/@chunk_ids/0")
        assert chunk2 != get("//tmp/static/@chunk_ids/1")

        self._check_tables(tables)
        wait(lambda: dynamic_table_chunks.get_delta() == 1)

    @authors("kvk1920")
    def test_file(self):
        create("file", "//tmp/f")
        content = b"data"
        write_file("//tmp/f", content)
        chunk = get_singular_chunk_id("//tmp/f")
        self._wait_for_chunk_obsolescence(chunk)

        non_table_chunk_counter = ReincarnatorStatistic("permanent_failures/non_table_chunk")

        set("//sys/@config/chunk_manager/chunk_reincarnator/min_allowed_creation_time", str(datetime.utcnow()))
        set("//sys/@config/chunk_manager/chunk_reincarnator/enable", True)
        wait(lambda: non_table_chunk_counter.get_delta() == 1)
        assert read_file("//tmp/f") == content

    @authors("kvk1920")
    def test_too_many_failed_jobs(self):
        try:
            update_nodes_dynamic_config({
                "data_node": {"testing_options": {"fail_reincarnation_jobs": True}}
            })
            create("table", "//tmp/t")
            content = [{"key": 1, "value": "1"}]
            write_table("//tmp/t", content)

            too_many_failed_jobs = ReincarnatorStatistic("permanent_failures/too_many_failed_jobs")

            self._wait_for_chunk_obsolescence(get_singular_chunk_id("//tmp/t"))
            set("//sys/@config/chunk_manager/chunk_reincarnator/max_failed_jobs", 2)
            set("//sys/@config/chunk_manager/chunk_reincarnator/min_allowed_creation_time", str(datetime.utcnow()))
            set("//sys/@config/chunk_manager/chunk_reincarnator/enable", True)
            wait(lambda: too_many_failed_jobs.get_delta() == 1)
            assert read_table("//tmp/t") == content
        except Exception:
            update_nodes_dynamic_config({
                "data_node": {"testing_options": {"fail_reincarnation_jobs": False}}
            })
            remove("//sys/@config/chunk_manager/chunk_reincarnator/max_failed_jobs")
            raise


##################################################################


class TestChunkReincarnatorMultiCell(TestChunkReincarnatorSingleCell):
    NUM_SECONDARY_MASTER_CELLS = 1

    @authors("kvk1920")
    def test_ignore_foreign_chunks(self):
        # Create table //tmp/t2 with one foreign and one not foreign chunk.
        self._create_table("//tmp/t1", attributes={"external_cell_tag": 11})
        write_table("//tmp/t1", {"key": "x"})
        self._create_table("//tmp/t2", attributes={"external": False})
        write_table("//tmp/t2", {"key": "y"})
        concatenate(["//tmp/t1"], "<append=true>//tmp/t2")

        foreign_chunk = get_singular_chunk_id("//tmp/t1")
        assert get(f"#{foreign_chunk}/@native_cell_tag") == 11

        failure_due_to_teleportations = ReincarnatorStatistic("permanent_failures/teleportations")
        reincarnations = ReincarnatorStatistic("successful_reincarnations")

        tables = self._save_tables("//tmp/t1", "//tmp/t2")
        self._wait_for_chunk_obsolescence(get("//tmp/t2/@chunk_ids/0"))
        self._wait_for_reincarnation("//tmp/t2", datetime.utcnow(), whole_table_reincarnation=False)

        self._check_tables(tables)
        assert get("//tmp/t2/@chunk_ids/1") == foreign_chunk
        wait(lambda: reincarnations.get_delta() == 1)
        # Foreign chunks are just ignored.
        assert failure_due_to_teleportations.get_delta("primary") == 0
        print_debug(failure_due_to_teleportations.counters.keys)
        wait(lambda: failure_due_to_teleportations.get_delta(str(get("//tmp/t1/@external_cell_tag"))) == 1)

    @authors("kvk1920")
    def test_ignore_exported_chunks(self):
        self._create_table("//tmp/t1", attributes={"external": False})
        write_table("//tmp/t1", {"key": "x"})
        self._create_table("//tmp/t2", attributes={"external_cell_tag": 11})
        write_table("//tmp/t2", {"key": "y"})
        concatenate(["//tmp/t1"], "<append=true>//tmp/t2")
        write_table("<append=true>//tmp/t1", {"key": "z"})
        exported_chunk, not_exported_chunk = get("//tmp/t1/@chunk_ids")
        assert len(get(f"#{exported_chunk}/@exports")) == 1
        assert not get(f"#{not_exported_chunk}/@exports")

        tables = self._save_tables("//tmp/t1", "//tmp/t2")
        for chunk in (exported_chunk, not_exported_chunk):
            self._wait_for_chunk_obsolescence(chunk)
        self._wait_for_reincarnation("//tmp/t1", datetime.utcnow(), whole_table_reincarnation=False)
        self._check_tables(tables)
        assert get("//tmp/t1/@chunk_ids/0") == exported_chunk
        assert get("//tmp/t1/@chunk_ids/1") != not_exported_chunk


##################################################################


class TestChunkReincarnatorForErasureSingleCell(TestChunkReincarnatorSingleCell):
    ERASURE_CODEC = "reed_solomon_3_3"


##################################################################


class TestChunkReincarnatorForErasureMultiCell(TestChunkReincarnatorMultiCell):
    ERASURE_CODEC = "reed_solomon_3_3"


##################################################################


class TestChunkReincarnationLeaderSwitch(TestChunkReincarnatorBase):
    @authors("kvk1920")
    def test_leader_switch_consistency(self):
        self._create_table("//tmp/t")
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})
        write_table("<append=true>//tmp/t", {"a": "e"})

        statistic = ReincarnatorStatistic("successful_reincarnations")

        tables = self._save_tables("//tmp/t")

        for chunk in get("//tmp/t/@chunk_ids"):
            self._wait_for_chunk_obsolescence(chunk)
        self._wait_for_reincarnation("//tmp/t", datetime.utcnow(),
                                     inject_leader_switch=True)

        wait(lambda: statistic.get_delta() == 4)
        self._check_tables(tables)
