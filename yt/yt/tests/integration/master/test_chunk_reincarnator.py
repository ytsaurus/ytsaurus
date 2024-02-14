from yt_env_setup import YTEnvSetup

from yt_commands import (
    alter_table, concatenate, get, ls, copy,
    authors, get_singular_chunk_id, print_debug, wait,
    create, write_table, set, read_table, exists, create_account,
    read_file, write_file, update_nodes_dynamic_config,
    get_active_primary_master_leader_address,
    get_active_primary_master_follower_address,
    is_active_primary_master_leader,
    is_active_primary_master_follower,
    switch_leader,
    make_batch_request, execute_batch, get_batch_output,
    sync_mount_table, sync_create_cells, insert_rows, lookup_rows, sync_unmount_table)

from yt.common import YtError

from yt_helpers import profiler_factory
from yt_type_helpers import make_schema

from datetime import datetime, timedelta
import pytest
import builtins

################################################################################


def sorted_requisition(requisition):
    def key(entry):
        return (
            entry["account"],
            entry["medium"],
            entry["replication_policy"]["replication_factor"],
            entry["replication_policy"]["data_parts_only"],
            entry["committed"])

    return sorted(requisition, key=key)


################################################################################


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

        return max(counter.get_delta() for counter in self.counters[cell]) - self.old_values[cell]

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
                "ignore_account_settings": True,
            }
        },
        "cell_master": {
            "mutation_time_commit_period": 150,
        }
    }

    ERASURE_CODEC = None

    def _build_requisition_entry(self, account="tmp"):
        return {
            "account": account,
            "medium": "default",
            "replication_policy": {
                "replication_factor": 3 if self.ERASURE_CODEC is None else 1,
                "data_parts_only": False,
            },
            "committed": True,
        }

    def _create_table(self, path, *, attributes=None):
        print_debug(f"Create table with erasure codec: {self.ERASURE_CODEC}")
        attributes = attributes or {}
        if self.ERASURE_CODEC and "erasure_codec" not in attributes:
            attributes["erasure_codec"] = self.ERASURE_CODEC
        create("table", path, attributes=attributes)

    def _wait_for_chunk_obsolescence(self, chunk_id):
        wait(lambda: datetime.strptime(get(f"//sys/estimated_creation_time/{chunk_id}/max"), "%Y-%m-%dT%H:%M:%S.%fZ")
             < datetime.utcnow() - timedelta(seconds=1))

    def _set_min_allowed_creation_time(self, min_allowed_creation_time):
        print_debug(f"Setting min_allowed_creation_time: {min_allowed_creation_time}")
        set("//sys/@config/chunk_manager/chunk_reincarnator/min_allowed_creation_time",
            str(min_allowed_creation_time))

    def _enable_chunk_reincarnator(self):
        set("//sys/@config/chunk_manager/chunk_reincarnator/enable", True)

    def _disable_chunk_reincarnator(self):
        set("//sys/@config/chunk_manager/chunk_reincarnator/enable", False)

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
            self._set_min_allowed_creation_time(min_allowed_creation_time)

        self._enable_chunk_reincarnator()

        def chunks_reincarnated():
            new_chunk_ids = get(f"{table}/@chunk_ids")
            if not whole_table_reincarnation:
                print_debug(f"Old chunks: {chunk_ids}")
                print_debug(f"New chunks: {new_chunk_ids}")
                return (builtins.set(chunk_ids) & builtins.set(new_chunk_ids)) != builtins.set(chunk_ids)

            if builtins.set(new_chunk_ids) & builtins.set(chunk_ids):
                return False

            old_chunk_ref_counters = execute_batch([
                make_batch_request("get", path=f"#{chunk_id}/@ref_counter")
                for chunk_id in chunk_ids])

            for old_chunk_ref_counter, chunk_id in zip(old_chunk_ref_counters, chunk_ids):
                try:
                    if get_batch_output(old_chunk_ref_counter):
                        print_debug(f"Chunk {chunk_id} is still alive")
                        return False
                except YtError:
                    pass

            return True

        if inject_leader_switch:
            self._switch_leader(get("//sys/@cell_id"))

        wait(chunks_reincarnated)
        self._disable_chunk_reincarnator()

    def _get_chunk_info(self, chunk_id):
        while True:
            attrs = get(f"#{chunk_id}/@")

            # Change of these attributes during reincarnation is OK.
            unimportant_attrs = [
                "id",
                "ref_counter",
                "ephemeral_ref_counter",
                "weak_ref_counter",
                "estimated_creation_time",
                "stored_replicas",
                "stored_master_replicas",
                "stored_sequoia_replicas",
                "last_seen_replicas",
                "replication_status",
                "scan_flags",
                "creation_time",
                "local_requisition_index",
                "disk_space",
                "meta_size",
                "master_meta_size",
                "approved_replica_count",
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

            for attr in unimportant_attrs:
                if attr in attrs:
                    attrs.pop(attr)

            return attrs

    def _wait_for_requisition(self, expected, *, chunk=None, table=None):
        assert int(chunk is None) + int(table is None) == 1
        if chunk:
            expected = sorted_requisition(expected)
            wait(lambda: sorted_requisition(get(f"#{chunk}/@requisition")) == expected, timeout=15)
        else:
            for chunk in get(f"{table}/@chunk_ids"):
                self._wait_for_requisition(expected, chunk=chunk)

    def _wait_for_external_requisition(self, chunk_id, expected):
        def sorted_external_requisition(external_requisition):
            return {
                cell_tag: sorted_requisition(requisition)
                for cell_tag, requisition in external_requisition.items()
            }

        expected = sorted_external_requisition(expected)
        wait(lambda: sorted_external_requisition(get(f"#{chunk_id}/@external_requisitions")) == expected, timeout=15)

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
        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/t")
        tables = self._save_tables("//tmp/t")
        self._wait_for_chunk_obsolescence(get_singular_chunk_id("//tmp/t"))
        self._wait_for_reincarnation("//tmp/t", datetime.utcnow())
        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/t")
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

        def wait_for_requisition():
            for i in range(1, 3):
                self._wait_for_requisition([self._build_requisition_entry()], table=f"//tmp/t{i}")

        wait_for_requisition()
        tables = self._save_tables("//tmp/t1", "//tmp/t2")

        self._wait_for_reincarnation("//tmp/t1", ts)

        new_chunk1 = get_singular_chunk_id("//tmp/t1")
        assert chunk1 != new_chunk1
        assert [new_chunk1, chunk2] == get("//tmp/t2/@chunk_ids")

        wait_for_requisition()
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

        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/t")
        tables = self._save_tables("//tmp/t")

        for chunk in get("//tmp/t/@chunk_ids"):
            self._wait_for_chunk_obsolescence(chunk)
        self._wait_for_reincarnation("//tmp/t", datetime.utcnow())

        wait(lambda: statistic.get_delta() == 4)
        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/t")
        self._check_tables(tables)

    @authors("kvk1920")
    def test_max_visited_chunk_ancestors_per_chunk(self):
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

        # chunk2 has about 10 ancestors (owning tables are accounted too).
        # chunk1 has many ancestors.
        set("//sys/@config/chunk_manager/chunk_reincarnator/max_visited_chunk_ancestors_per_chunk", 10)

        def wait_for_requisition():
            for i in range(1, 10):
                self._wait_for_requisition([self._build_requisition_entry()], table=f"//tmp/t{i}")

        wait_for_requisition()
        tables = self._save_tables(*[f"//tmp/t{i}" for i in range(1, 10)])

        self._wait_for_chunk_obsolescence(chunk2)
        reincarnation_time = datetime.utcnow()
        self._wait_for_reincarnation("//tmp/t9", reincarnation_time, whole_table_reincarnation=False)

        new_chunks = get("//tmp/t9/@chunk_ids")
        assert len(new_chunks) == 9

        # NB: This chunk should not be reincarnated due to large amount of ancestors.
        assert new_chunks[0] == chunk1
        wait(lambda: too_many_ancestors_counter.get_delta() >= 1)

        wait_for_requisition()
        self._check_tables(tables)
        tables = self._save_tables(*[f"//tmp/t{i}" for i in range(1, 10)])

        set("//sys/@config/chunk_manager/chunk_reincarnator/max_visited_chunk_ancestors_per_chunk", 220)
        too_many_ancestors_counter.reset()

        self._wait_for_chunk_obsolescence(chunk1)
        assert chunk1 == get("//tmp/t9/@chunk_ids")[0]
        self._wait_for_reincarnation("//tmp/t1", reincarnation_time, whole_table_reincarnation=False, interesting_chunks=[chunk1])
        too_many_ancestors_counter.reset()
        assert too_many_ancestors_counter.get_delta() == 0
        wait(lambda: reincarnation_counter.get_delta() >= 1)

        wait_for_requisition()
        self._check_tables(tables)

    @authors("kvk1920")
    @pytest.mark.parametrize("mount", [False, True])
    def test_dynamic_tables(self, mount):
        if mount:
            sync_create_cells(1)

        schema = make_schema([
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ])
        self._create_table("//tmp/static", attributes={"schema": schema})
        write_table("//tmp/static", {"key": 2, "value": "abc"})
        copy("//tmp/static", "//tmp/dynamic")
        alter_table("//tmp/dynamic", dynamic=True, schema=schema)

        if mount:
            sync_mount_table("//tmp/dynamic")
        write_table("<append=true>//tmp/static", {"key": 3, "value": "bcd"})

        dynamic_table_chunks = ReincarnatorStatistic("permanent_failures/dynamic_table_chunk")

        chunk1, chunk2 = get("//tmp/static/@chunk_ids")
        assert chunk1 == get_singular_chunk_id("//tmp/dynamic")

        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/static")
        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/dynamic")
        tables = self._save_tables("//tmp/static", "//tmp/dynamic")
        self._wait_for_chunk_obsolescence(get("//tmp/static/@chunk_ids/-1"))
        self._wait_for_reincarnation("//tmp/static", datetime.utcnow(), whole_table_reincarnation=not mount)

        if mount:
            # This chunk is reachable from mounted dynamic table.
            assert chunk1 == get("//tmp/static/@chunk_ids/0")
        else:
            assert chunk1 != get("//tmp/static/@chunk_ids/0")
        assert chunk2 != get("//tmp/static/@chunk_ids/1")

        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/static")
        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/dynamic")
        self._check_tables(tables)
        if mount:
            wait(lambda: dynamic_table_chunks.get_delta() == 1)

    @authors("kvk1920")
    def test_dynamic_table_lookup_after_reincarnation(self):
        sync_create_cells(1)

        schema = make_schema([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ])
        schema.attributes["unique_keys"] = True
        self._create_table("//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", {"key": 1, "value": "a"})
        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 2, "value": "b"}])
        sync_unmount_table("//tmp/t")
        assert read_table("//tmp/t") == [
            {"key": 1, "value": "a"},
            {"key": 2, "value": "b"}
        ]
        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/t")
        tables = self._save_tables("//tmp/t")

        old_chunks = builtins.set(get("//tmp/t/@chunk_ids"))

        self._wait_for_reincarnation("//tmp/t", datetime.utcnow())

        new_chunks = builtins.set(get("//tmp/t/@chunk_ids"))

        assert not (old_chunks & new_chunks), "Every dynamic table's chunk should be reincarnated"

        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/t")
        self._check_tables(tables)

        sync_mount_table("//tmp/t")
        content = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert content == [{"key": 1, "value": "a"}, {"key": 2, "value": "b"}]

    @authors("kvk1920")
    def test_file(self):
        create("file", "//tmp/f")
        content = b"data"
        write_file("//tmp/f", content)
        chunk = get_singular_chunk_id("//tmp/f")
        self._wait_for_chunk_obsolescence(chunk)

        non_table_chunk_counter = ReincarnatorStatistic("permanent_failures/no_table_ancestors")

        self._set_min_allowed_creation_time(datetime.utcnow())
        self._enable_chunk_reincarnator()
        wait(lambda: non_table_chunk_counter.get_delta() == 1)
        assert read_file("//tmp/f") == content

    @authors("kvk1920")
    def test_too_many_failed_jobs(self):
        update_nodes_dynamic_config({
            "data_node": {"testing_options": {"fail_reincarnation_jobs": True}}
        })
        self._create_table("//tmp/t")
        content = [{"key": 1, "value": "1"}]
        write_table("//tmp/t", content)

        too_many_failed_jobs = ReincarnatorStatistic("permanent_failures/too_many_failed_jobs")

        self._wait_for_chunk_obsolescence(get_singular_chunk_id("//tmp/t"))
        set("//sys/@config/chunk_manager/chunk_reincarnator/max_failed_jobs", 2)
        self._set_min_allowed_creation_time(datetime.utcnow())
        self._enable_chunk_reincarnator()
        wait(lambda: too_many_failed_jobs.get_delta() == 1)
        assert read_table("//tmp/t") == content


##################################################################


class TestChunkReincarnatorMultiCell(TestChunkReincarnatorSingleCell):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("kvk1920")
    @pytest.mark.parametrize("on_primary", [True, False])
    def test_foreign_chunks(self, on_primary):
        # Create table `//tmp/t2` with one foreign and one not foreign chunk.
        self._create_table(
            "//tmp/t1",
            attributes={"external_cell_tag": 11} if on_primary else {"external": False})
        write_table("//tmp/t1", {"key": "x"})
        self._create_table(
            "//tmp/t2",
            attributes={"external": False} if on_primary else {"external_cell_tag": 11})
        write_table("//tmp/t2", {"key": "y"})
        concatenate(["//tmp/t1"], "<append=true>//tmp/t2")

        old_chunks = get("//tmp/t2/@chunk_ids")

        foreign_chunk = get_singular_chunk_id("//tmp/t1")
        native_chunk = old_chunks[0]
        assert get(f"#{native_chunk}/@native_cell_tag") == 10 if on_primary else 11
        assert get(f"#{foreign_chunk}/@native_cell_tag") == 11 if on_primary else 10

        reincarnations = ReincarnatorStatistic("successful_reincarnations")
        teleported = ReincarnatorStatistic("teleported_reincarnations")

        def wait_for_requisitions(native, foreign):
            for chunk in (native, foreign):
                self._wait_for_requisition([self._build_requisition_entry()], chunk=chunk)
            self._wait_for_external_requisition(foreign, {
                "10" if on_primary else "11": [self._build_requisition_entry()]
            })

        wait_for_requisitions(native_chunk, foreign_chunk)
        tables = self._save_tables("//tmp/t1", "//tmp/t2")
        self._wait_for_reincarnation("//tmp/t2", datetime.utcnow(), inject_leader_switch=True)

        wait_for_requisitions(get("//tmp/t2/@chunk_ids")[0], get_singular_chunk_id("//tmp/t1"))
        self._check_tables(tables)
        wait(lambda: reincarnations.get_delta() == 2, timeout=10)
        wait(lambda: teleported.get_delta() == 1, timeout=10)
        # Unused chunks should be removed.
        wait(lambda: all(not exists(f"#{chunk_id}") for chunk_id in old_chunks), timeout=10)

    @authors("kvk1920")
    @pytest.mark.parametrize("on_primary", [True, False])
    def test_too_many_ancestors_on_foreign_cell(self, on_primary):
        self._create_table(
            "//tmp/native",
            attributes={"external": False} if on_primary else {"external_cell_tag": 11})
        write_table("//tmp/native", {"key": "x"})
        self._create_table(
            "//tmp/foreign1",
            attributes={"external_cell_tag": 11} if on_primary else {"external": False})
        concatenate(["//tmp/native"], "<append=%true>//tmp/foreign1")

        for i in range(2, 11):
            copy(f"//tmp/foreign{i - 1}", f"//tmp/foreign{i}")

        write_table("<append=%true>//tmp/native", {"key": "y"})

        exported_chunk, non_exported_chunk = get("//tmp/native/@chunk_ids")
        # This chunk has many ancestors on foreign cell.
        assert get(f"#{exported_chunk}/@exports")
        # This chunk hasn't more than 2 ancestors.
        assert not get(f"#{non_exported_chunk}/@exports")

        reincarnation_counter = ReincarnatorStatistic("successful_reincarnations")
        too_many_ancestors_counter = ReincarnatorStatistic("permanent_failures/too_many_ancestors")

        set("//sys/@config/chunk_manager/chunk_reincarnator/max_visited_chunk_ancestors_per_chunk", 6)

        def wait_for_requisitions(exported, non_exported):
            for chunk in (exported, non_exported):
                self._wait_for_requisition([self._build_requisition_entry()], chunk=chunk)
            self._wait_for_external_requisition(exported, {
                "11" if on_primary else "10": [self._build_requisition_entry()]
            })

        wait_for_requisitions(exported_chunk, non_exported_chunk)
        tables = self._save_tables("//tmp/native", *[f"//tmp/foreign{i}" for i in range(1, 10)])
        self._wait_for_chunk_obsolescence(exported_chunk)
        self._wait_for_chunk_obsolescence(non_exported_chunk)

        self._wait_for_reincarnation(
            "//tmp/native",
            datetime.utcnow(),
            whole_table_reincarnation=False,
            inject_leader_switch=True)

        new_chunks = get("//tmp/native/@chunk_ids")
        assert new_chunks[0] == exported_chunk
        assert new_chunks[1] != non_exported_chunk
        wait(lambda: too_many_ancestors_counter.get_delta() >= 1)
        wait(lambda: reincarnation_counter.get_delta() >= 1)
        wait_for_requisitions(*new_chunks)
        self._check_tables(tables)

    @authors("kvk1920")
    @pytest.mark.parametrize("exported", [False, True])
    def test_schedule_chunk_reincarnation(self, exported):
        self._set_min_allowed_creation_time(datetime.utcnow())

        self._create_table("//tmp/native", attributes={"external": False})
        write_table("//tmp/native", {"key": 1, "value": "a"})
        self._create_table("//tmp/foreign", attributes={"external_cell_tag": 11})
        concatenate(["//tmp/native"], "//tmp/foreign")
        write_table("<append=%true>//tmp/native", {"key": 2, "value": "b"})

        def get_target_chunk():
            exported_chunk, non_exported_chunk = get("//tmp/native/@chunk_ids")
            return exported_chunk if exported else non_exported_chunk

        def wait_for_requisition():
            exported_chunk, non_exported_chunk = get("//tmp/native/@chunk_ids")
            self._wait_for_requisition([self._build_requisition_entry()], chunk=exported_chunk)
            self._wait_for_requisition([self._build_requisition_entry()], chunk=non_exported_chunk)
            external_requisition = {}
            for cell_tag in get(f"#{exported_chunk}/@exports"):
                external_requisition[cell_tag] = [self._build_requisition_entry()]
            if external_requisition:
                self._wait_for_external_requisition(exported_chunk, external_requisition)

        wait_for_requisition()
        tables = self._save_tables("//tmp/native", "//tmp/foreign")

        self._enable_chunk_reincarnator()

        set(f"#{get_target_chunk()}/@schedule_reincarnation", {
            "ignore_account_settings": True,
            "ignore_creation_time": True,
        })

        target_chunk = get_target_chunk()
        wait(lambda: not exists(f"#{target_chunk}"))

        wait_for_requisition()
        self._check_tables(tables)

    @authors("kvk1920")
    def test_schedule_table_reincarnation(self):
        self._set_min_allowed_creation_time(datetime.utcnow())
        # Manually scheduled reincarnations can ignore account settings.
        set("//sys/@config/chunk_manager/chunk_reincarnator/ignore_account_settings", False)

        create_account("a")
        set("//sys/accounts/a/@enable_chunk_reincarnation", True)
        create_account("b")
        set("//sys/accounts/b/@enable_chunk_reincarnation", False)

        self._create_table("//tmp/native", attributes={"external": False, "account": "a"})
        write_table("//tmp/native", {"key": 1, "value": "a"})
        self._create_table("//tmp/foreign", attributes={"external_cell_tag": 11, "account": "b"})
        concatenate(["//tmp/native"], "//tmp/foreign")
        write_table("<append=%true>//tmp/native", {"key": 2, "value": "b"})

        chunks = get("//tmp/native/@chunk_ids")

        def wait_for_requisition():
            exported, non_exported = get("//tmp/native/@chunk_ids")
            self._wait_for_requisition([
                self._build_requisition_entry("a"),
                self._build_requisition_entry("b"),
            ], chunk=exported)
            self._wait_for_requisition([self._build_requisition_entry("a")], chunk=non_exported)
            self._wait_for_external_requisition(exported, {
                "11": [self._build_requisition_entry("b")]
            })

        wait_for_requisition()
        tables = self._save_tables("//tmp/native", "//tmp/foreign")

        self._enable_chunk_reincarnator()

        set("//tmp/native/@schedule_reincarnation", {
            "ignore_account_settings": True,
            "ignore_creation_time": True,
        })

        for chunk in chunks:
            wait(lambda: not exists(f"#{chunk}"))

        wait_for_requisition()
        self._check_tables(tables)

    @authors("kvk1920")
    @pytest.mark.parametrize("native_is,deny_on", [
        ("primary", "native"),
        ("primary", "foreign"),
        ("secondary", "foreign")])
    def test_account_settings(self, native_is, deny_on):
        create_account("allow")
        create_account("deny")
        set("//sys/@config/chunk_manager/chunk_reincarnator/ignore_account_settings", False)
        set("//sys/accounts/allow/@enable_chunk_reincarnation", True)
        assert get("//sys/accounts/allow/@enable_chunk_reincarnation")

        native_cell = {"external": False} if native_is == "primary" else {"external_cell_tag": 11}
        foreign_cell = {"external": False} if native_is == "secondary" else {"external_cell_tag": 11}

        cell_a = native_cell
        cell_b = native_cell if deny_on == "native" else foreign_cell

        self._create_table("//tmp/a", attributes={"account": "allow"} | cell_a)
        write_table("//tmp/a", {"key": "a", "value": 1})
        self._create_table("//tmp/b", attributes={"account": "deny"} | cell_b)
        concatenate(["//tmp/a"], "//tmp/b")
        write_table("<append=%true>//tmp/a", {"key": "b", "value": 42})

        def wait_for_requisition():
            shared_chunk, unique_chunk = get("//tmp/a/@chunk_ids")
            self._wait_for_requisition(
                [self._build_requisition_entry("allow"), self._build_requisition_entry("deny")],
                chunk=shared_chunk)
            self._wait_for_requisition([self._build_requisition_entry("allow")], chunk=unique_chunk)
            if deny_on == "foreign":
                self._wait_for_external_requisition(shared_chunk, {
                    "11" if native_is == "primary" else "10": [self._build_requisition_entry("deny")]
                })

        wait_for_requisition()
        tables = self._save_tables("//tmp/a", "//tmp/b")

        chunk1, chunk2 = get("//tmp/a/@chunk_ids")
        self._wait_for_chunk_obsolescence(chunk1)
        self._wait_for_chunk_obsolescence(chunk2)

        self._wait_for_reincarnation(
            "//tmp/a",
            min_allowed_creation_time=datetime.utcnow(),
            whole_table_reincarnation=False,
            interesting_chunks=[chunk2])

        new_chunk1, new_chunk2 = get("//tmp/a/@chunk_ids")
        assert new_chunk1 == chunk1
        assert new_chunk2 != chunk2

        wait_for_requisition()
        self._check_tables(tables)

        set("//sys/@config/chunk_manager/chunk_reincarnator/ignore_account_settings", True)

        wait_for_requisition()
        tables = self._save_tables("//tmp/a", "//tmp/b")

        self._wait_for_chunk_obsolescence(new_chunk1)
        self._wait_for_chunk_obsolescence(new_chunk2)

        self._wait_for_reincarnation("//tmp/a", min_allowed_creation_time=datetime.utcnow())

        wait_for_requisition()
        self._check_tables(tables)

    @authors("kvk1920")
    def test_chunk_exported_to_many_cells(self):
        self._create_table("//tmp/native", attributes={"external": False})
        write_table("//tmp/native", {"key": "x"})
        self._create_table("//tmp/foreign1", attributes={"external_cell_tag": 11})
        self._create_table("//tmp/foreign2", attributes={"external_cell_tag": 12})
        concatenate(["//tmp/native"], "//tmp/foreign1")
        concatenate(["//tmp/native"], "//tmp/foreign2")

        reincarnations = ReincarnatorStatistic("successful_reincarnations")
        teleportations = ReincarnatorStatistic("teleported_reincarnations")

        old_chunks = get("//tmp/native/@chunk_ids")

        def wait_for_requisitions():
            for chunk in get("//tmp/native/@chunk_ids"):
                self._wait_for_requisition([self._build_requisition_entry()], chunk=chunk)
                self._wait_for_external_requisition(chunk, {
                    "11": [self._build_requisition_entry()],
                    "12": [self._build_requisition_entry()],
                })

        wait_for_requisitions()
        tables = self._save_tables("//tmp/native", "//tmp/foreign1", "//tmp/foreign2")
        self._wait_for_reincarnation("//tmp/native", datetime.utcnow(), inject_leader_switch=True)

        wait_for_requisitions()
        self._check_tables(tables)
        wait(lambda: reincarnations.get_delta() == 1, timeout=10)
        wait(lambda: teleportations.get_delta() == 2, timeout=10)
        # Unused chunks should be removed.
        for chunk in old_chunks:
            wait(lambda: not exists(f"#{chunk}"), timeout=10)

    @authors("kvk1920")
    def test_chunk_reincarnation_check_failed_on_foreign_cell(self):
        schema = make_schema([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ])
        schema.attributes["unique_keys"] = True

        self._create_table("//tmp/native", attributes={"external": False, "schema": schema})
        write_table("//tmp/native", {"key": 1, "value": "hello"})

        self._create_table("//tmp/foreign_static", attributes={"external_cell_tag": 11})
        concatenate(["//tmp/native"], "//tmp/foreign_static")

        self._create_table(
            "//tmp/foreign_dynamic",
            attributes={"schema": schema, "external_cell_tag": 11})

        concatenate(["//tmp/native"], "//tmp/foreign_dynamic")

        write_table("<append=true>//tmp/native", {"key": 2, "value": "world"})

        alter_table("//tmp/foreign_dynamic", dynamic=True)

        reincarnations = ReincarnatorStatistic("successful_reincarnations")
        teleportations = ReincarnatorStatistic("teleported_reincarnations")
        dynamic_tables = ReincarnatorStatistic("permanent_failures/dynamic_table_chunk")

        exported_dynamic_chunk, static_chunk = get("//tmp/native/@chunk_ids")

        def wait_for_requisition():
            exported, non_exported = get("//tmp/native/@chunk_ids")
            for chunk in (exported, non_exported):
                self._wait_for_requisition([self._build_requisition_entry()], chunk=chunk)
            self._wait_for_external_requisition(exported, {
                "11": [self._build_requisition_entry()],
            })

        wait_for_requisition()
        tables = self._save_tables("//tmp/native", "//tmp/foreign_static", "//tmp/foreign_dynamic")

        sync_create_cells(1)
        sync_mount_table("//tmp/foreign_dynamic")

        self._wait_for_chunk_obsolescence(static_chunk)

        self._wait_for_reincarnation(
            "//tmp/native",
            datetime.utcnow(),
            whole_table_reincarnation=False)

        wait(lambda: reincarnations.get_delta() == 1, timeout=10)
        wait(lambda: teleportations.get_delta() == 0, timeout=10)
        wait(lambda: dynamic_tables.get_delta() == 1, timeout=10)

        assert get("//tmp/native/@chunk_ids")[0] == exported_dynamic_chunk

        sync_unmount_table("//tmp/foreign_dynamic")

        self._wait_for_chunk_obsolescence(exported_dynamic_chunk)

        self._wait_for_reincarnation(
            "//tmp/native",
            datetime.utcnow(),
            whole_table_reincarnation=False,
            interesting_chunks=[exported_dynamic_chunk])

        assert get("//tmp/native/@chunk_ids")[0] != exported_dynamic_chunk

        wait(lambda: reincarnations.get_delta() == 3, timeout=10)
        wait(lambda: teleportations.get_delta() == 1, timeout=10)
        wait(lambda: dynamic_tables.get_delta() == 1, timeout=10)

        wait_for_requisition()
        self._check_tables(tables)


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

        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/t")
        tables = self._save_tables("//tmp/t")

        for chunk in get("//tmp/t/@chunk_ids"):
            self._wait_for_chunk_obsolescence(chunk)
        self._wait_for_reincarnation("//tmp/t", datetime.utcnow(),
                                     inject_leader_switch=True)

        wait(lambda: statistic.get_delta() == 4)
        self._wait_for_requisition([self._build_requisition_entry()], table="//tmp/t")
        self._check_tables(tables)
