import os
import shlex
import shutil
import subprocess

from native_snapshot_exporter import SnapshotBuilderBase
from yt_commands import get, wait
from yt_env_setup import search_binary_path
from yt_sequoia_helpers import get_ground_driver, select_rows_from_ground
from yt_helpers import master_exit_read_only_sync

from yt.sequoia_tools import DESCRIPTORS
import yt.yson as yson

##################################################################

PROC_TIMEOUT = 30

NODE_ID_TO_PATH = "node_id_to_path"
NODE_FORKS = "node_forks"
NODE_SNAPSHOTS = "node_snapshots"
CHILD_NODE = "child_node"
CHILD_FORKS = "child_forks"
TRANSACTIONS = "transactions"
TRANSACTION_DESCENDANTS = "transaction_descendants"
DEPENDENT_TRANSACTIONS = "dependent_transactions"
TRANSACTION_REPLICAS = "transaction_replicas"
PATH_TO_NODE_ID = "path_to_node_id"
PATH_FORKS = "path_forks"
ACLS = "acls"

TABLE_NAMES = [
    NODE_ID_TO_PATH,
    NODE_FORKS,
    NODE_SNAPSHOTS,
    CHILD_NODE,
    TRANSACTIONS,
    CHILD_FORKS,
    TRANSACTION_REPLICAS,
    TRANSACTION_DESCENDANTS,
    DEPENDENT_TRANSACTIONS,
    PATH_TO_NODE_ID,
    PATH_FORKS,
    ACLS,
]

RETIRABLE_TABLES = [
    ACLS,
]

##################################################################


class SequoiaReconstructor(SnapshotBuilderBase):
    def reconstructed_table_path(self, table_name: str) -> str:
        return os.path.join(self.output_dir, f"{table_name}.yson")

    def create_reconstructor_config(self, table_names):
        self.output_dir = os.path.join(self.path_to_run, "reconstructed_sequoia")
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.mkdir(self.output_dir)

        config = {"single_cell_single_run": True}
        for table_name in table_names:
            config[f"{table_name}_output"] = {"file_name": self.reconstructed_table_path(table_name)}

        return yson.dumps(yson.json_to_yson(config)).decode("utf-8")

    # Masters enter read only during build_snapshot_and_reconstruct_sequoia.
    # We should exit read only after sequoia is checked.
    def build_snapshot_and_reconstruct_sequoia(self, table_names):
        self.build_master_snapshot(set_read_only=True)

        reconstructor_config = self.create_reconstructor_config(table_names)

        return self._reconstruct_sequoia(
            self.snapshot_path,
            self.config_path,
            reconstructor_config,
            table_names)

    def _reconstruct_sequoia(
            self,
            snapshot_path,
            config_path,
            reconstructor_config,
            table_names):
        ytserver_binary = search_binary_path("ytserver-all")

        command = [
            ytserver_binary,
            "ytserver-sequoia-reconstructor",
            "--master-config", config_path,
            "--snapshot-path", snapshot_path,
            "--reconstructor-config", reconstructor_config,
        ]

        command = shlex.join(command)
        subprocess.run(
            command,
            shell=True,
            check=True,
            timeout=PROC_TIMEOUT
        )

        tables = dict()

        for table in table_names:
            table_path = self.reconstructed_table_path(table)
            with open(table_path, "rb") as f:
                tables[table] = yson.load(f)

        return tables


def _check_all_items_in_list(list_to_check, expected_items):
    expected = expected_items.copy()
    for item in list_to_check:
        found = False
        for expected_item in expected:
            if str(item) == str(expected_item):
                expected.remove(expected_item)
                found = True
                break

        if not found:
            return False
    return True


def check_sequoia_tables_reconstruction(yt_env, table_names=TABLE_NAMES):
    reconstructor = SequoiaReconstructor(
        os.path.join(yt_env.path_to_run, "primary"),
        yt_env.bin_path)
    tables = reconstructor.build_snapshot_and_reconstruct_sequoia(table_names)

    descriptors = DESCRIPTORS.as_dict()

    def check_tables_are_same(table_name):
        schema = get(f"{descriptors[table_name].get_default_path()}/@schema", driver=get_ground_driver())
        ground_result = select_rows_from_ground(f"* from [{descriptors[table_name].get_default_path()}]")
        ground_table = []

        for row in ground_result:
            table_row = []
            for column in schema:
                if "expression" in column:
                    continue
                table_row.append(row[column["name"]])
            ground_table.append(table_row)

        if table_name not in tables or tables[table_name] is None:
            reconstructed_table = []
        else:
            reconstructed_table = [list(i) for i in tables[table_name]]

        return _check_all_items_in_list(ground_table, reconstructed_table) and _check_all_items_in_list(reconstructed_table, ground_table)

    non_retriable_tables = [table_name for table_name in table_names if table_name not in RETIRABLE_TABLES]
    for table_name in non_retriable_tables:
        assert check_tables_are_same(table_name)

    master_exit_read_only_sync()
    retriable_tables = [table_name for table_name in table_names if table_name in RETIRABLE_TABLES]

    for table_name in retriable_tables:
        wait(lambda: check_tables_are_same(table_name))
