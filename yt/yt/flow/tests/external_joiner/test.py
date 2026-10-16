import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_AUTO_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/pipeline_auto.yson"
)
PIPELINE_MANUAL_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/pipeline_manual.yson"
)

BANK_DICT = [
    {"BankId": 1, "BankName": "Alpha"},
    {"BankId": 2, "BankName": "Beta"},
    {"BankId": 3, "BankName": "Gamma"},
]

USER_METADATA = [
    {"UserId": "user-0", "UserNickname": "alice"},
    {"UserId": "user-1", "UserNickname": "bob"},
    {"UserId": "user-2", "UserNickname": "carol"},
    {"UserId": "user-3", "UserNickname": "dave"},
    {"UserId": "user-4", "UserNickname": "eve"},
]

USER_BANK_PAIRS = [
    ("user-0", 1),
    ("user-1", 2),
    ("user-2", 3),
    ("user-3", 1),
    ("user-4", 2),
]

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.bank_dict = self.work_yt_path + "/bank_dict"
        self.user_metadata = self.work_yt_path + "/user_metadata"
        self.output_table = self.work_yt_path + "/output_table"
        self.input_data = [{"UserId": user_id, "BankId": bank_id} for user_id, bank_id in USER_BANK_PAIRS]
        bank_name_by_id = {row["BankId"]: row["BankName"] for row in BANK_DICT}
        nickname_by_user = {row["UserId"]: row["UserNickname"] for row in USER_METADATA}
        self.expected_output = sorted(
            (user_id, bank_name_by_id[bank_id], nickname_by_user[user_id])
            for user_id, bank_id in USER_BANK_PAIRS
        )

    def prepare_tables(self):
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        self.client.insert_rows(self.bank_dict, BANK_DICT)
        self.client.insert_rows(self.user_metadata, USER_METADATA)
        self.client.insert_rows(self.input_queue, self.input_data)

    def prepare_pipeline_config(self, config_path):
        pipeline_config = get_yson_config(config_path)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": True,
            }
        )

        joiners = pipeline_config["spec"]["computations"]["enricher"]["external_state_joiners"]
        joiners["/bank"]["parameters"].update({"path": f"<cluster=primary>{self.bank_dict}"})
        joiners["/user_metadata"]["parameters"].update({"path": f"<cluster=primary>{self.user_metadata}"})

        pipeline_config["spec"]["computations"]["enricher"]["sinks"]["out"]["parameters"].update(
            {
                "table_path": f"<cluster=primary>{self.output_table}",
            }
        )

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def get_output(self):
        rows = list(self.client.select_rows(f"UserId, BankName, UserNickname FROM [{self.output_table}]"))
        return sorted((row["UserId"], row["BankName"], row["UserNickname"]) for row in rows)

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("config_path", "case_id"),
        [
            pytest.param(PIPELINE_AUTO_CONFIG_PATH, "auto", id="auto_preload"),
            pytest.param(PIPELINE_MANUAL_CONFIG_PATH, "manual", id="manual_preload"),
        ],
    )
    def test_reader(self, config_path, case_id):
        self.prepare_tables()
        pipeline_config_path = self.prepare_pipeline_config(config_path)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == self.expected_output

            # Mode 3: enumerate every reader row. Managers section is empty (no managers).
            res = self.client.read_states(
                self.pipeline_path,
                computation_id="enricher",
                target="external_key_state",
                limit=100)
            assert res["external_key_states"] == []
            joined = res["joined_external_key_states"]
            bank_rows = [r for r in joined if "/bank" in r["states"]]
            user_rows = [r for r in joined if "/user_metadata" in r["states"]]
            assert {r["states"]["/bank"]["BankName"] for r in bank_rows} == {b["BankName"] for b in BANK_DICT}
            assert {r["states"]["/user_metadata"]["UserNickname"] for r in user_rows} == {
                u["UserNickname"] for u in USER_METADATA
            }

            # Mode 3 with name filter: only the /bank slot participates.
            res = self.client.read_states(
                self.pipeline_path,
                computation_id="enricher",
                target="external_key_state",
                name="/bank",
                limit=100)
            joined = res["joined_external_key_states"]
            assert all(list(r["states"].keys()) == ["/bank"] for r in joined)
            assert {r["states"]["/bank"]["BankName"] for r in joined} == {b["BankName"] for b in BANK_DICT}

            # Mode 1 against the no-override reader: lookup by UserId yields its nickname; the
            # /bank reader can't resolve a (BankId)-keyed row from {"UserId": ...} so it surfaces
            # as an error.
            res = self.client.read_states(
                self.pipeline_path,
                computation_id="enricher",
                target="external_key_state",
                key={"UserId": "user-0"})
            assert res["external_key_states"] == []
            assert len(res["joined_external_key_states"]) == 1
            row = res["joined_external_key_states"][0]
            assert row["states"] == {"/user_metadata": {"UserNickname": "alice"}}
            assert any("/bank" in err for err in res["errors"])

            # delete-states is manager-only; readers are skipped entirely.
            result = self.client.delete_states(
                self.pipeline_path,
                computation_id="enricher",
                target="external_key_state",
                commit=True)
            assert result["committed"]
            assert result["matched_states"]["external_key_states"]["total"] == 0
            assert result["matched_states"]["external_key_states"]["details"] == {}
