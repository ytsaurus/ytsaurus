from yt_env_setup import YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive
from yt.test_helpers import wait

from operations_archive import clean_operations

class TestGetJob(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())
        self._tmpdir = create_tmpdir("jobids")

    def teardown(self):
        remove("//sys/operations_archive")

    def test_get_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        job_id_file = os.path.join(self._tmpdir, "jobids")
        op = map(
            dont_track=True,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo $JOB_ID > {0} ; cat ; BREAKPOINT".format(job_id_file)),
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            })

        wait_breakpoint()
        wait(lambda: get(get_operation_cypress_path(op.id) + "/@brief_progress/jobs/running") == 1)

        with open(job_id_file) as inf:
            job_id = inf.read().strip()
        job_info = get_operation(op.id, job_id)

        assert job_info["type"] == "map"
