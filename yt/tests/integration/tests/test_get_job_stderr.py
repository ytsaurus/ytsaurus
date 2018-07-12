from yt_env_setup import YTEnvSetup
from yt_commands import *
import yt.environment.init_operation_archive as init_operation_archive

from operations_archive import clean_operations

def id_to_parts(id):
    id_parts = id.split("-")
    id_hi = long(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    id_lo = long(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    return id_hi, id_lo

class TestGetJobStderr(YTEnvSetup):
    NUM_MASTERS = 1 
    NUM_NODES = 3 
    NUM_SCHEDULERS = 1 
    USE_DYNAMIC_TABLES = True

    def setup(self):
        self.sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

    def teardown(self):
        remove("//sys/operations_archive")

    def test_get_job_stderr(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            dont_track=True,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo STDERR-OUTPUT >&2 ; BREAKPOINT ; cat"),
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            })

        job_id = wait_breakpoint()[0]
        res = get_job_stderr(op.id, job_id)
        assert res == "STDERR-OUTPUT\n"
        release_breakpoint()
        op.track()
        res = get_job_stderr(op.id, job_id)
        assert res == "STDERR-OUTPUT\n"

        clean_operations(self.Env.create_native_client())

        res = get_job_stderr(op.id, job_id)
        assert res == "STDERR-OUTPUT\n"
