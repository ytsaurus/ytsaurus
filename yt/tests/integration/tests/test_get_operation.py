from yt_env_setup import YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive

from operations_archive import clean_operations

def id_to_parts(id):
    id_parts = id.split("-")
    id_hi = long(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    id_lo = long(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    return id_hi, id_lo

def get_operation_path(op_id, storage_mode):
    if storage_mode == "hash_buckets":
        return "//sys/operations/{}/{}".format("%02x" % (long(op_id.split("-")[3], 16) % 256), op_id)
    else:
        return "//sys/operations/" + op_id

class TestGetOperation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    def setup(self):
        self.sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

    def teardown(self):
        remove("//sys/operations_archive")

    @pytest.mark.parametrize("storage_mode", ["simple_hash_buckets", "hash_buckets", "compatible"])
    def test_get_operation(self, storage_mode):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            precommand="echo STDERR-OUTPUT >&2",
            command="cat",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                },
                "testing": {
                    "cypress_storage_mode": storage_mode,
                },
            })

        def check(res1, res2):
            for key in ["authenticated_user", "brief_progress", "brief_spec", "finish_time", "operation_type", "result", "start_time", "state", "suspended", "title", "weight"]:
                ok1 = key in res1
                ok2 = key in res2
                assert ok1 == ok2, "{0} is missing in one of [res1, res2]".format(key)

                if ok1:
                    assert res1[key] == res2[key]

        res_get_operation = get_operation(op.id)
        res_cypress = get(get_operation_path(op.id, storage_mode) + "/@")
        res_orchid_progress = get("//sys/scheduler/orchid/scheduler/operations/{0}/progress".format(op.id))

        check(res_get_operation, res_cypress)

        res_get_operation_progress = res_get_operation["progress"]

        for key in res_orchid_progress:
            if key != "build_time":
                assert key in res_get_operation_progress

        op.resume_jobs()
        op.track()

        res_cypress_finished = get(get_operation_path(op.id, storage_mode) + "/@")

        clean_operations(self.Env.create_native_client())

        res_get_operation_archive = get_operation(op.id)

        for key in res_get_operation_archive.keys():
            if key in res_cypress:
                if key == "id":
                    assert res_get_operation_archive[key] == op.id
                else:
                    assert res_get_operation_archive[key] == res_cypress_finished[key]
            else:
                print key
                print res_get_operation_archive[key]

    @pytest.mark.parametrize("storage_mode", ["simple_hash_buckets", "hash_buckets", "compatible"])
    def test_attributes(self, storage_mode):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            precommand="echo STDERR-OUTPUT >&2",
            command="cat",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                },
                "testing": {
                    "cypress_storage_mode": storage_mode,
                },
            })

        assert list(get_operation(op.id, attributes=["state"])) == ["state"]

        for read_from in ("cache", "follower"):
            res_get_operation = get_operation(op.id, attributes=["progress", "state"], read_from=read_from)
            res_cypress = get(get_operation_path(op.id, storage_mode) + "/@", attributes=["progress", "state"])

            assert sorted(list(res_get_operation)) == ["progress", "state"]
            assert sorted(list(res_cypress)) == ["progress", "state"]
            assert res_get_operation["state"] == res_cypress["state"]

        op.resume_jobs()
        op.track()

        clean_operations(self.Env.create_native_client())

        res_get_operation_archive = get_operation(op.id, attributes=["progress", "state"])
        assert sorted(list(res_get_operation_archive)) == ["progress", "state"]
        assert res_get_operation_archive["state"] == "completed"

        with pytest.raises(YtError):
            get_operation(op.id, attributes=["abc"])
