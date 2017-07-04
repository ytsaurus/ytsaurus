from yt_env_setup import wait, YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive
from yt.wrapper.common import uuid_hash_pair
from yt.common import date_string_to_timestamp_mcs

import __builtin__
import datetime
import itertools
import pytest
import shutil

def id_to_parts(id):
    id_parts = id.split("-")
    id_hi = long(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    id_lo = long(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    return id_hi, id_lo

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
    
    def test_get_operation(self):
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
                }
            })

        def check(res1, res2):
            for key in ["authenticated_user", "brief_progress", "brief_spec", "finish_time", "operation_type", "start_time", "state", "suspended", "title", "weight"]:
                ok1 = key in res1
                ok2 = key in res2
                assert ok1 == ok2

                if ok1:
                    assert res1[key] == res2[key]

        res_get_operation = get_operation(op.id)
        res_cypress = get("//sys/operations/{0}/@".format(op.id))
        res_orchid_progress = get("//sys/scheduler/orchid/scheduler/operations/{0}/progress".format(op.id))

        check(res_get_operation, res_cypress)

        res_get_operation_progress = res_get_operation["progress"]

        for key in res_orchid_progress:
            if key != "build_time":
                assert key in res_get_operation_progress

        op.resume_jobs()
        op.track() 

        res_cypress = get("//sys/operations/{0}/@".format(op.id))

        index_columns = ["state", "authenticated_user", "operation_type"]
        value_columns = ["progress", "brief_progress", "spec", "brief_spec", "result", "filter_factors", "events"]

        by_id_row = {}
        for key in index_columns + value_columns:
            if key in res_cypress.keys():
                by_id_row[key] = res_cypress.get(key)

        id_hi, id_lo = id_to_parts(op.id)

        by_id_row["id_hi"] = yson.YsonUint64(id_hi)
        by_id_row["id_lo"] = yson.YsonUint64(id_lo)
        by_id_row["start_time"] = date_string_to_timestamp_mcs(res_cypress["start_time"])
        by_id_row["finish_time"] = date_string_to_timestamp_mcs(res_cypress["finish_time"])

        insert_rows("//sys/operations_archive/ordered_by_id", [by_id_row])

        remove("//sys/operations/{0}".format(op.id))

        res_get_operation_archive = get_operation(op.id)

        for key in res_get_operation_archive.keys():
            if key in res_cypress:
                if key == "id":
                    assert res_get_operation_archive[key] == op.id
                else:
                    assert res_get_operation_archive[key] == res_cypress[key]
            else:
                print key
                print res_get_operation_archive[key]
