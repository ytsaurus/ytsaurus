import yt.environment.init_operation_archive as init_operation_archive
from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import json_to_yson

from operations_archive import clean_operations

import pytest
import os
import json

CYPRESS_NODES = [
    "19b5c14-c41a6620-7fa0d708-29a241d2",
    "1dee545-fe4c4006-cd95617-54f87a31",
    "bd90befa-101169a-3fc03e8-1cb90ada",
    "d7df8-7d0c30ec-582ebd65-9ad7535a",
    "b0165c58-114d5b9a-3f403e8-5a44ae00"
]


class TestListOperations(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    def setup(self):
        self.sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

    def teardown(self):
        pass

    def test_list_operations(self):
        cur_dir = os.getcwd()
        for op_id in CYPRESS_NODES:
            with open(cur_dir + "/tests/nodes/" + "cypress_" + op_id + ".json") as json_data:
                res = json.loads(json_data.read())
                attr = json_to_yson(res).attributes
                create("map_node", "//sys/operations/" + op_id, attributes=attr)
                create("map_node", "//sys/operations/" + op_id + "/jobs")

        #should fail when cypress is not available
        list_operations()

        #should fail when limit is invalid
        with pytest.raises(YtError):
            list_operations(limit=99999)

        #should fail when cursor_time is out of range (before from_time)
        with pytest.raises(YtError):
            list_operations(from_time="2015-01-01T01:00:00Z", to_time="2015-01-01T02:00:00Z", cursor_time="2015-01-01T00:00:00Z") 
        
        #should fail when cursor_time is out of range (after to_time)
        with pytest.raises(YtError):
            list_operations(from_time="2015-01-01T01:00:00Z", to_time="2015-01-01T02:00:00Z", cursor_time="2015-01-01T03:00:00Z")

        #should list operations from cypress without filters
        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L), ("failed", 1L), ("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 2L), ("map_reduce", 1L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a",
                                                          "1dee545-fe4c4006-cd95617-54f87a31",
                                                          "19b5c14-c41a6620-7fa0d708-29a241d2"]

        #should list operations from cypress with from_time & to_time filter
        res = list_operations(from_time="2016-03-02T00:00:00Z", to_time="2016-03-02T12:00:00Z")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 1L)]
        assert res["failed_jobs_count"] == 0L
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31"]

        #should list operations from cypress with cursor_time/past filter
        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", cursor_time="2016-03-02T12:00:00Z", cursor_direction="past")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L), ("failed", 1L), ("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 2L), ("map_reduce", 1L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31",
                                                          "19b5c14-c41a6620-7fa0d708-29a241d2"]

        #should list operations from cypress with cursor_time/future filter
        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", cursor_time="2016-03-02T00:00:00Z", cursor_direction="future")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L), ("failed", 1L), ("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 2L), ("map_reduce", 1L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a",
                                                          "1dee545-fe4c4006-cd95617-54f87a31"]

        #should list operations from cypress without cursor_time/past filter
        res = list_operations(cursor_direction="past", limit=2)
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("odin", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("odin", 2L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L), ("failed", 2L), ("running", 2L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 2L), ("map_reduce", 1L), ("sort", 2L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["b0165c58-114d5b9a-3f403e8-5a44ae00",
                                                          "bd90befa-101169a-3fc03e8-1cb90ada"]

        #should list operations from cypress without cursor_time/future filter
        res = list_operations(cursor_direction="future", limit=2)
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("odin", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("odin", 2L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L), ("failed", 2L), ("running", 2L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 2L), ("map_reduce", 1L), ("sort", 2L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31",
                                                          "19b5c14-c41a6620-7fa0d708-29a241d2"]

        #should list operations from cypress with type filter
        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", type="map_reduce")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L), ("failed", 1L), ("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 2L), ("map_reduce", 1L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a"]

        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-04-12T00:00:00Z", type="map")
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31",
                                                          "19b5c14-c41a6620-7fa0d708-29a241d2"]

        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-04-12T00:00:00Z", type="sort")
        assert [op["id"] for op in res["operations"]] == ["bd90befa-101169a-3fc03e8-1cb90ada"]

        #should list operations from cypress with state filter
        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", state="completed")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L), ("failed", 1L), ("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 1L)]
        assert res["failed_jobs_count"] == 0L
        assert [op["id"] for op in res["operations"]] == ["19b5c14-c41a6620-7fa0d708-29a241d2"]

        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", state="failed")
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a"]

        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-04-12T00:00:00Z", state="initializing")
        assert [op["id"] for op in res["operations"]] == []

        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-04-12T00:00:00Z", state="running")
        assert [op["id"] for op in res["operations"]] == ["bd90befa-101169a-3fc03e8-1cb90ada",
                                                          "1dee545-fe4c4006-cd95617-54f87a31"]

        #should list operations from cypress with user filter
        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", user="psushin")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 1L)]
        assert res["failed_jobs_count"] == 0L
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31"]

        #should list operations from cypress with text filter
        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", filter="MRPROC")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("failed", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map_reduce", 1L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a"]

        #should list operations from cypress with pool filter
        res = list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", pool="ignat")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("ignat", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 1L)]
        assert res["failed_jobs_count"] == 0L
        assert [op["id"] for op in res["operations"]] == ["19b5c14-c41a6620-7fa0d708-29a241d2"]

        #should list operations w.r.t. limit parameter (incomplete result)
        assert list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", limit=1)["incomplete"] == True

        #should list operations w.r.t. limit parameter (complete result)
        assert list_operations(from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", limit=3)["incomplete"] == False

        clean_operations(client=self.Env.create_native_client())

        #should fail when from_time is not determined
        with pytest.raises(YtError):
            list_operations(include_archive=True, to_time="2020-01-01T01:00:00Z")

        #should fail when to_time is not determined
        with pytest.raises(YtError):
            list_operations(include_archive=True, from_time="2000-01-01T01:00:00Z")

        #should fail when archive is not available
        list_operations(include_archive=True, from_time="2000-01-01T01:00:00Z", to_time="2020-01-01T01:00:00Z")

        #should list operations from cypress and archive without filters
        res = list_operations(include_archive=True, from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L), ("failed", 1L), ("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 2L), ("map_reduce", 1L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a",
                                                          "1dee545-fe4c4006-cd95617-54f87a31",
                                                          "19b5c14-c41a6620-7fa0d708-29a241d2"]

        #should list operations from cypress and archive with text filter
        res = list_operations(include_archive=True, from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", filter="MRPROC")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("data_quality_robot", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("failed", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map_reduce", 1L)]
        assert res["failed_jobs_count"] == 1L
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a"]

        #should list operations from cypress and archive with pool filter
        res = list_operations(include_archive=True, from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", pool="psushin")
        assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("data-quality_robot", 1L), ("ignat", 1L), ("psushin", 1L)]
        assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("psushin", 1L)]
        assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("running", 1L)]
        assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 1L)]
        assert res["failed_jobs_count"] == 0L
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31"]

        #should list operations from cypress and archive with from_time & to_time filter
        res = list_operations(include_archive=True, from_time="2016-03-02T00:00:00Z", to_time="2016-03-02T16:00:00Z")
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a",
                                                          "1dee545-fe4c4006-cd95617-54f87a31"]

        #should list operations from cypress and archive with cursor_time/past filter
        res = list_operations(include_archive=True, from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", cursor_time="2016-03-02T12:00:00Z", cursor_direction="past")
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31",
                                                          "19b5c14-c41a6620-7fa0d708-29a241d2"]

        #should list operations from cypress and archive with cursor_time/future filter
        res = list_operations(include_archive=True, from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", cursor_time="2016-03-02T00:00:00Z", cursor_direction="future")
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a",
                                                          "1dee545-fe4c4006-cd95617-54f87a31"]

        #should list operations from cypress and archive without cursor_time/past filter
        res = list_operations(include_archive=True, from_time="2000-01-01T01:00:00Z", to_time="2020-01-01T01:00:00Z", cursor_direction="past", limit=2)
        assert [op["id"] for op in res["operations"]] == ["b0165c58-114d5b9a-3f403e8-5a44ae00",
                                                          "bd90befa-101169a-3fc03e8-1cb90ada"]

        #should list operations from cypress and archive without cursor_time/future filter
        res = list_operations(include_archive=True, from_time="2000-01-01T01:00:00Z", to_time="2020-01-01T01:00:00Z", cursor_direction="future", limit=2)
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31",
                                                          "19b5c14-c41a6620-7fa0d708-29a241d2"]

        #should list operations from cypress and archive with failed jobs filter(True)
        res = list_operations(include_archive=True, from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", with_failed_jobs=True)
        assert [op["id"] for op in res["operations"]] == ["d7df8-7d0c30ec-582ebd65-9ad7535a"]

        #should list operations from cypress and archive with failed jobs filter(False)
        res = list_operations(include_archive=True, from_time="2016-02-25T00:00:00Z", to_time="2016-03-04T00:00:00Z", with_failed_jobs=False)
        assert [op["id"] for op in res["operations"]] == ["1dee545-fe4c4006-cd95617-54f87a31",
                                                          "19b5c14-c41a6620-7fa0d708-29a241d2"]

        #should merge operation from cypress and arhive and take attributes from cypress
        op_id = "19b5c14-c41a6620-7fa0d708-29a241d2"
        with open(cur_dir + "/tests/nodes/" + "cypress_" + op_id + ".json") as json_data:
                res = json.loads(json_data.read())
                attr = json_to_yson(res).attributes
                attr["brief_spec"]["title"] = "OK"
                create("map_node", "//sys/operations/" + op_id, attributes=attr)

        for read_from in ("cache", "follower"):
            res = list_operations(include_archive=True, from_time="2016-02-25T23:50:00Z", to_time="2016-02-25T23:55:00Z", read_from=read_from)
            assert sorted([(key, res["pool_counts"][key]) for key in res["pool_counts"].keys()]) == [("ignat", 1L)]
            assert sorted([(key, res["user_counts"][key]) for key in res["user_counts"].keys()]) == [("ignat", 1L)]
            assert sorted([(key, res["state_counts"][key]) for key in res["state_counts"].keys()]) == [("completed", 1L)]
            assert sorted([(key, res["type_counts"][key]) for key in res["type_counts"].keys()]) == [("map", 1L)]
            assert res["failed_jobs_count"] == 0L
            assert [op["id"] for op in res["operations"]] == ["19b5c14-c41a6620-7fa0d708-29a241d2"]
            assert res["operations"][0]["brief_spec"]["title"] == "OK"
