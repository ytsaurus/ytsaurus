from . import test_simple
from . import test_udfs

from common import TestQueriesYqlBase, TestUpdateYqlAgentQtWorkerDynamicConfigMixin

from yt.environment.helpers import assert_items_equal

from yt_commands import (abort_transaction, authors, create, create_pool, create_user,
                         generate_timestamp, generate_uuid, get, insert_rows, remount_table,
                         select_rows, set, sync_compact_table, raises_yt_error, wait,
                         write_table, update_access_control_object_acl)

from yt_queries import get_query

from yt.wrapper import yson

from dirty_equals import AnyThing

import pytest
import time
import zstandard as zstd


class TestQTWorkerStart(TestQueriesYqlBase):
    YQL_QTWORKER = True

    @authors("mpereskokova")
    def test_qtworker_start(self, query_tracker, yql_agent):
        pass


class TestQTWorkerNotReady(test_simple.TestQueriesYqlSimpleBase):
    YQL_QTWORKER = True
    NUM_YQL_AGENTS = 2
    # The second agent starts without a qtworker, so it cannot execute queries at all.
    YQL_QTWORKER_SKIP_INSTANCES = [1]

    @staticmethod
    def _is_ready(address):
        return get(f"//sys/yql_agent/instances/{address}/orchid/yql_agent/ready")

    @authors("mpereskokova")
    @pytest.mark.timeout(300)
    def test_not_ready_agent_is_skipped(self, query_tracker, yql_agent):
        ready_address, not_ready_address = yql_agent.yql_agent.addresses

        assert self._is_ready(ready_address)
        assert not self._is_ready(not_ready_address)

        for _ in range(3):
            query = self.start_query("yql", "select 1 as a")
            query.track()
            assert_items_equal(query.read_result(0), [{"a": 1}])
            assert query.get()["annotations"]["assigned_engine"] == ready_address

        yql_agent.yql_agent.start_qtworker(1)
        wait(lambda: self._is_ready(not_ready_address), timeout=120)

        def is_assigned_to_recovered_agent():
            query = self.start_query("yql", "select 1 as a")
            query.track()
            assert_items_equal(query.read_result(0), [{"a": 1}])
            return query.get()["annotations"]["assigned_engine"] == not_ready_address

        wait(is_assigned_to_recovered_agent)


class TestQTWorkerProgress(test_simple.TestQueriesYqlSimpleBase):
    YQL_QTWORKER = True
    QUERY_TRACKER_DYNAMIC_CONFIG = {"not_indexed_queries_ttl": 2000}

    def _progress_parts(self, query, finished=False, **kwargs):
        table = "finished_query_progresses" if finished else "active_query_progresses"
        return list(select_rows(f"* from [//sys/query_tracker/{table}] where [query_id] = '{query.id}'", **kwargs))

    def _assert_progress_parts(self, query, finished=False):
        timestamp = generate_timestamp()
        parts = self._progress_parts(query, finished=finished, timestamp=timestamp)
        progress_parts = [row for row in parts if row["part_name"] == "yql_progress"]
        main_parts = [row for row in parts if row["part_name"] != "yql_progress"]
        assert progress_parts
        assert main_parts

        full = query.get(timestamp=timestamp)["progress"]
        assert {*full} == {row["part_name"] for row in parts} | {"main_revision", "progress_revision"}
        assert {*full["yql_progress"]} == {row["subpart_name"] for row in progress_parts}
        assert all(isinstance(value, dict) for value in full["yql_progress"].values())
        assert full["progress_revision"] == max(row["revision"] for row in progress_parts)
        assert full["main_revision"] == max(row["revision"] for row in main_parts)

        def get_progress(**options):
            return query.get(timestamp=timestamp, progress_options=options)["progress"]

        assert get_progress() == full
        assert get_progress(progress_parts=[]) == full
        assert get_progress(after_main_revision=0, after_progress_revision=0) == full

        for selected in (["yql_progress"], ["yql_plan"], ["yql_progress", "yql_plan"]):
            expected = {name: full[name] for name in selected}
            if "yql_progress" in selected:
                expected["progress_revision"] = full["progress_revision"]
            selected_main_parts = [row for row in main_parts if row["part_name"] in selected]
            if selected_main_parts:
                expected["main_revision"] = max(row["revision"] for row in selected_main_parts)
            assert get_progress(progress_parts=selected) == expected

        assert get_progress(after_main_revision=full["main_revision"] + 1) == {
            "yql_progress": full["yql_progress"],
            "progress_revision": full["progress_revision"],
        }
        assert get_progress(after_progress_revision=full["progress_revision"] + 1) == {
            key: value for key, value in full.items() if key not in ("yql_progress", "progress_revision")
        }
        assert get_progress(
            after_main_revision=full["main_revision"] + 1,
            after_progress_revision=full["progress_revision"] + 1,
        ) == {}

        # Revision cursors are exclusive and apply independently.
        assert get_progress(
            after_main_revision=full["main_revision"],
            after_progress_revision=full["progress_revision"],
        ) == {}

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_progress_parts(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        write_table("//tmp/t", [{"a": 42}])

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})
        query = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        wait(lambda: self._exists_pending_stage_in_progress(query))

        assert not self._progress_parts(query, finished=True)
        self._assert_progress_parts(query)

        set("//sys/pools/small/@resource_limits/user_slots", 1)
        query.track()

        assert not self._progress_parts(query)
        self._assert_progress_parts(query, finished=True)

    @authors("mpereskokova")
    def test_truncation_flags_are_top_level(self, query_tracker, yql_agent):
        query = self.start_query("yql", "select 1 as result")
        query.track()
        original = query.get()["progress"]
        progress_revision = original.get("progress_revision", 0) + 1
        main_revision = original.get("main_revision", 0) + 1
        flags = [
            "yql_plan_truncated",
            "yql_statistics_truncated",
            "yql_progress_truncated",
            "yql_task_info_truncated",
            "yql_ast_truncated",
        ]
        compressed_true = zstd.ZstdCompressor().compress(yson.dumps(True))
        insert_rows("//sys/query_tracker/finished_query_progresses", [{
            "query_id": query.id,
            "part_name": flag,
            "subpart_name": "",
            "revision": progress_revision if flag == "yql_progress_truncated" else main_revision,
            "part_value": compressed_true,
        } for flag in flags])

        timestamp = generate_timestamp()

        def get_progress(**options):
            return query.get(timestamp=timestamp, progress_options=options)["progress"]

        full = get_progress()
        assert all(full[flag] for flag in flags)
        assert not any(flag in full.get("yql_progress", {}) for flag in flags)
        assert full["progress_revision"] == progress_revision
        assert full["main_revision"] == main_revision

        for flag in flags:
            revision_key = "progress_revision" if flag == "yql_progress_truncated" else "main_revision"
            revision = full[revision_key]
            assert get_progress(progress_parts=[flag]) == {flag: True, revision_key: revision}
            assert get_progress(progress_parts=[flag], **{f"after_{revision_key}": revision}) == {}

        assert get_progress(
            after_main_revision=main_revision,
            after_progress_revision=progress_revision - 1,
        ) == {"yql_progress_truncated": True, "progress_revision": progress_revision}
        assert get_progress(
            progress_parts=flags,
            after_progress_revision=progress_revision,
        ) == {flag: True for flag in flags if flag != "yql_progress_truncated"} | {"main_revision": main_revision}

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_progress_parts_preserved_on_detach(self, query_tracker, yql_agent):
        create("table", "//tmp/t_detach", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        write_table("//tmp/t_detach", [{"a": 42}])

        create_pool("small_detach", attributes={"resource_limits": {"user_slots": 0}})
        query = self.start_query(
            "yql",
            'pragma yt.StaticPool = "small_detach"; select a+1 as result from primary.`//tmp/t_detach`')
        wait(lambda: self._exists_pending_stage_in_progress(query))
        assert len(self._progress_parts(query)) > 0

        lease_transaction_id = query.get(attributes=["lease_transaction_id"])["lease_transaction_id"]
        abort_transaction(lease_transaction_id)

        with raises_yt_error("Query lease was lost; restarting query execution is unsafe"):
            query.track()
        wait(lambda: len(self._progress_parts(query)) == 0)
        self._assert_progress_parts(query, finished=True)

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_progress_ttl(self, query_tracker, yql_agent):
        set("//sys/query_tracker/finished_query_progresses/@min_data_ttl", 1000)
        remount_table("//sys/query_tracker/finished_query_progresses")

        create("table", "//tmp/t_ttl", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        write_table("//tmp/t_ttl", [{"a": 42}])

        indexed = self.start_query("yql", "select a+1 as result from primary.`//tmp/t_ttl`")
        indexed.track()

        not_indexed = self.start_query(
            "yql",
            "select a+2 as result from primary.`//tmp/t_ttl`",
            settings={"is_indexed": False})
        not_indexed.track()

        assert not self._progress_parts(indexed)
        assert not self._progress_parts(not_indexed)
        assert len(self._progress_parts(indexed, finished=True)) > 0
        assert len(self._progress_parts(not_indexed, finished=True)) > 0

        time.sleep(4)  # > row TTL (two seconds)
        sync_compact_table("//sys/query_tracker/finished_query_progresses")

        assert len(self._progress_parts(indexed, finished=True)) > 0
        assert len(self._progress_parts(not_indexed, finished=True)) == 0

    @authors("mpereskokova")
    def test_get_query_compact_form(self, query_tracker):
        guid = generate_uuid()
        progress = {"yql_plan": {"some_field": 1}}

        compressor = zstd.ZstdCompressor(level=9)
        insert_rows("//sys/query_tracker/finished_queries", [{
            "query_id": guid,
            "engine": "yql",
            "user": "root",
            "query": "run_forever",
            "start_time": 0,
            "state": "running",
            "settings": {},
            "annotations": {},
            "progress": compressor.compress(yson.dumps(progress, yson_format="text")),
        }])

        assert get_query(guid)["progress"] == progress
        assert get_query(guid, progress_options={})["progress"] == progress

        for options in (
            {"progress_parts": ["yql_plan"]},
            {"after_main_revision": 5},
            {"after_progress_revision": 5},
            {"progress_parts": ["yql_plan"], "after_main_revision": 5, "after_progress_revision": 5},
        ):
            with raises_yt_error("stores progress in compact form"):
                get_query(guid, progress_options=options)


@authors("mpereskokova")
class TestSimpleQueriesYqlWithQtWorker(test_simple.TestSimpleQueriesYql):
    YQL_QTWORKER = True


@authors("ziganshinmr")
class TestDynamicTokenResolutionWithQtWorker(test_simple.TestDynamicTokenResolution):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestStackOverflowWithQtWorker(test_simple.TestStackOverflow):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlAgentWithQtWorker(test_simple.TestYqlAgent):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlAgentDynConfigWithQtWorker(TestUpdateYqlAgentQtWorkerDynamicConfigMixin, test_simple.TestYqlAgentDynConfig):
    YQL_QTWORKER = True

    def _safe_test_query(self, query, rows):
        try:
            self._test_simple_query(query, rows)
            return True
        except Exception:
            return False

    def _dyn_config_expect_error(self, yql_agent, dyn_config, expected_error):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        self._update_dyn_config(yql_agent, dyn_config)
        with raises_yt_error(expected_error):
            self._test_simple_query("select * from primary.`//tmp/t`", rows)

        # should work after fixing
        self._update_dyn_config(yql_agent, {
            "gateways": {
                "yt": {
                    "cluster_mapping": [
                    ],
                },
            },
        })
        wait(lambda: self._safe_test_query("select * from primary.`//tmp/t`", rows))


@authors("ziganshinmr")
class TestYqlAgentInitialDynConfigWithQtWorker(TestUpdateYqlAgentQtWorkerDynamicConfigMixin, test_simple.TestYqlAgentInitialDynConfig):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestMaxYqlVersionConfigAttrWithQtWorker(test_simple.TestMaxYqlVersionConfigAttr):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestNotTableResultWithQtWorker(test_simple.TestNotTableResult):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestGetOperationLinkWithQtWorker(test_simple.TestGetOperationLink):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestMetricsWithQtWorker(test_simple.TestMetrics):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestLibsWithQtWorker(test_simple.TestLibs):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestTypesWithQtWorker(test_simple.TestTypes):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlAgentBanWithQtWorker(test_simple.TestYqlAgentBan):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestComplexQueriesYqlWithQtWorker(test_simple.TestComplexQueriesYql):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestExecutionModesYqlWithQtWorker(test_simple.TestExecutionModesYql):
    YQL_QTWORKER = True

    @authors("mpereskokova")
    def test_validate(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        for mode in ["validate", 0]:
            query = self.start_query("yql", "select * from `//tmp/t1`", settings={"execution_mode": mode})
            query.track()
            result = query.get()
            assert result["result_count"] == 0
            # Unlike the native plugin, the plan is not returned here, which is the correct behavior.
            assert "yql_plan" not in result["progress"]


@authors("mpereskokova")
class TestYqlPluginWithQtWorker(test_simple.TestYqlPlugin):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestDefaultClusterWithQtWorker(test_simple.TestDefaultCluster):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestAllYqlAgentsOverloadWithQtWorker(test_simple.TestAllYqlAgentsOverload):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestPartialYqlAgentsOverloadWithQtWorker(test_simple.TestPartialYqlAgentsOverload):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestQueriesYqlLimitedResultWithQtWorker(test_simple.TestQueriesYqlLimitedResult):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestQueriesYqlResultTruncationWithQtWorker(test_simple.TestQueriesYqlResultTruncation):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestQueriesYqlAuthWithQtWorker(test_simple.TestQueriesYqlAuth):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestQueriesYqlWithSecretsWithQtWorker(test_simple.TestQueriesYqlWithSecrets):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestQueriesYqlWithSecretProtectionWithQtWorker(test_simple.TestQueriesYqlWithSecretProtection):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlColumnOrderAggregateWithAsWithQtWorker(test_simple.TestYqlColumnOrderAggregateWithAs):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlColumnOrderIssue707WithQtWorker(test_simple.TestYqlColumnOrderIssue707):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlColumnOrderParametrizeWithQtWorker(test_simple.TestYqlColumnOrderParametrize):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlColumnOrderSelectScalarsWithQtWorker(test_simple.TestYqlColumnOrderSelectScalars):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlColumnOrderDifferentSourcesWithQtWorker(test_simple.TestYqlColumnOrderDifferentSources):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestAssignedEngineWithQtWorker(test_simple.TestAssignedEngine):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestAstReturnsWithQtWorker(test_simple.TestAstReturns):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlVersionChangesWithQtWorker(test_simple.TestYqlVersionChanges):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestAgentWithInvalidMaxYqlVersionWithQtWorker(test_simple.TestAgentWithInvalidMaxYqlVersion):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestAgentWithUndefinedMaxYqlVersionWithQtWorker(test_simple.TestAgentWithUndefinedMaxYqlVersion):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestGetQueryTrackerInfoWithMaxYqlVersionWithQtWorker(test_simple.TestGetQueryTrackerInfoWithMaxYqlVersion):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestGetQueryTrackerInfoWithoutMaxYqlVersionWithQtWorker(test_simple.TestGetQueryTrackerInfoWithoutMaxYqlVersion):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestGetQueryTrackerInfoWithInvalidMaxYqlVersionWithQtWorker(test_simple.TestGetQueryTrackerInfoWithInvalidMaxYqlVersion):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestGetQueryTrackerInfoWithVisibleYqlVersionStaticWithQtWorker(test_simple.TestGetQueryTrackerInfoWithVisibleYqlVersionStatic):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestGetQueryTrackerInfoWithVisibleYqlVersionDynamicWithQtWorker(test_simple.TestGetQueryTrackerInfoWithVisibleYqlVersionDynamic):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestGetQueryTrackerInfoWithVisibleYqlVersionBothWithQtWorker(test_simple.TestGetQueryTrackerInfoWithVisibleYqlVersionBoth):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestGetQueryTrackerInfoWithVisibleYqlVersionBothNotReleasedWithQtWorker(test_simple.TestGetQueryTrackerInfoWithVisibleYqlVersionBothNotReleased):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestDeclareWithQtWorker(test_simple.TestDeclare):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestsDDLWithQtWorker(test_simple.TestsDDL):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestCrossClusterQueriesYqlWithQtWorker(test_simple.TestCrossClusterQueriesYql):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestOperationOptionsWithQtWorker(test_simple.TestOperationOptions):
    YQL_QTWORKER = True


@authors("ziganshinmr")
class TestUdfsWithQtWorker(test_udfs.TestUdfs):
    YQL_QTWORKER = True


@authors("ziganshinmr")
class TestPythonUdfWithQtWorker(test_udfs.TestPythonUdf):
    YQL_QTWORKER = True


@authors("ziganshinmr")
class TestUdfRegistry(TestQueriesYqlBase):
    YQL_QTWORKER = True

    # Note that UDFs from yql/essentials/udfs/common/
    # are preloaded as trusted during qtworker startup,
    # so we are using SimpleUdf which is outside of this path
    YQL_UDF_REGISTRY = {
        "simple": {
            "path": "yql/essentials/udfs/test/simple/libsimple_udf.so",
            "modules": {
                "SimpleUdf": {
                    "functions": [
                        {
                            "OptionalArgCount": 0,
                            "ArgCount": 1,
                            "MinLangVer": 0,
                            "SupportsBlocks": False,
                            "Name": "SimpleUdf.Echo",
                            "RunConfigType": "[\"VoidType\"]",
                            "IsTypeAwareness": False,
                            "IsStrict": False,
                            "CallableType": "[\"CallableType\";[];[[\"DataType\";\"String\"]];[[[\"OptionalType\";[\"DataType\";\"String\"]]]]]",
                            "MaxLangVer": 0
                        }
                    ]
                }
            },
        },
    }

    @authors("ziganshinmr")
    @pytest.mark.timeout(120)
    def test_udf_registry(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [{"a": "a meow"}])
        query = self.start_query("yql", "select SimpleUdf::Echo(a) as echoed_a from primary.`//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"echoed_a": "a meow"}])

    @authors("ziganshinmr")
    @pytest.mark.timeout(120)
    def test_udf_meta(self, query_tracker, yql_agent):
        cluster = yql_agent.yql_agent.env.id

        with raises_yt_error("Query of type \"udf_meta\" must not be indexed"):
            self.start_query(
                "yql",
                "",
                settings={"query_type": "udf_meta"},
                access_control_objects=["admin"]
                # Indexed
            ).track()

        with raises_yt_error("Query of type \"udf_meta\" is expected to have only \"admin\" access control object set"):
            self.start_query(
                "yql",
                "",
                settings={"query_type": "udf_meta", "is_indexed": False},
                # No ACO
            ).track()

        with raises_yt_error("\"administer\" permission required to run \"udf_meta\" queries"):
            create_user("unprivileged")
            self.start_query(
                "yql",
                "",
                settings={"query_type": "udf_meta", "is_indexed": False},
                access_control_objects=["admin"],
                authenticated_user="unprivileged"
            ).track()

        create_user("privileged")
        update_access_control_object_acl("queries", "admin", [
            {"action": "allow", "subjects": ["privileged"], "permissions": ["administer"], "inheritance_mode": "object_and_descendants"},
        ])

        query = self.start_query(
            "yql",
            "",
            files=[{"name": "simple", "content": f"yt://{cluster}//sys/yql_agent/udfs/libsimple_udf.so", "type": "url"}],
            settings={"query_type": "udf_meta", "is_indexed": False},
            access_control_objects=["admin"],
            authenticated_user="privileged"
        )
        query.track()
        result = query.read_result(0)

        assert len(result) == 1
        udf_meta = result[0].get("result")
        assert udf_meta == [
            {
                "Imports": [
                    {
                        "CustomUdfPrefix": "",
                        "Modules": ["SimpleUdf"],
                        "FileAlias": "yt://primary//sys/yql_agent/udfs/libsimple_udf.so",
                    }
                ],
                "Udfs": AnyThing(),
            }
        ]


class TestTaskDataFlavors(test_simple.TestQueriesYqlSimpleBase):
    YQL_QTWORKER = True

    @authors("mpereskokova")
    def test_default_flavor(self, query_tracker, yql_agent):
        self._test_simple_query("select 1 as a", [{"a": 1}])
        self._test_simple_query("select 1 as a", [{"a": 1}], settings={"yql_flavor": "default"})

    @authors("mpereskokova")
    def test_unknown_flavor(self, query_tracker, yql_agent):
        with raises_yt_error("No task data builder registered for flavor 'unknown-flavor'") as err:
            self._run_simple_query("select 1", settings={"yql_flavor": "unknown-flavor"})
        assert err[0].contains_text("default")
