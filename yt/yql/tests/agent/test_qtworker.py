from . import test_simple
from . import test_udfs

from common import TestQueriesYqlBase, TestUpdateYqlAgentQtWorkerDynamicConfigMixin

from yt.environment.helpers import assert_items_equal

from yt_commands import (abort_transaction, authors, create, create_pool, create_user,
                         generate_uuid, get, insert_rows, remount_table,
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

    def _progress_parts(self, query):
        return list(select_rows(f"* from [//sys/query_tracker/query_progresses] where [query_id] = '{query.id}'"))

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

        parts = self._progress_parts(query)
        assert len(parts) > 0
        part_names = {row["part_name"] for row in parts}
        assert "yql_progress" in part_names

        full = query.get()["progress"]
        assert part_names <= {*full.keys()}

        filtered = query.get(progress_parts=["yql_progress", "yql_revision"])["progress"]
        assert {*filtered.keys()} == {"yql_progress", "yql_revision"}

        delta = query.get(min_progress_revision=0)["progress"]
        assert "yql_progress" in delta
        assert "yql_revision" in delta

        # Progress is always sent due to lack of revision.
        only_progress = query.get(min_progress_revision=100000)["progress"]
        assert "yql_progress" in only_progress
        assert "yql_revision" not in only_progress

        set("//sys/pools/small/@resource_limits/user_slots", 1)
        query.track()

        assert len(self._progress_parts(query)) != 0

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_progress_parts_removed_on_detach(self, query_tracker, yql_agent):
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

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_progress_ttl(self, query_tracker, yql_agent):
        set("//sys/query_tracker/query_progresses/@min_data_ttl", 1000)
        remount_table("//sys/query_tracker/query_progresses")

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

        assert len(self._progress_parts(indexed)) > 0
        assert len(self._progress_parts(not_indexed)) > 0

        time.sleep(4)  # > row TTL (two seconds)
        sync_compact_table("//sys/query_tracker/query_progresses")

        assert len(self._progress_parts(indexed)) > 0
        assert len(self._progress_parts(not_indexed)) == 0

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

        with raises_yt_error() as err:
            get_query(guid, progress_parts=["yql_plan"])
        assert err[0].contains_text("stores progress in compact form")

        with raises_yt_error() as err:
            get_query(guid, min_progress_revision=5)
        assert err[0].contains_text("stores progress in compact form")

        with raises_yt_error() as err:
            get_query(guid, progress_parts=["yql_plan"], min_progress_revision=5)
        assert err[0].contains_text("stores progress in compact form")


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
