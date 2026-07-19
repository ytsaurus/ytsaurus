import test_simple

from common import TestQueriesYqlBase

from conftest import merge_old_dynconfig_into_new_static

from yt_commands import authors, create, write_file, write_table, raises_yt_error, wait

from google.protobuf.text_format import MessageToString

import pytest


class TestQTWorkerStart(TestQueriesYqlBase):
    YQL_QTWORKER = True

    @authors("mpereskokova")
    def test_qtworker_start(self, query_tracker, yql_agent):
        pass


@authors("mpereskokova")
class TestSimpleQueriesYqlWithQtWorker(test_simple.TestSimpleQueriesYql):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestStackOverflowWithQtWorker(test_simple.TestStackOverflow):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlAgentWithQtWorker(test_simple.TestYqlAgent):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestYqlAgentDynConfigWithQtWorker(test_simple.TestYqlAgentDynConfig):
    YQL_QTWORKER = True

    def _update_dyn_config(self, yql_agent, dyn_config):
        if "gateways" in dyn_config:
            config = yql_agent.render_gateways_conf(yql_agent.yql_agent.env)
            merge_old_dynconfig_into_new_static(config, dyn_config["gateways"])
            filename = "//sys/yql_agent/proto_gateways/default.conf"
            create("file", filename, recursive=True, force=True)
            write_file(filename, MessageToString(config).encode('utf-8'))

        super()._update_dyn_config(yql_agent, dyn_config)

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


@authors("mpereskokova")
class TestMaxYqlVersionConfigAttrWithQtWorker(test_simple.TestMaxYqlVersionConfigAttr):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestNotTableResultWithQtWorker(test_simple.TestNotTableResult):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
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
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestExecutionModesYqlWithQtWorker(test_simple.TestExecutionModesYql):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestYqlPluginWithQtWorker(test_simple.TestYqlPlugin):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestDefaultClusterWithQtWorker(test_simple.TestDefaultCluster):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestAllYqlAgentsOverloadWithQtWorker(test_simple.TestAllYqlAgentsOverload):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestPartialYqlAgentsOverloadWithQtWorker(test_simple.TestPartialYqlAgentsOverload):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestQueriesYqlLimitedResultWithQtWorker(test_simple.TestQueriesYqlLimitedResult):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestQueriesYqlResultTruncationWithQtWorker(test_simple.TestQueriesYqlResultTruncation):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestQueriesYqlAuthWithQtWorker(test_simple.TestQueriesYqlAuth):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestQueriesYqlWithSecretsWithQtWorker(test_simple.TestQueriesYqlWithSecrets):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
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
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestYqlColumnOrderDifferentSourcesWithQtWorker(test_simple.TestYqlColumnOrderDifferentSources):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestAssignedEngineWithQtWorker(test_simple.TestAssignedEngine):
    YQL_QTWORKER = True


@authors("mpereskokova")
class TestAstReturnsWithQtWorker(test_simple.TestAstReturns):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestYqlVersionChangesWithQtWorker(test_simple.TestYqlVersionChanges):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestAgentWithInvalidMaxYqlVersionWithQtWorker(test_simple.TestAgentWithInvalidMaxYqlVersion):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
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
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestDeclareWithQtWorker(test_simple.TestDeclare):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestsDDLWithQtWorker(test_simple.TestsDDL):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestCrossClusterQueriesYqlWithQtWorker(test_simple.TestCrossClusterQueriesYql):
    YQL_QTWORKER = True


@authors("mpereskokova")
@pytest.mark.skip(reason="TODO@mpereskokova")
class TestOperationOptionsWithQtWorker(test_simple.TestOperationOptions):
    YQL_QTWORKER = True
