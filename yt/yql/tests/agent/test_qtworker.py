import test_simple
import test_udfs

from common import TestQueriesYqlBase, TestUpdateYqlAgentQtWorkerDynamicConfigMixin

from yt.environment.helpers import assert_items_equal

from yt_commands import authors, create, create_user, issue_token, write_table, raises_yt_error, wait, update_access_control_object_acl

from dirty_equals import AnyThing

import yql.tools.yqlworker.interface.proto.task_pb2 as task_pb2

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
        addresss = yql_agent.yql_agent.env.get_http_proxy_address()

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
                        "FileAlias": f"yt://{addresss}//sys/yql_agent/udfs/libsimple_udf.so",
                    }
                ],
                "Udfs": AnyThing(),
            }
        ]


class TaskDataFlavorsMixin:
    YQL_SERVICE_TOKEN = "fake-yql-service-token"

    @staticmethod
    def _make_task_data(**fields):
        # These fields are filled by the yql service in production and must be
        # taken from the task data as is, unlike the fields overwritten by the plugin.
        fields = {
            "Syntax": task_pb2.SQLv1,
            "PersistedId": True,
            "Runner": "yql-agent",
            "Program": "select \"the program from the task data must not be used\"",
            "Id": "the id from the task data must not be used",
            **fields,
        }
        return task_pb2.TTaskData(**fields).SerializeToString()

    @classmethod
    def _yql_service_settings(self, task_data=None, tokens=None, **extra):
        settings = {
            "yql_flavor": "yql-service",
            "tokens": {
                "yql_service_token": self.YQL_SERVICE_TOKEN,
                **(tokens or {}),
            },
        }
        if task_data is not None:
            settings["yql_task_data"] = task_data
        settings.update(extra)
        return settings


class TestTaskDataFlavors(TaskDataFlavorsMixin, test_simple.TestQueriesYqlSimpleBase):
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

    @authors("mpereskokova")
    def test_yql_service_flavor_without_token(self, query_tracker, yql_agent):
        self._test_simple_query_error(
            "select 1",
            "Missed yql_service_token setting for yql-service flavor",
            settings={"yql_flavor": "yql-service", "yql_task_data": self._make_task_data()},
        )

    @authors("mpereskokova")
    def test_yql_service_flavor_without_task_data(self, query_tracker, yql_agent):
        self._test_simple_query_error(
            "select 1",
            "Missed yql_task_data setting for yql-service flavor",
            settings=self._yql_service_settings(),
        )

    @authors("mpereskokova")
    def test_yql_service_flavor_with_malformed_task_data(self, query_tracker, yql_agent):
        self._test_simple_query_error(
            "select 1",
            "Failed to deserialize TTaskData",
            settings=self._yql_service_settings("not-a-serialized-task-data"),
        )

    @authors("mpereskokova")
    def test_yql_service_flavor_with_malformed_user_auth_data(self, query_tracker, yql_agent):
        self._test_simple_query_error(
            "select 1",
            "Failed to deserialize UserAuthData",
            settings=self._yql_service_settings(
                self._make_task_data(),
                tokens={"yql_user_auth_data": "not-a-serialized-user-auth-data"},
            ),
        )

    @authors("mpereskokova")
    def test_yql_service_flavor(self, query_tracker, yql_agent):
        result = self._run_simple_query(
            "select CurrentLanguageVersion() as result",
            settings=self._yql_service_settings(
                self._make_task_data(),
                yql_version="2025.02",
                tokens={"yql_auth_data": task_pb2.TTaskAuthTokens().SerializeToString()},
            ),
        )
        assert_items_equal(result, [{"result": "2025.02"}])

    @authors("mpereskokova")
    def test_yql_service_flavor_files(self, query_tracker, yql_agent):
        task_data = self._make_task_data(Files=[
            task_pb2.TTaskFile(
                Name="test_file_raw",
                Type=task_pb2.TTaskFile.CONTENT,
                Content="the file from the task data must not be used",
            ),
        ])

        self._test_simple_query(
            "select FileContent(\"test_file_raw\") as column",
            [{"column": "test_content"}],
            files=[{"name": "test_file_raw", "content": "test_content", "type": "raw_inline_data"}],
            settings=self._yql_service_settings(task_data),
        )

    @authors("mpereskokova")
    def test_yql_service_flavor_reads_table(self, query_tracker, yql_agent):
        cluster = yql_agent.yql_agent.env.id

        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t", rows)

        task_data = self._make_task_data(DefaultTranslationCluster=cluster, Url=cluster)

        self._test_simple_query(
            f"select * from {cluster}.`//tmp/t`",
            rows,
            settings=self._yql_service_settings(task_data),
        )


class TestTaskDataFlavorsAuth(TaskDataFlavorsMixin, test_simple.TestQueriesYqlAuthBase):
    # //tmp/t with a = 42 is created by the base class, it is readable by "allowed_user" only.
    YQL_QTWORKER = True

    def _run_yql_service_query(self, username, auth_tokens=None):
        cluster = self.Env.id

        tokens = {}
        if auth_tokens is not None:
            tokens["yql_auth_data"] = auth_tokens.SerializeToString()

        self._test_simple_query(
            f"select a + 1 as b from {cluster}.`//tmp/t`",
            [{"b": 43}],
            authenticated_user=username,
            settings=self._yql_service_settings(
                self._make_task_data(DefaultTranslationCluster=cluster, Url=cluster),
                tokens=tokens,
            ),
        )

    @staticmethod
    def _make_auth_tokens(token):
        return task_pb2.TTaskAuthTokens(Tokens=[
            task_pb2.TTaskAuthToken(Alias="default_yt", Category="yt", Content=token.encode("utf-8")),
        ])

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_yql_service_flavor_auth_data(self, query_tracker, yql_agent):
        allowed_token, _ = issue_token("allowed_user")
        self._run_yql_service_query("allowed_user", self._make_auth_tokens(allowed_token))

        denied_token, _ = issue_token("denied_user")
        with raises_yt_error("Access denied for user \"denied_user\""):
            self._run_yql_service_query("denied_user", self._make_auth_tokens(denied_token))

        self._run_yql_service_query(
            "allowed_user",
            self._make_auth_tokens(allowed_token),
        )
