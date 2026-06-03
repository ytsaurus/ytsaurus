from common import TestQueriesYqlBase

from yt_commands import authors


class TestQTWorkerStart(TestQueriesYqlBase):
    YQL_QTWORKER = True

    @authors("mpereskokova")
    def test_qtworker_start(self, query_tracker, yql_agent):
        pass
