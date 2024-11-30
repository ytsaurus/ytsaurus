from .conftest import authors

import yt.wrapper as yt

import pytest

from yt.wrapper.testlib.helpers import wait


# This testsuite is mostly following the similar cases from tests/integration/queries/test_mock.py.
# By using the mock engine we can keep this suite fast and not depending on heavy engines like YQL or CHYT.

@pytest.mark.usefixtures("yt_env", "yt_query_tracker")
class TestQueryCommands(object):
    ERROR = {"code": 42, "message": "Mock query execution error", "attributes": {"some_attr": "some_value"}}
    ROWSET1 = {
        "schema": [{"name": "foo", "type": "int64"}, {"name": "bar", "type": "string"}],
        "rows": [{"foo": 42, "bar": "abc"}, {"foo": -17, "bar": "def"}, {"foo": 123, "bar": "ghi"}]
    }
    ROWSET2 = {
        "schema": [{"name": "baz", "type": "any"}],
        "rows": [{"baz": 42}, {"baz": "abc"}, {"baz": True}]
    }
    TRUNCATED_ROWSET = {
        "schema": [{"name": "qux", "type": "int64"}],
        "rows": [{"qux": 0}, {"qux": 1}, {"qux": 2}],
        "is_truncated": True
    }

    @authors("max42")
    def test_fail(self, yt_query_tracker):
        with pytest.raises(yt.errors.YtQueryFailedError):
            yt.run_query("mock", "fail")

        with pytest.raises(yt.errors.YtQueryFailedError):
            yt.run_query("mock", "fail_by_exception")

        with pytest.raises(yt.errors.YtQueryFailedError):
            yt.run_query("mock", "fail_after", settings={"duration": 3000})

        with pytest.raises(yt.errors.YtQueryFailedError):
            yt.run_query("mock", "complete_after", settings={
                "duration": 500,
                "results": [
                    {"error": self.ERROR},
                ]
            })

        q = yt.run_query("mock", "complete_after", sync=False, settings={
            "duration": 500,
            "results": [
                {"error": self.ERROR},
            ]
        })
        with pytest.raises(yt.errors.YtQueryFailedError):
            q.wait()
        assert q.get_state() == "completed"
        assert q.get_error() is not None
        assert q.get_error(return_error_if_all_results_are_errors=False) is None
        assert q.get_result(0).get_error() is not None

    @authors("max42")
    def test_abort(self, yt_query_tracker):
        q = yt.run_query("mock", "run_forever", sync=False)
        wait(lambda: q.get_state() == "running")
        q.abort()
        with pytest.raises(yt.errors.YtQueryFailedError):
            q.wait()
        assert q.get_state() == "aborted"

    @authors("max42")
    def test_complete_single_result(self, yt_query_tracker):
        q = yt.run_query("mock", "complete_after", settings={
            "duration": 500,
            "results": [
                self.ROWSET1,
            ]
        })

        assert list(q) == self.ROWSET1["rows"]
        assert list(q.get_result(0)) == self.ROWSET1["rows"]
        assert list(q.get_result(0).read_rows()) == self.ROWSET1["rows"]

    @authors("max42")
    def test_complete_multiple_results(self, yt_query_tracker):
        q = yt.run_query("mock", "complete_after", settings={
            "duration": 500,
            "results": [
                self.ROWSET1,
                self.ROWSET2,
            ]
        })

        with pytest.raises(yt.errors.YtError):
            # For a query with a number of results different from one, using it as an iterator is not allowed.
            list(q)

        results = q.get_results()
        assert len(results) == 2
        assert list(results[0]) == self.ROWSET1["rows"]
        assert list(results[1]) == self.ROWSET2["rows"]

    @authors("max42")
    def test_complete_truncated_result(self, yt_query_tracker):
        q = yt.run_query("mock", "complete_after", settings={
            "duration": 500,
            "results": [
                self.TRUNCATED_ROWSET,
            ]
        })

        # For a query with a truncated result, using it as an iterator throws an error,
        # unless user very specifically asks for the resulting rows.
        with pytest.raises(yt.errors.YtError):
            list(q)
        with pytest.raises(yt.errors.YtError):
            list(q.get_result(0))
        with pytest.raises(yt.errors.YtError):
            list(q.get_result(0).read_rows())
        assert list(q.get_result(0).read_rows(validate_not_truncated=False)) == self.TRUNCATED_ROWSET["rows"]
