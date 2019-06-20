from yt.common import YtError

import yt.wrapper as yt

import pytest

def _create_error(code):
    return yt.YtResponseError(error={"code": 1,
                                     "inner_errors": [
                                         {"code": code,
                                          "inner_errors": [] }
                                     ]})

def test_response_error():
    assert _create_error(str(904)).is_request_rate_limit_exceeded()

def test_rpc_unavailable():
    assert _create_error(str(105)).is_rpc_unavailable()

@pytest.mark.usefixtures("yt_env")
class TestYtError(object):
    def test_error_matching(self):
        yt.create("table", "//tmp/t_in")
        yt.write_table("//tmp/t_in", [{"a": 1}])
        yt.create("table", "//tmp/t_out", attributes={"schema": [{"name": "a", "sort_order": "ascending", "type": "int64"}]})
        try:
            yt.run_map("echo '{a=1};{a=0}'", format="yson", source_table="//tmp/t_in", destination_table="//tmp/t_out", spec={"max_failed_job_count": 0})
        except YtError as err:
            assert err.contains_code(1205)  # JobProxy::EErrorCode::UserJobFailed
            assert err.contains_code(301)   # TableClient::EErrorCode::SortOrderViolation
            assert err.find_matching_error(code=1205).code == 1205
            assert err.find_matching_error(code=301).code == 301
            assert err.find_matching_error(code=42) is None
            assert err.find_matching_error(predicate=lambda error: error.attributes.get("fatal") == True).code == 1205
            assert err.contains_text("job failed")
            assert err.contains_text("order violation")
            assert err.contains_text("closing table")
            assert err.contains_text("fatal")
            assert not err.contains_code("dzhoba ne rabotaet")
            assert err.matches_regexp(".*ob.*(iled|orted)")
            assert not err.matches_regexp("(peration|ob).*ompleted")
