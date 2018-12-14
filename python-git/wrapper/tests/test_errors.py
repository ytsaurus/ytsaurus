import yt.wrapper as yt

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
