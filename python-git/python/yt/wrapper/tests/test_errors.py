import yt.wrapper as yt

def test_response_error():
    error = yt.YtResponseError("http://abc/api/v2/get", headers={},
                               error={"code": 1,
                                      "inner_errors": [
                                          {"code": "904",
                                           "inner_errors": [] }
                                      ]})
    assert error.is_request_rate_limit_exceeded()
