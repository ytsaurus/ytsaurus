import yt.wrapper as yt

def test_response_error():
    error = yt.YtResponseError(error={"code": 1,
                                      "inner_errors": [
                                          {"code": "904",
                                           "inner_errors": [] }
                                      ]})
    assert error.is_request_rate_limit_exceeded()
