from .conftest import authors
from .helpers import inject_http_error

import yt.wrapper as yt

import yt.packages.requests as yt_requests
from yt.packages import urllib3

import pytest
from copy import deepcopy


@pytest.mark.usefixtures("test_environment_v4")
class TestHTTPRetries(object):
    @authors("denvr")
    def test_retries_502(self):
        client = yt.YtClient(config=yt.config.config)

        response_502_empty = yt_requests.Response()
        response_502_empty.status_code = 502
        response_502_empty.reason = "Bad Gateway"
        response_502_empty._content = b""
        response_502_empty.headers["Content-Type"] = "application/json"
        response_502_empty.raw = urllib3.HTTPResponse()

        # one 502 with empty body
        data = None
        with inject_http_error(client, "list", 0, 1, 1, response=response_502_empty) as cnt:
            data = client.list("/")
        assert cnt.filtered_raises == 1
        assert data == ["sys", "tmp"]

        response_502_good_json = yt_requests.Response()
        response_502_good_json.status_code = 502
        response_502_good_json.reason = "Bad Gateway"
        response_502_good_json._content = b"{\"error\": \"Bad Gateway\"}"
        response_502_good_json.headers["Content-Type"] = "application/json"
        response_502_good_json.raw = urllib3.HTTPResponse()
        error_502 = yt_requests.HTTPError(
            f"{response_502_good_json.status_code} Server Error: {response_502_good_json.reason} for url: {response_502_good_json.url}",
            response=response_502_good_json,
        )
        # one 502 with json body, expects yt error
        data = None
        with pytest.raises(yt.errors.YtResponseError) as ex:
            with inject_http_error(client, "list", 0, 1, 1, response=response_502_good_json) as cnt:
                data = client.list("/")
        assert cnt.filtered_raises == 1
        assert ex.value.inner_errors == [{'error': 'Bad Gateway'}]
        assert data is None
        # TODO(denvr) - continue retries, no inner errors

        response_502_bad_json = deepcopy(response_502_good_json)
        response_502_bad_json._content = b"<xml>not json</xml>"
        response_502_bad_json.raw = urllib3.HTTPResponse()
        error_502_bad_json = deepcopy(error_502)
        error_502_bad_json.response._content = b"no json data"
        # 502 with no json body
        data = None
        with pytest.raises(yt.errors.YtIncorrectResponse) as ex:
            with inject_http_error(client, "list", 0, 10, 1, response=response_502_bad_json) as cnt:
                data = client.list("/")
        assert cnt.filtered_raises == 10
        assert ex.value.message == "Response body can not be decoded from JSON"
        assert data is None, "Retries did not help"

        data = None
        with inject_http_error(client, "list", 0, 1, 1, response=response_502_bad_json) as cnt:
            data = client.list("/")
        assert data == ["sys", "tmp"], "Retries helps"
