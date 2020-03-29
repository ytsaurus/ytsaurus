from yt.yson import dumps
from yt.wrapper import YtClient
from yt.wrapper import YtHttpResponseError
from yt.wrapper.config import get_config, get_option, set_option
from yt.wrapper.http_helpers import get_proxy_url, RequestRetrier, get_token, make_request_with_retries, \
    get_error_from_headers
from yt.wrapper.http_driver import HeavyProxyProvider, HeavyProxyProviderState, TokenAuth
from yt.wrapper.common import get_version, get_started_by_short
import yt.logger as logger
import json


class ClickhouseYtClient(object):
    def __init__(self, alias, yt_client=None, cluster=None):
        if yt_client is not None:
            assert cluster is None
            self.yt_client = yt_client
        else:
            self.yt_client = YtClient(proxy=cluster, config=get_config(None))
        self.alias = alias

    def make_query(self, query, timeout=None):
        params = {"database": self.alias}

        query += "    FORMAT JSON"

        # prepare url.
        url_pattern = "http://{proxy}/query"
        proxy_provider_state = get_option("_heavy_proxy_provider_state", self.yt_client)
        if proxy_provider_state is None:
            proxy_provider_state = HeavyProxyProviderState()
            set_option("_heavy_proxy_provider_state", proxy_provider_state, self.yt_client)
        proxy_provider = HeavyProxyProvider(self.yt_client, proxy_provider_state)
        url = url_pattern.format(proxy="{proxy}", database=self.alias)

        # prepare params, format and headers
        user_agent = "Python ClickhouseYt client " + get_version()

        headers = {"User-Agent": user_agent,
                   "X-Started-By": dumps(get_started_by_short())}

        auth = TokenAuth(get_token(client=self.yt_client))

        self.yt_client.config["proxy"]["check_response_format"] = False

        response = make_request_with_retries(
            "POST",
            url,
            make_retries=True,
            retry_action=None,
            log_body=True,
            headers=headers,
            data=query,
            params=params,
            timeout=timeout,
            auth=auth,
            proxy_provider=proxy_provider,
            client=self.yt_client)

        def process_error(response):
            trailers = response.trailers()
            if trailers is None:
                return

            error = get_error_from_headers(trailers)
            if error is not None:
                raise YtHttpResponseError(error=json.loads(error), **response.request_info)

        response_content = response.text
        # NOTE: Should be called after accessing "text" or "content" attribute
        # to ensure that all response is read and trailers are processed and can be accessed.
        process_error(response)

        return json.loads(response_content)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--alias", help="Clique alias to query", required=True)
    parser.add_argument("--query", help="Query", required=True)
    args = parser.parse_args()

    chyt_client = ClickhouseYtClient(args.alias)
    print json.dumps(chyt_client.make_query(args.query), indent=4)
