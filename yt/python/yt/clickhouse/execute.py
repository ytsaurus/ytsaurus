import datetime

from .helpers import get_alias_from_env_or_raise

from yt.yson import dumps
from yt.wrapper import YtClient
from yt.wrapper.config import get_config, get_option, set_option
from yt.wrapper.http_helpers import get_token, format_logging_params, raise_for_token, _raise_for_status, create_response  # noqa
from yt.wrapper.http_driver import HeavyProxyProvider, HeavyProxyProviderState, TokenAuth
from yt.wrapper.common import get_version, get_started_by_short, generate_uuid, YtError
from yt.wrapper.errors import create_http_response_error
from yt.wrapper.common import hide_auth_headers
import yt.packages.requests as requests
import yt.logger as logger
import yt.json_wrapper as json
import re


FORMAT_CLAUSE_REGEX = re.compile(r"\sFORMAT\s+[A-Z_0-9]+[\s;]*$")


def get_heavy_proxy_provider(client):
    proxy_provider_state = get_option("_heavy_proxy_provider_state", client)
    if proxy_provider_state is None:
        proxy_provider_state = HeavyProxyProviderState()
        set_option("_heavy_proxy_provider_state", proxy_provider_state, client)
    proxy_provider = HeavyProxyProvider(client, proxy_provider_state)
    return proxy_provider


def execute(query, alias=None, raw=None, format=None, settings=None, traceparent=None, client=None):
    """Executes ClickHouse query in given CHYT clique.

    :param query: Query to execute.
    :type query: str
    :param alias: Clique alias. May be omitted, in which case will be retrieved from CHYT_ALIAS env variable.
    :type alias: str or None
    :param raw: if set to False, returned values are parsed into Python types (number types, dicts, lists, etc).
    Non-None format keyword implies raw = True.
    :type raw: bool or None
    :param format: ClickHouse format. Non-None value implies raw = True.
    :type format: str or None
    :param settings: Set of ClickHouse settings to apply.
    :type settings: dict or None
    :param client: YT client.
    :type client: YtClient or None
    :return: row iterator
    """
    settings = settings or {}

    alias = alias or get_alias_from_env_or_raise()

    if client is None:
        client = YtClient(config=get_config(client=None))

    if format is not None and raw:
        raise YtError("Raw cannot be specified simultaneously with format")

    if format is not None:
        raw = True

    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]
        if not raw:
            format = "JSONEachRow"
            settings["output_format_json_quote_64bit_integers"] = False

    if FORMAT_CLAUSE_REGEX.search(query.upper()):
        raise YtError("Do not specify FORMAT clause in query; use format keyword instead")

    proxy_provider = get_heavy_proxy_provider(client)
    url = "{}/query".format(proxy_provider())

    params = settings

    params.update({
        "database": alias,
        "default_format": format
    })

    user_agent = "Python ClickhouseYt client " + get_version()

    headers = {
        "User-Agent": user_agent,
        "X-Started-By": dumps(get_started_by_short()),
        "X-YT-Correlation-Id": generate_uuid(get_option("_random_generator", client))
    }

    if traceparent is not None:
        headers["traceparent"] = traceparent

    auth = TokenAuth(get_token(client=client))
    tvm_auth = get_config(client)["tvm_auth"]
    if tvm_auth is not None:
        auth = tvm_auth

    random_generator = get_option("_random_generator", client)
    request_id = "%08x" % random_generator.randrange(16**8)
    logging_params = {
        "headers": hide_auth_headers(headers),
        "request_id": request_id,
        "query": query.encode("utf-8"),
    }

    logger.debug("Perform HTTP post request %s (%s)",
                 url,
                 format_logging_params(logging_params))

    request_info = {"request_headers": headers, "url": url, "params": params}

    response_line_count = 0

    start_time = datetime.datetime.now()

    with requests.post(
            url,
            data=query.encode('utf-8'),
            params=params,
            headers=headers,
            auth=auth,
            timeout=get_config(client)["proxy"]["request_timeout"],
            stream=True) as response:

        logging_params = {
            "headers": hide_auth_headers(dict(response.headers)),
            "request_id": request_id,
            "status_code": response.status_code,
        }
        logger.debug("Response received (%s)", format_logging_params(logging_params))

        if response.status_code != 200:
            if "X-Yt-Error" in response.headers:
                # This case corresponds to situation when error is initiated by out proxy code.
                response = create_response(
                    response=response,
                    request_info={"headers": headers, "url": url, "params": params},
                    request_id=request_id,
                    error_format=None,
                    client=client
                )
                _raise_for_status(response, response.request_info)
            else:
                # This case corresponds to situation when error is forwarded from ClickHouse.
                error = create_http_response_error(YtError("ClickHouse error: " + response.text.strip(), attributes={
                    "trace_id": response.headers.get("X-YT-Trace-Id"),
                    "span_id": response.headers.get("X-YT-Span-Id"),
                }), response_headers=response.headers, **request_info)
                raise error

        for line in response.iter_lines():
            response_line_count += 1
            if raw:
                yield line
            else:
                yield json.loads(line)

    finish_time = datetime.datetime.now()

    logging_params = {
        "request_id": request_id,
        "response_line_count": response_line_count,
        "total_time": finish_time - start_time
    }
    logger.debug("Finished request execution (%s)", format_logging_params(logging_params))
