from yt_odin_checks.lib.check_runner import main

import yt.packages.requests as requests

import yt.wrapper as yt


def check_proxy(proxy, logger, states):
    url = "http://{proxy}/api/v3".format(proxy=proxy)
    try:
        # TODO(ignat): Fix to avoid using private API.
        yt.http_helpers.make_request_with_retries("get", url, make_retries=True).json()
    except (yt.YtError, requests.RequestException) as error:
        logger.exception(str(error))
        return states.UNAVAILABLE_STATE

    return states.FULLY_AVAILABLE_STATE


def run_check(yt_client, logger, options, states):
    # TODO(ignat): Fix to avoid using private API.
    proxy = yt.http_helpers.get_proxy_url(client=yt_client)
    url = "http://{proxy}/hosts".format(proxy=proxy)
    heavy_proxies = yt.http_helpers.make_request_with_retries("get", url, make_retries=True).json()

    if not heavy_proxies:
        logger.error("There are not available heavy proxies.")
        return states.UNAVAILABLE_STATE

    logger.info("There are %d heavy proxies.", len(heavy_proxies))
    availabilities = [check_proxy(http_proxy, logger, states) for http_proxy in heavy_proxies]
    return float(sum(availabilities)) / len(heavy_proxies)


if __name__ == "__main__":
    main(run_check)
