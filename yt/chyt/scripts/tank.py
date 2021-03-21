from yt.wrapper.http_driver import HeavyProxyProvider
from yt.wrapper.http_helpers import get_token

import argparse
import logging
import multiprocessing
import requests
import signal

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s"))
logger.addHandler(handler)


# Copy-paste from arcadia/ads/libs/chyt/chyt.py :)
def run_chyt_query(worker_index, query, heavy_proxy, alias, token):
    logger.debug("[%d] Executing query: %s", worker_index, query)
    url = "http://{proxy}/query?database={alias}".format(proxy=heavy_proxy, alias=alias)
    s = requests.Session()
    s.headers['Authorization'] = 'OAuth {token}'.format(token=token)
    resp = s.post(url, data=query, allow_redirects=False, timeout=300)
    logger.debug("[%d] Response status: %s", worker_index, resp.status_code)
    logger.debug("[%d] Response headers: %s", worker_index, resp.headers)
    logger.debug("[%d] Response content: %s", worker_index, resp.content)
    if resp.status_code != 200:
        logger.error("Error: %s", resp.content)
    resp.raise_for_status()
    rows = resp.content.strip().split('\n')
    logger.debug("[%d] Time spent: %s seconds, rows returned: %s", worker_index, resp.elapsed.total_seconds(), len(rows))
    query_execution = {
        "time": resp.elapsed.total_seconds(),
        "query_id": resp.headers["X-ClickHouse-Query-Id"],
        "trace_id": resp.headers["X-Yt-Trace-Id"],
        "proxy": resp.headers["X-Yt-Proxy"],
        "request_id": resp.headers["X-Yt-Request-Id"],
    }
    return query_execution


def worker_guarded(worker_index, alias, query, log_each_query):
    logger.info("[%d] Starting worker", worker_index)
    heavy_proxy_provider = HeavyProxyProvider(None)
    heavy_proxy = heavy_proxy_provider()
    logger.info("[%d] Heavy proxy is %s", worker_index, heavy_proxy)

    index = 0
    executions = []
    while True:
        logger.debug("[%d] Making query %d", worker_index, index)
        execution = run_chyt_query(worker_index, query, heavy_proxy, alias, get_token())
        executions.append(execution)
        index += 1
        if log_each_query:
            logger.info("[%d] Query %d: query_id = %s, request_id = %s, time = %f", worker_index,
                        index, execution["query_id"], execution["request_id"], execution["time"])
        if index & (index - 1) == 0:
            logger.info("[%d] Average time on first %d queries is %f", worker_index, index,
                        sum(execution["time"] for execution in executions) / index)
            slowest_execution = max(executions, key=lambda execution: execution["time"])
            logger.info("[%d] Slowest execution is: query_id = %s, request_id = %s, time = %f", worker_index,
                        slowest_execution["query_id"], slowest_execution["request_id"], slowest_execution["time"])

def worker(worker_index, alias, query, log_each_query):
    try:
        worker_guarded(worker_index, alias, query, log_each_query)
    except Exception:
        logger.exception("[%d] Caught exception", worker_index)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool-size", type=int, help="Number of requests going in parallel", required=True)
    parser.add_argument("--alias", help="Operation alias to query", required=True)
    parser.add_argument("--query", help="Query", required=True)
    parser.add_argument("-v", "--verbose", help="Verbosity", action="store_true")
    parser.add_argument("--log-each-query", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("Running tank with pool of size %d over clique %s with query %s", args.pool_size, args.alias, args.query)
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = multiprocessing.Pool(args.pool_size)
    signal.signal(signal.SIGINT, original_sigint_handler)
    results = []
    for index in xrange(args.pool_size):
        results.append(pool.apply_async(worker, args=(index, args.alias, args.query, args.log_each_query)))
    try:
        for result in results:
            result.get(1e9)
    except KeyboardInterrupt:
        logger.info("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
        pool.join()
    else:
        pool.close()
        pool.join()


if __name__ == "__main__":
    main()
