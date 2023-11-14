from yt_odin_checks.lib.check_runner import main

from yt.packages.requests.adapters import HTTPAdapter
from yt.packages.requests.exceptions import RequestException
import yt.packages.requests as requests

from collections import defaultdict
from threading import Thread
import time


MAX_REQUESTS_PER_WORKER = 5
TIMEOUT = 5

UNKNOWN = -1
OK = 0
WARNING = 1
CRITICAL = 2


class Worker(Thread):
    def __init__(self, token, proxies, is_testing_environment, logger):
        super(Worker, self).__init__()
        self.token = token
        self.proxies = proxies
        self.is_testing_environment = is_testing_environment
        self.results = dict([(proxy, CRITICAL) for proxy in self.proxies])
        self.logger = logger

    def run_impl(self):
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=3))
        for proxy in self.proxies:
            url = "http://{}/auth/whoami".format(proxy)
            check_result = CRITICAL
            try:
                resp = s.post(url, headers={"Authorization": "OAuth " + self.token}, timeout=TIMEOUT)
                if self.is_testing_environment:
                    if resp.status_code == 200:
                        check_result = OK
                    else:
                        check_result = WARNING
                else:
                    if resp.status_code == 200 and "odin" in resp.json().get("login", ""):
                        check_result = OK
                    if b"optimistic_cache" in resp.content:
                        check_result = WARNING
            except RequestException:
                check_result = UNKNOWN
            self.results[proxy] = check_result

    def run(self):
        try:
            self.run_impl()
        except Exception:
            self.logger.exception("Exception in a worker thread")
            raise

    def get_results(self):
        return self.results


def get_active_proxies(cluster_url, test_url):
    url = "{}/hosts/all".format(cluster_url)
    proxies = []
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))
    resp = s.get(url, timeout=TIMEOUT)
    for proxy in resp.json():
        if proxy["dead"]:
            continue
        if test_url:
            proxies.append("localhost:" + proxy["name"].split(":")[1])
        else:
            proxies.append(proxy["name"].split(":")[0])
    return proxies


def run_check(secrets, yt_client, logger, options, states):
    is_testing_environment = options.get("is_testing_environment", False)
    url_from_config = yt_client.config['proxy']['url']
    if is_testing_environment:
        cluster_url = "http://localhost:" + url_from_config.split(":")[1]
    elif any(i in url_from_config for i in ["yt.yandex", ".ofd."]):
        cluster_url = "http://{}".format(url_from_config)
    else:
        cluster_url = "http://{}.yt.yandex.net".format(url_from_config)

    try:
        proxies = get_active_proxies(cluster_url, is_testing_environment)
    except (RequestException, ValueError) as error:
        msg = "Can't get a list of proxies with error: '{}'".format(error)
        logger.info(msg)
        return states.UNAVAILABLE_STATE, msg

    workers = []
    for i in range(0, len(proxies), MAX_REQUESTS_PER_WORKER):
        worker = Worker(
            secrets["yt_token"],
            proxies[i:i + MAX_REQUESTS_PER_WORKER],
            is_testing_environment,
            logger)
        workers.append(worker)
        worker.start()

    bad_proxies = defaultdict(list)
    while workers:
        alive_workers = []
        for worker in workers:
            is_alive = worker.isAlive() if hasattr(worker, "isAlive") else worker.is_alive()
            if is_alive:
                alive_workers.append(worker)
                continue

            for proxy, result in worker.get_results().items():
                if result in (WARNING, CRITICAL, UNKNOWN):
                    bad_proxies[result].append(proxy)
            time.sleep(0.1)

        workers = alive_workers

    exit_code = OK
    messages = []
    if UNKNOWN in bad_proxies:
        exit_code = WARNING
        message = "WARNING! Some proxies unavailable: {}".format(" ".join(bad_proxies[UNKNOWN]))
        logger.info(message)
        messages.append(message)
    if WARNING in bad_proxies:
        exit_code = WARNING
        message = "WARNING! Some proxies use optimistic cache for auth: {}".format(" ".join(bad_proxies[WARNING]))
        logger.info(message)
        messages.append(message)
    if CRITICAL in bad_proxies:
        exit_code = CRITICAL
        message = "CRITICAL! Some proxies can't auth me: {}".format(" ".join(bad_proxies[CRITICAL]))
        logger.info(message)
        messages.append(message)

    if exit_code == OK:
        return states.FULLY_AVAILABLE_STATE, "OK"
    elif exit_code == WARNING:
        return states.PARTIALLY_AVAILABLE_STATE, '; '.join(messages)
    elif exit_code == CRITICAL:
        return states.UNAVAILABLE_STATE, '; '.join(messages)


if __name__ == "__main__":
    main(run_check)
