import random
import string

import yt.wrapper as yt
from yt.wrapper.http_helpers import get_proxy_url


def random_string(n, exact=False):
    len = n if exact else random.randint(1, n)
    return "".join(random.choice(string.ascii_letters) for i in range(len))


def make_client_factory(backend="http", proxy=None):
    def factory(trace=None):
        client = yt.YtClient(
            proxy=proxy or get_proxy_url(),
            config={"backend": backend, "dynamic_table_retries": {"enable": False}})
        if trace is not None:
            client.COMMAND_PARAMS["ifs_trace"] = trace
        return client
    return factory


def create_client(backend="http", trace=None, proxy=None):
    return make_client_factory(backend=backend, proxy=proxy)(trace=trace)


default_client_factory = make_client_factory()


class ValidationError(Exception):
    pass
