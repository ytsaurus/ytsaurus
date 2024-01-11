import yt.wrapper as yt
from yt.wrapper.client import YtClient
from yt.wrapper.native_driver import make_request
from yt.wrapper.http_helpers import get_proxy_url
import yt.yson as yson
from yt.common import YtError
from time import sleep
import random
import sys
import os

class JobBase(object):
    def __init__(self, spec):
        self.retry_interval = spec.retries.interval
        self.retry_count = spec.retries.count
        self.proxy = get_proxy_url()
        self.ipv4 = spec.ipv4

    def make_request(self, command, params, data, client):
        for attempt in range(1, self.retry_count + 1):
            try:
                return make_request(command, params, data=data, client=client)
            except YtError as error:
                print("Attempt", attempt, file=sys.stderr)
                print(str(error), file=sys.stderr)
                print("\n" + "=" * 80 + "\n\n", file=sys.stderr)
                sleep(random.randint(1, self.retry_interval))
        if len(params.get("query", "")) > 200:
            query = params["query"]
            params["query"] = query[:100] + "... ({} characters truncated) ...".format(len(query) - 200) + query[-100:]
        raise Exception("Failed to execute command (%s attempts): %s %s" % (attempt, command, str(params)))

    def create_client(self):
        client_config = {"backend": "rpc", "allow_http_requests_to_yt_from_job": True}
        if self.ipv4:
            client_config["driver_address_resolver_config"] = {"enable_ipv4": True}
        return yt.YtClient(
            proxy=self.proxy,
            config=client_config,
            token=os.environ["YT_SECURE_VAULT_YT_TOKEN"]
        )

    def prepare(self, value):
        if not isinstance(value, list):
            value = [value]
        return yson.dumps(value, yson_type="list_fragment", yson_format="text")
