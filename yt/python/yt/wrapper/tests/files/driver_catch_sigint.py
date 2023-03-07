from yt.yson import load
from yt.packages.six.moves import xrange
from yt.common import update

import yt.wrapper as yt

import yt_driver_bindings

import signal
import sys

hanging_client_config = {
    "driver_config": {
        "primary_master": {
            "retry_backoff_time": 20000,
            "hard_backoff_time": 20000,
            "soft_backoff_time": 20000,
            "addresses": ["testaddr.yandex.net:2822"]
        },
        "cell_directory": {
            "retry_backoff_time": 20000,
            "hard_backoff_time": 20000,
            "soft_backoff_time": 20000
        }
    }
}

def main():
    signal.signal(signal.SIGINT, signal.default_int_handler)

    with open(sys.argv[1], "rb") as f:
        console_driver_config = load(f)

    yt_driver_bindings.configure_logging(console_driver_config["logging"])

    normal_client = yt.YtClient(config=
        {
            "driver_config": console_driver_config["driver"],
            "backend": "native"
        }
    )

    hanging_client = yt.YtClient(config=update(
        {
            "driver_config": console_driver_config["driver"],
            "backend": "native"
        },
        hanging_client_config)
    )

    # Hanged stream read.
    normal_client.config["write_retries"]["enable"] = False
    normal_client.write_file("//tmp/test_file", (b"".join([b"abcde" for i in xrange(1000)]) for _ in xrange(10 * 1000)))
    response_stream = normal_client.read_file("//tmp/test_file")
    next(response_stream)

    # Hanged client.
    hanging_client.get("//home/test_table")

if __name__ == "__main__":
    main()
