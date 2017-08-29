from yt.yson import load
import yt_driver_bindings

import yt.wrapper as yt

import signal
import sys

def main():
    signal.signal(signal.SIGINT, signal.default_int_handler)
    config = {
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

    with open(sys.argv[1], "rb") as f:
        console_driver_config = load(f)

    yt.config["driver_config"] = console_driver_config["driver"]
    yt_driver_bindings.configure_logging(console_driver_config["logging"])

    yt.update_config(config)
    yt.config["backend"] = "native"

    yt.get("//home/test_table")

if __name__ == "__main__":
    main()
