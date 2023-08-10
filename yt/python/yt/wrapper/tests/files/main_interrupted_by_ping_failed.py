from yt.wrapper.testlib.helpers import set_config_option

import yt.wrapper as yt

import os
import sys
import time
import argparse

get_time = time.monotonic if hasattr(time, 'monotonic') else time.time


def simulate_ping_failed():
    with set_config_option("ping_failed_mode", "interrupt_main"):
        with set_config_option("transaction_timeout", 2000):
            tx_context_manager = yt.Transaction(ping_period=0, ping_timeout=5000)
            tx = tx_context_manager.__enter__()

            wait_time = 100.0

            wait_begin = get_time()
            wait_end = wait_begin + wait_time
            aborted = False

            while True:
                if not aborted:
                    yt.abort_transaction(tx.transaction_id)
                    aborted = True
                time.sleep(0.1)
                assert get_time() < wait_end

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exit-code", type=int, required=True)
    args = parser.parse_args()

    try:
        simulate_ping_failed()
    except KeyboardInterrupt:
        sys.exit(args.exit_code)


if __name__ == "__main__":
    main()
