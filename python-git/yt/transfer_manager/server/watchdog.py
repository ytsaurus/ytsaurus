#!/usr/bin/env python

import yt.wrapper as yt

import time
import requests
import argparse
import subprocess
import logging

logger = logging.getLogger("Watchdog")

def main():
    logger.setLevel(logging.INFO)
    logger.handlers.append(logging.StreamHandler())
    logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    parser = argparse.ArgumentParser(description="Transfer Manager watchdog")
    parser.add_argument("--url", help="Transfer Manager url", required=True)
    parser.add_argument("--attempt-count", type=int, default=1)
    parser.add_argument("--attempt-timeout", type=int, default=10)
    parser.add_argument("--cypress-path", help="Transfer Manager path in Cypress",
                        default="//sys/transfer_manager")
    args = parser.parse_args()

    logger.info("Starting ping attempts...")

    if not args.url.startswith("http://"):
        args.url = "http://" + args.url

    failed_attempts = 0
    for attempt in xrange(args.attempt_count):
        try:
            requests.get(args.url + "/ping/", timeout=args.attempt_timeout)
        except requests.exceptions.RequestException:
            failed_attempts += 1
            logger.exception("Attempt %d failed", attempt + 1)

    if failed_attempts == args.attempt_count:
        logger.warning("All attempts failed. Killing Transfer Manager and aborting lock transaction")

        locks = yt.get(yt.ypath_join(args.cypress_path, "lock/@locks"))
        if not locks:
            return
        yt.abort_transaction(locks[0]["transaction_id"])
        logger.info("Aborted lock transaction %s", locks[0]["transaction_id"])

        subprocess.check_call(["sv", "kill", "transfer_manager"])
        time.sleep(30)
        subprocess.check_call(["sv", "start", "transfer_manager"])
        logger.info("Restarted Transfer Manager")
    else:
        logger.info("Everything is ok. Transfer Manager is alive")

if __name__ == "__main__":
    main()
