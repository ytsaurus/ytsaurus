#!/usr/bin/env python

import yt.wrapper as yt

import time
import requests
import argparse
import socket
import subprocess
import logging

logger = logging.getLogger("Watchdog")

SLEEP_TIME_BEFORE_START = 10

def main():
    logger.setLevel(logging.INFO)
    logger.handlers.append(logging.StreamHandler())
    logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    parser = argparse.ArgumentParser(description="Transfer Manager watchdog")
    parser.add_argument("--url", help="Transfer Manager url", required=True)
    parser.add_argument("--failed-attempt-count-to-kill", type=int, default=1)
    parser.add_argument("--attempt-timeout", type=int, default=10)
    parser.add_argument("--cypress-path", help="Transfer Manager path in Cypress",
                        default="//sys/transfer_manager")
    args = parser.parse_args()

    status = subprocess.check_output(["sv", "status", "transfer_manager"])
    if status.startswith("down"):
        logger.info("Transfer Manager is down. Doing nothing")
        return

    logger.info("Starting ping attempts...")

    if not args.url.startswith("http://"):
        args.url = "http://" + args.url

    failed_attempts = 0
    for attempt in xrange(args.failed_attempt_count_to_kill):
        try:
            requests.get(args.url + "/ping/", timeout=args.attempt_timeout)
        except requests.exceptions.RequestException:
            failed_attempts += 1
            logger.exception("Attempt %d failed", attempt + 1)

    if failed_attempts == args.failed_attempt_count_to_kill:
        logger.warning("All attempts failed. Killing Transfer Manager and aborting lock transaction")

        locks = yt.get(yt.ypath_join(args.cypress_path, "lock/@locks"))
        if not locks:
            logger.info("No locks acquired. Killing Transfer Manager process")
            subprocess.check_call(["sv", "kill", "transfer_manager"])
            time.sleep(SLEEP_TIME_BEFORE_START)
            subprocess.check_call(["sv", "start", "transfer_manager"])
            logger.info("Started Transfer Manager")
            return

        transaction_id = locks[0]["transaction_id"]

        subprocess.check_call(["sv", "force-stop", "transfer_manager"])
        logger.info("Killed Transfer Manager process")
        time.sleep(SLEEP_TIME_BEFORE_START)

        if socket.getfqdn() == yt.get_attribute(args.cypress_path, "address"):
            yt.abort_transaction(transaction_id)
            logger.info("Aborted lock transaction %s", transaction_id)
        else:
            logger.info("Lock is acquired by another instance, skipping transaction abort")

        subprocess.check_call(["sv", "start", "transfer_manager"])
        logger.info("Started Transfer Manager")
    else:
        logger.info("Everything is ok. Transfer Manager is alive")

if __name__ == "__main__":
    main()
