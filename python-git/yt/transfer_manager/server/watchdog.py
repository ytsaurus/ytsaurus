#!/usr/bin/env python

from yt.packages.six.moves import xrange

import yt.wrapper as yt

import time
import requests
import argparse
import socket
import subprocess
import logging
import smtplib
from datetime import datetime
from email.mime.text import MIMEText

logger = logging.getLogger("Watchdog")

SLEEP_TIME_BEFORE_START = 10

MAIL_SERVER = "outbound-relay.yandex.net"
MAIL_TOPIC = "Watchdog run " + datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def send_email(text, recipients):
    from_ = "tm-watchdog@" + socket.getfqdn()

    msg = MIMEText(text)
    msg["Subject"] = MAIL_TOPIC
    msg["From"] = from_
    msg["To"] = ", ".join(recipients)

    s = smtplib.SMTP(MAIL_SERVER)
    s.sendmail(from_, recipients, msg.as_string())
    s.quit()

def main():
    logger.setLevel(logging.INFO)
    logger.handlers.append(logging.StreamHandler())
    logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    parser = argparse.ArgumentParser(description="Transfer Manager watchdog")
    parser.add_argument("--url", help="Transfer Manager url", required=True)
    parser.add_argument("--failed-attempt-count-to-kill", type=int, default=1)
    parser.add_argument("--attempt-timeout", type=int, default=10)
    parser.add_argument("--mail-to", action="append", help="send notifications to specified emails")
    parser.add_argument("--cypress-path", help="Transfer Manager path in Cypress",
                        default="//sys/transfer_manager")
    args = parser.parse_args()

    if not args.mail_to:
        args.mail_to = ["asaitgalin@yandex-team.ru", "ignat@yandex-team.ru"]

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

        send_email("Watchdog failed to ping TM after {0} attempts. "
                   "Instance will be killed.".format(args.failed_attempt_count_to_kill), args.mail_to)

        locks = yt.get(yt.ypath_join(args.cypress_path, "lock/@locks"))
        if not locks:
            logger.info("No locks acquired. Killing Transfer Manager process")
            subprocess.check_call(["sv", "kill", "transfer_manager"])
            time.sleep(SLEEP_TIME_BEFORE_START)
            subprocess.check_call(["sv", "start", "transfer_manager"])
            logger.info("Started Transfer Manager")
            return

        transaction_id = locks[0]["transaction_id"]

        try:
            subprocess.check_call(["sv", "force-stop", "transfer_manager"])
        except subprocess.CalledProcessError as err:
            logger.warning("Force stop exited with non-zero status %d and message: %s",
                           err.returncode, err.output)

        logger.info("Killed Transfer Manager process")
        time.sleep(SLEEP_TIME_BEFORE_START)

        if socket.getfqdn() == yt.get_attribute(args.cypress_path, "address"):
            send_email("Instance was killed but its transaction (id: {0}) "
                       "is still alive. Aborting it.".format(transaction_id), args.mail_to)
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
