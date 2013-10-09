#!/usr/bin/env python

import yt.logger as logger
import yt.wrapper as yt

import os
import sys
import time
import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(description='Run command under lock')
    parser.add_argument('path')
    parser.add_argument('command')
    parser.add_argument('--step', type=float, default=1.0)
    args = parser.parse_args()

    with yt.PingableTransaction() as tx:
        try:
            yt.lock(args.path)
        except yt.YtResponseError as error:
            if error.is_concurrent_transaction_lock_conflict():
                logger.info("Lock conflict (path %s)", args.path)
                return
            raise

        logger.info("Running command")
        proc = subprocess.Popen(args.command, stdout=sys.stdout, stderr=sys.stderr, shell=True, env=os.environ.copy())

        while True:
            if not tx.ping.is_alive():
                logger.error("Pinging thread failed. Terminating command.")
                proc.signal(2)
                time.sleep(args.step)
                proc.terminate()
                break

            if proc.poll() is not None:
                break

            time.sleep(args.step)

if __name__ == "__main__":
    main()

