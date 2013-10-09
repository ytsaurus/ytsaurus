#!/usr/bin/env python

import yt.logger as logger
import yt.wrapper as yt

import sys
import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(description='Run command under lock')
    parser.add_argument('path')
    parser.add_argument('command')
    args = parser.parse_args()

    with yt.Transaction():
        try:
            yt.lock(args.path)
        except yt.YtResponseError as error:
            if error.is_concurrent_transaction_lock_conflict():
                logger.info("Lock conflict (path %s)", args.path)
                return
            raise

        subprocess.call(args.command, stdout=sys.stdout, stderr=sys.stderr, shell=True)

if __name__ == "__main__":
    main()

