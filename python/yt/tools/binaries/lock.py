#!/usr/bin/env python

import yt.logger as logger
import yt.wrapper as yt

import os
import sys
import time
import ctypes
import signal
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

        ctypes.cdll.LoadLibrary("libc.so.6")
        libc = ctypes.CDLL('libc.so.6')
        PR_SET_PDEATHSIG = 1

        logger.info("Running command %s", args.command)
        proc = subprocess.Popen(args.command, stdout=sys.stdout, stderr=sys.stderr, shell=True, env=os.environ.copy(),
                                preexec_fn=lambda: libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM))

        while True:
            if not tx.ping.is_alive():
                logger.error("Pinging thread failed. Terminating command.")
                proc.send_signal(2)
                time.sleep(args.step)
                proc.terminate()
                time.sleep(args.step)
                proc.kill()
                break

            if proc.poll() is not None:
                break

            time.sleep(args.step)

    sys.exit(proc.returncode)

if __name__ == "__main__":
    main()

