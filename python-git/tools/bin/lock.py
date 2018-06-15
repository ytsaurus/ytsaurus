#!/usr/bin/env python

from yt.common import set_pdeathsig, YT_NULL_TRANSACTION_ID
import yt.logger as logger
import yt.wrapper as yt

import os
import sys
import time
import signal
import argparse
import socket
import subprocess

def main():
    parser = argparse.ArgumentParser(description="Run command under lock")
    parser.add_argument("path")
    parser.add_argument("command", nargs="+")
    parser.add_argument("--proxy")
    parser.add_argument("--step", type=float, default=1.0)
    parser.add_argument("--conflict-exit-code", type=int, default=1)
    parser.add_argument("--set-address", action="store_true", default=False)
    parser.add_argument("--create-lock", action="store_true", default=False, help="Unconditionally creates lock path")
    args = parser.parse_args()

    if args.proxy is not None:
        yt.config["proxy"]["url"] = args.proxy

    if args.create_lock:
        yt.create("map_node", args.path, ignore_existing=True, recursive=True)

    with yt.Transaction(attributes={"title": "yt_lock transaction"}) as tx:
        try:
            yt.lock(args.path)
        except yt.YtResponseError as error:
            if error.is_concurrent_transaction_lock_conflict():
                logger.info("Lock conflict (path %s)", args.path)
                sys.exit(args.conflict_exit_code)
            raise

        if args.set_address:
            if args.address_path is not None:
                with yt.Transaction(transaction_id=YT_NULL_TRANSACTION_ID):
                    yt.set(args.address_path, socket.getfqdn())
            else:
                yt.set(args.path + "/@address", socket.getfqdn())

        def handler():
            tx.__exit__(None, None, None)
            sys.exit(1)

        signal.signal(signal.SIGTERM, lambda signum, frame: handler())

        if len(args.command) == 1:
            # Support for commands like `yt_lock.py ... "sleep 1000"`
            command = args.command[0]
            run_shell = True
        else:
            # Support for commands like `yt_lock.py ... sleep 1000`
            command = args.command
            run_shell = False
        logger.info("Running command %s", args.command)
        proc = subprocess.Popen(command, stdout=sys.stdout, stderr=sys.stderr, shell=run_shell, env=os.environ.copy(),
                                preexec_fn=set_pdeathsig)

        while True:
            if not tx._ping_thread.is_alive():
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

