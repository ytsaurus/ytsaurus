#!/usr/bin/env python

from yt.wrapper import YtClient

import psutil

import argparse
import logging
import os
import socket


def cleanup_transaction(yt_lock_path, yt_client):
    locks = yt_client.get_attribute(yt_lock_path, "locks")
    logging.info("Locks found: %s", locks)
    if locks and locks[0]["mode"] == "exclusive":
        tx_id = locks[0]["transaction_id"]
        tx_hostname = yt_client.get_attribute("#{}".format(tx_id), "hostname")
        my_hostname = socket.gethostname()
        logging.info("Transaction hostname: %s, my hostname: %s", tx_hostname, my_hostname)
        if tx_hostname == my_hostname:
            logging.info("Aborting transaction")
            yt_client.abort_transaction(tx_id)
        else:
            logging.info("Transaction hostname mismatch, nothing to do")
    elif locks:
        logging.info("First lock is non-exclusive, nothing to do")


def cleanup_processes():
    my_pid = os.getpid()
    to_kill = []
    for process in psutil.process_iter():
        if process.pid == my_pid:
            continue
        try:
            process_exe = process.exe()
        except psutil.NoSuchProcess:
            continue
        except psutil.Error:
            logging.exception("Failed to get executable for pid %s", process.pid)
            continue

        parent_exe = ""
        try:
            parent = process.parent()
            if parent is not None:
                parent_exe = parent.exe()
        except psutil.NoSuchProcess:
            pass
        except psutil.Error:
            logging.exception("Failed to get parent executable for pid %s", process.pid)

        should_kill = lambda exe: "yt_odin" in os.path.basename(exe)  # noqa
        if any(should_kill(exe) for exe in (process_exe, parent_exe)):
            to_kill.append(process)

    for process in to_kill:
        try:
            name = process.name()
        except psutil.Error:
            name = "<unknown>"
        try:
            exe = process.exe()
        except psutil.Error:
            exe = "<unknown>"
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass
        except psutil.Error:
            logging.exception("Failed to kill pid %s (name: %s, exe: %s)", process.pid, name, exe)
        else:
            logging.info("Killed pid %s (name: %s, exe: %s)", process.pid, name, exe)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("yt-proxy", help="YT proxy to use")
    parser.add_argument("yt-token-path", help="path to YT token file")
    parser.add_argument("yt-lock-path", help="Cypress path to the Odin lock")
    return vars(parser.parse_args())


def get_yt_client(yt_proxy, yt_token_path):
    with open(yt_token_path, "r") as f:
        yt_token = f.read().strip()
    return YtClient(token=yt_token, proxy=yt_proxy)


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    args = parse_arguments()
    logging.info("Command line arguments: %s", args)

    cleanup_processes()

    yt_client = get_yt_client(args["yt-proxy"], args["yt-token-path"])
    cleanup_transaction(args["yt-lock-path"], yt_client)


if __name__ == "__main__":
    main()
