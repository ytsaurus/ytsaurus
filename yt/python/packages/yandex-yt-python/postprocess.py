import glob
import sys
import argparse
import logging

import requests

from helpers import execute_command, get_version


def parse_arguments():
    parser = argparse.ArgumentParser(description="Build & upload package egg and create conductor ticket")
    parser.add_argument("--python-binary", default=sys.executable, help="Path to python binary")
    parser.add_argument("--create-conductor-ticket", action="store_true", default=False)
    return parser.parse_args()


def create_conductor_ticket(version):
    cookies = {"conductor_auth": "419fb75155c27d44f1d110ec833400fa"}
    params = {
        "package": "yandex-yt-python",
        "version": version,
        "branch": "unstable",
    }

    response = requests.get(
        "http://c.yandex-team.ru/api/generator/package_version_ticket",
        params=params,
        cookies=cookies)

    if not response.ok:
        escaped_content = response.content.replace(b"\n", b"\\n")
        logging.error("Response: {}".format(escaped_content))
        response.raise_for_status()

    if "unstable" in response.json():
        logging.info("Conductor ticket for this version already exists: {}".format(response.json()["unstable"]))
        return

    _, changelog, _ = execute_command(["dpkg-parsechangelog"], capture_output=True)

    def truncate(message):
        if len(message) < 1500:
            return message
        truncated_notice = "<message truncated>"
        return message[:1500 - len(truncated_notice)] + truncated_notice

    params = {
        "package[0]": "yandex-yt-python",
        "version[0]": version,
        "ticket[branch]": "unstable",
        "ticket[comment]": truncate(changelog),
    }

    requests.post(
        "http://c.yandex-team.ru/auth_update/ticket_add",
        params=params,
        cookies=cookies)

    if not response.ok:
        escaped_content = response.content.replace(b"\n", b"\\n")
        logging.error("Response: {}".format(escaped_content))
        response.raise_for_status()

    logging.info("Ticket created: {}".format(response.json()))


def main(args=None):
    if not args:
        args = parse_arguments()

    version = get_version()

    if args.create_conductor_ticket:
        logging.info("Creating conductor ticket...")
        create_conductor_ticket(version)
    else:
        logging.info("Skipping creating conductor ticket")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
