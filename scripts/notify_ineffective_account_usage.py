#!/usr/bin/env python

import yt.logger as logger
import yt.wrapper as yt

import smtplib
from email.mime.text import MIMEText

import argparse
from datetime import datetime

now = datetime.now()

def get_age(obj):
    "2012-10-19T11:22:58.190448Z"
    pattern = "%Y-%m-%dT%H:%M:%S"

    time_str = obj.attributes["modification_time"]
    time_str = time_str.rsplit(".")[0]
    return now - datetime.strptime(time_str, pattern)

def get_space(obj):
    return obj.attributes["resource_usage"]["disk_space"]

def send(msg, recipients):
    recipients = map(lambda user: user + "@yandex-team.ru", recipients)
    sender = "info@kant.yt.yandex.net"
    recipients.append("yt-dev-root@yandex-team.ru")
    email_msg = MIMEText(msg)
    email_msg["Subject"] = "Data usage on kant.yt.yandex.net"
    email_msg["From"] = sender
    email_msg["To"] = ", ".join(recipients)
    smtp = smtplib.SMTP('localhost')
    smtp.sendmail(sender, recipients, email_msg.as_string())
    smtp.quit()

def main():
    parser = argparse.ArgumentParser(description='Analyze account space usage and notify responsibles if necessary.')
    parser.add_argument('--min-size', type=int, default=2 * 1024 ** 4)
    parser.add_argument('--min-ratio', type=int, default=0.1)
    parser.add_argument('--days', type=int, default=14)
    parser.add_argument('--top-tables', type=int, default=10)
    parser.add_argument('--exclude', action="append")
    args = parser.parse_args()

    if args.exclude is None:
        args.exclude = []

    inefficiency = {}
    for table in yt.search("/", node_type="table", attributes=["account", "modification_time", "erasure_codec", "resource_usage", "compression_codec"]):
        if get_age(table).days <= args.days:
            continue
        if table.attributes["erasure_codec"] != "none":
            continue
        #if table.attributes["compression_codec"] != "lz4":
        #    continue

        account = table.attributes["account"]
        inefficiency[account] = inefficiency.get(account, []) + [table]

    accounts = yt.get("//sys/accounts", attributes=["resource_limits", "responsibles"])
    accounts_size = dict((str(account), value.attributes["resource_limits"]["disk_space"]) for account, value in accounts.iteritems())

    for account, tables in inefficiency.iteritems():
        if account in args.exclude:
            continue
        space = sum(map(get_space, tables)) / 2
        if space > args.min_size and space > args.min_ratio * accounts_size[account]:
            logger.info("Data in account '%s' stored inefficiently (limit: %d, bad data: %d)", account, accounts_size[account], space)
            send("This is an automatic message, don't reply.\n"
                 "\n"
                 "You are responsible for account '{}' on kant.yt.yandex.net. "
                 "More than 10% of data in your account is stored inefficiently. "
                 "Please compress it or convert it into erasure."
                 "It can be done using yt_convert_to_erasure.py from package yandex-yt-python-tools\n"
                 "\n"
                 "Top {} tables to consider:\n".format(account, args.top_tables) +
                 "\n".join(sorted(tables, reverse=True, key=get_space)[:args.top_tables]),
                 accounts[account].attributes.get("responsibles", []))

        

if __name__ == "__main__":
    main()
