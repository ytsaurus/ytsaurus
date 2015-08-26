#!/usr/bin/env python

import yt.wrapper as yt

import os
from argparse import ArgumentParser
from termcolor import colored

def get_users():
    return yt.get("//sys/users")

def get_admins():
    return set(yt.get("//sys/groups/admins/@members") + yt.get("//sys/groups/devs/@members") + ["root"])

def ok(message):
    print message, "-", colored("OK", "green")

def error(message):
    print message, "-", colored("Error", "red")

def check_tokens_acl():
    is_ok = True
    admins = set(get_admins())
    for user in get_users():
        if user in admins:
            continue
        if yt.check_permission(user, "read", "//sys/tokens")["action"] == "allow":
            error("//sys/tokens are open for " + user)
            is_ok = False
    if is_ok:
        ok("Acl of //sys/tokens")

def check_schemas():
    is_ok = True
    for schema in yt.list("//sys/schemas"):
        acl = os.path.join("//sys/schemas", schema, "@acl")
        try:
            if yt.check_permission("guest", "read", acl)["action"] == "deny":
                error("Read permission denied for " + acl)
                is_ok = False
        except yt.YtError:
            continue
    if is_ok:
        ok("Acls of //sys/schemas/*")

def check_cron_odin():
    users = get_users()
    for user in ["cron", "odin"]:
        if user not in users:
            error("User %s is missing" % user)
        else:
            ok("User %s" % user)

def check_devs_admins():
    is_ok = True
    groups = yt.get("//sys/groups")
    for group in ["devs", "admins"]:
        if group not in groups:
            error("Group %s is missing" % group)
            is_ok = False
    if not "admins" in yt.get("//sys/groups/devs/@member_of"):
        error("Group devs is not member of group admins")
        is_ok = False

    if is_ok:
        ok("Groups devs and admins")


def main():
    parser = ArgumentParser()
    parser.add_argument('--proxy')
    args = parser.parse_args()

    if args.proxy is not None:
        yt.config["proxy"]["url"] = args.proxy

    check_tokens_acl()
    check_schemas()
    check_cron_odin()
    check_devs_admins()

if __name__ == "__main__":
    main()

