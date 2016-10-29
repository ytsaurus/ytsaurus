#!/usr/bin/python

import yt.wrapper as yt

import argparse

def main():
    parser = argparse.ArgumentParser(description='Add user.')
    parser.add_argument('name')
    parser.add_argument('--account')
    parser.add_argument('--disk-space', type=int)
    parser.add_argument('--create-homedir', action="store_true", default=False)
    args = parser.parse_args()

    if args.account is not None:
        assert yt.exists("//sys/accounts/" + args.account) == (args.disk_space is None)
    else:
        assert not args.create_homedir

    yt.create("user", attributes={"name": args.name})
    if args.account is not None:
        if not yt.exists("//sys/accounts/" + args.account):
            yt.create("account", attributes={"name": args.account})
            if yt.exists("//sys/media"):
                disk_space_path = "disk_space_per_medium/default"
            else:
                disk_space_path = "disk_space"
            yt.set("//sys/accounts/{}/@resource_limits/{}".format(args.account, disk_space_path), args.disk_space)
        yt.set("//sys/accounts/{}/@acl/end".format(args.account), {"action": "allow", "subjects": [args.name], "permissions": ["use"]})

    if args.create_homedir:
        yt.mkdir("//home/" + args.name)
        yt.set("//home/{}/@account".format(args.name), args.account)
        yt.set("//home/{}/@acl".format(args.name), [{"action": "allow","subjects": [args.name], "permissions": ["write"]}])


if __name__ == "__main__":
    main()

