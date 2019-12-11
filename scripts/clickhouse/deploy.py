#!/usr/bin/python2.7

import argparse
import subprocess
import yt.wrapper as yt
import yt.yson as yson
import sys
import os
import re

def main():
    parser = argparse.ArgumentParser(description="Deploy CHYT binary")
    parser.add_argument("--src", help="Path of binary to be deployed; by default one from /home/max42/yt/build-rel/bin is taken")
    parser.add_argument("--tags", metavar="TAG", nargs="*", help="tags that will be appended like +tag1+tag2", default=[])
    parser.add_argument("--kind", default="ytserver-clickhouse",
                        choices=["ytserver-clickhouse", "clickhouse-trampoline", "ytserver-log-tailer"],
                        help="Kind of binary to be deployed")
    args = parser.parse_args()

    src = args.src or ("/home/max42/yt/build-rel/bin/" + args.kind)

    print >>sys.stderr, "Invoking {} to find out commit".format([src, "--version"])
    version = subprocess.check_output([src, "--version"]).strip()
    yt_commit = re.match(".*~([0-9a-z]*).*", version).groups()[0]

    attrs = {
        "executable": True,
        "yt_version": version,
        "yt_version_url": yson.to_yson_type("https://github.yandex-team.ru/yt/yt/tree/" + yt_commit + "/yt/server/clickhouse_server", attributes={"_type_tag": "url"}),
    }

    if args.kind == "ytserver-clickhouse":
        ch_version, ch_commit = subprocess.check_output([src, "--clickhouse-version"]).strip().split()
        attrs.update({
            "ch_version": ch_version,
            "ch_commit": ch_commit,
            "ch_version_url": yson.to_yson_type("https://github.com/yandex/clickhouse/tree/" + ch_commit, attributes={"_type_tag": "url"}),
        })

    tags = "".join("+" + tag for tag in args.tags)
    filename = "{0}-{1}{2}".format(args.kind, version, tags)
    cypress_path = "//sys/clickhouse/bin/" + filename
    print >>sys.stderr, "Deploying {} to {}".format(src, cypress_path)
    yt.create("file", cypress_path, attributes=attrs, ignore_existing=True)
    yt.write_file(cypress_path, open(src), filename_hint=filename, size_hint=os.stat(src).st_size)

if __name__ == "__main__":
    main()
