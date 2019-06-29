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
    parser.add_argument("--src", default="/home/max42/yt/build-rel/bin/ytserver-clickhouse")
    parser.add_argument("--tags", metavar="TAG", nargs="*", help="tags that will be appended like +tag1+tag2", default=[])
    args = parser.parse_args()

    version = subprocess.check_output([args.src, "--version"]).strip()
    yt_commit = re.match(".*~([0-9a-z]*).*", version).groups()[0]
    ch_version, ch_commit = subprocess.check_output([args.src, "--clickhouse-version"]).strip().split()
    tags = "".join("+" + tag for tag in args.tags)
    filename = "ytserver-clickhouse-{0}{1}".format(version, tags)
    cypress_path = "//sys/clickhouse/bin/" + filename
    print >>sys.stderr, "Deploying {} to {}".format(args.src, cypress_path)  
    yt.create("file", cypress_path, attributes={
        "executable": True, 
        "yt_version": version,
        "ch_version": ch_version,
        "ch_commit": ch_commit,
        "ch_version_url": yson.to_yson_type("https://github.com/yandex/clickhouse/tree/" + ch_commit, attributes={"_type_tag": "url"}),
        "yt_version_url": yson.to_yson_type("https://github.yandex-team.ru/yt/yt/tree/" + yt_commit, attributes={"_type_tag": "url"}),
    }, ignore_existing=True)
    yt.write_file(cypress_path, open(args.src), filename_hint=filename, size_hint=os.stat(args.src).st_size)

if __name__ == "__main__":
    main()
