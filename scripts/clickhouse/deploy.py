#!/usr/bin/python2.7

import argparse
import subprocess
import yt.wrapper as yt
import yt.yson as yson
import sys
import os
import os.path
import re
import datetime

def is_multiplatform_bin(kind):
    return kind == "yt-start-clickhouse-clique"

def get_bin_path(src_dir, kind, platform="linux"):
    assert not is_multiplatform_bin(kind) or platform is not None
    platform_dir = platform if is_multiplatform_bin(kind) else ""
    platform_ext = ".exe" if platform == "windows" else ""
    return os.path.join(src_dir, platform_dir, kind + platform_ext)
    
def get_version(src_dir, kind):
    bin_path = get_bin_path(src_dir, kind)
    args = [bin_path, "--version"]
    print >>sys.stderr, "Invoking {} to find out commit".format(args)
    version = subprocess.check_output(args, stderr=subprocess.STDOUT).strip()

    if kind in ("ytserver-clickhouse", "clickhouse-trampoline", "ytserver-log-tailer"):
        yt_commit = re.search("~([0-9a-z]*)", version).groups()[0]
        return (yt_commit, version)
    else:
        yt_commit = re.search("git:([0-9a-z]*)", version).groups()[0]
        return (yt_commit, datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "-" + yt_commit)
        
def main():
    parser = argparse.ArgumentParser(description="Deploy CHYT binary")
    parser.add_argument("--src-dir", default="../../../build-rel/bin", help="Path containing binary to be deployed; by default, ../../../build-rel/bin")
    parser.add_argument("--tags", metavar="TAG", nargs="*", help="tags that will be appended like +tag1+tag2", default=[])
    parser.add_argument("--kind", default="ytserver-clickhouse",
                        choices=["ytserver-clickhouse", "clickhouse-trampoline", "ytserver-log-tailer", "yt-start-clickhouse-clique"],
                        help="Kind of binary to be deployed")
    args = parser.parse_args()

    yt_commit, version = get_version(args.src_dir, args.kind) 

    kind_to_repo_path = {
        "ytserver-clickhouse": "/yt/server/clickhouse_server",
        "clickhouse-trampoline": "/yt/server/clickhouse_trampoline",
        "ytserver-log-tailer": "/yt/server/log_tailer",
        "yt-start-clickhouse-clique": "/scripts/clickhouse",
    }

    repo_path = kind_to_repo_path[args.kind]

    attrs = {
        "executable": True,
        "yt_version": version,
        "yt_version_url": yson.to_yson_type("https://github.yandex-team.ru/yt/yt/tree/" + yt_commit + repo_path, attributes={"_type_tag": "url"}),
    }

    if args.kind == "ytserver-clickhouse":
        ch_version, ch_commit = subprocess.check_output([get_bin_path(args.src_dir, args.kind), "--clickhouse-version"]).strip().split()
        attrs.update({
            "ch_version": ch_version,
            "ch_commit": ch_commit,
            "ch_version_url": yson.to_yson_type("https://github.com/yandex/clickhouse/tree/" + ch_commit, attributes={"_type_tag": "url"}),
        })

    tags = "".join("+" + tag for tag in args.tags)

    platforms = ["linux"]
    if is_multiplatform_bin(args.kind):
        platforms += ["darwin", "windows"]

    for platform in platforms:
        platform_suffix = "." + platform if is_multiplatform_bin(args.kind) else ""
        platform_ext = ".exe" if platform == "windows" else ""
        filename = "{0}{1}-{2}{3}{4}".format(args.kind, platform_suffix, version, tags, platform_ext)
        cypress_path = "//sys/clickhouse/bin/" + filename
        bin_path = get_bin_path(args.src_dir, args.kind, platform=platform)
        print >>sys.stderr, "[{}] Deploying {} to {}".format(platform, bin_path, cypress_path)
        yt.create("file", cypress_path, attributes=attrs, ignore_existing=True)
        yt.write_file(cypress_path, open(bin_path), filename_hint=filename, size_hint=os.stat(bin_path).st_size)

if __name__ == "__main__":
    main()
