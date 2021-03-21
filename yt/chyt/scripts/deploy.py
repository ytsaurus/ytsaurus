#!/usr/bin/python3

import argparse
import subprocess
import yt.wrapper as yt
from yt.wrapper.config import get_config
import yt.yson as yson
import sys
import os
import os.path
import re
import datetime


args: argparse.Namespace


def is_multiplatform_bin(kind):
    return kind == "chyt"


def get_bin_path(src_dir, kind, platform="linux"):
    assert not is_multiplatform_bin(kind) or platform is not None
    platform_dir = platform if is_multiplatform_bin(kind) else ""
    platform_ext = ".exe" if platform == "windows" else ""
    return os.path.join(src_dir, platform_dir, kind + platform_ext)


def to_yson_url(url):
    return yson.to_yson_type(url, attributes={"_type_tag": "url"})


# Relative to arcadia:/yt/yt or to github:/yt.
COMPONENT_REPOSITORY_ROOTS = {
    "ytserver-clickhouse": "server/clickhouse_server",
    "clickhouse-trampoline": "server/clickhouse_trampoline",
    "ytserver-log-tailer": "server/log_tailer",
    "chyt": "scripts/clickhouse",
    "ytserver-proxy": "server/rpc_proxy",
}

def format_component_url(version, commit):
    global args
    if version < "20.":
        template = "https://github.yandex-team.ru/yt/yt/tree/{commit}/yt/{root}"
    else:
        template = "https://a.yandex-team.ru/arc_vcs/history/yt/yt/{root}/?from={commit}"
    return to_yson_url(template.format(commit=commit, root=COMPONENT_REPOSITORY_ROOTS[args.kind]))

def invoke(extra_args):
    if not isinstance(extra_args, list):
        extra_args = [extra_args]
    bin_path = get_bin_path(args.src_dir, args.kind)
    invoke_args = [bin_path] + extra_args
    print("Invoking {} to find out commit".format(invoke_args), file=sys.stderr)
    return list(map(str.strip, subprocess.check_output(invoke_args, stderr=subprocess.STDOUT).decode("ascii").strip().split()))


def get_attributes():
    global args

    result = {}

    if args.kind in ("ytserver-clickhouse", "ytserver-log-tailer", "ytserver-proxy"):
        try:
            yt_version, = invoke("--yt-version")
        except subprocess.CalledProcessError:
            yt_version, = invoke("--version")

        yt_commit = re.search("~([0-9a-z]*)", yt_version).groups()[0]
        result.update({
            "yt_version": yt_version,
            "yt_verion_url": format_component_url(yt_version, yt_commit),
        })
    if args.kind == "ytserver-clickhouse":
        ch_version = invoke("--clickhouse-version")
        result.update({
            "ch_version": ch_version,
        })
        chyt_version, = invoke("--version")
        chyt_commit = re.search("~([0-9a-z]*)", chyt_version).groups()[0]
        result.update({
            "version": chyt_version,
            "commit": chyt_commit,
            "version_url": format_component_url(chyt_version, chyt_commit)
        })
    if args.kind == "clickhouse-trampoline":
        version, = invoke("--version")
        commit = re.search("~([0-9a-z]*)", version).groups()[0]
        result.update({
            "version": version,
            "commit": commit,
            "version_url": format_component_url(chyt_version, commit)
        })
    if args.kind == "chyt":
        _1, _2, version, commit  = invoke("--version")
        commit = commit[1:-1]
        result.update({
            "version": version,
            "commit": commit,
        })

    return result


# This version becomes part of binary name in Cypress.
def get_cypress_version(attributes):
    if args.kind in ("ytserver-log-tailer", "ytserver-proxy", "clickhouse-trampoline"):
        return attributes["yt_version"]
    elif args.kind in ("ytserver-clickhouse",):
        return attributes["version"]
    elif args.kind in ("chyt",):
        return attributes["version"] + "~" + attributes["commit"]
    else:
        # We want lexicographical ordering to be chronological.
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "-" + attributes["version"]


def main():
    parser = argparse.ArgumentParser(description="Deploy CHYT binary")
    parser.add_argument("--src-dir", help="Path containing binary to be deployed", required=True)
    parser.add_argument("--tags", metavar="TAG", nargs="*", help="tags that will be appended like +tag1+tag2", default=[])
    parser.add_argument("--kind", default="ytserver-clickhouse",
                        choices=["ytserver-clickhouse", "clickhouse-trampoline", "ytserver-log-tailer", "chyt", "ytserver-proxy"],
                        help="Kind of binary to be deployed")
    parser.add_argument("--dry-run", action="store_true", help="Skip upload part")
    global args
    args = parser.parse_args()

    attributes = get_attributes()
    attributes.update({
        "executable": True,
        "vital": False,
    })

    cypress_version = get_cypress_version(attributes)

    print("Cypress version:", cypress_version, file=sys.stderr)
    print("Cypress attributes:", yson.dumps(attributes, yson_format="pretty").decode("ascii"), file=sys.stderr)

    if ".0~" in cypress_version and args.kind == "ytserver-clickhouse":
        print("Binary lacks version patch, make sure to rebuild release binary with -DYT_VERSION_PATCH=<patch>; \n"
              "Patch should be an integer greater by one then the last release patch in the same branch",
              file=sys.stderr)
        exit(1)

    tags = "".join("+" + tag for tag in args.tags)

    platforms = ["linux"]
    if is_multiplatform_bin(args.kind):
        platforms += ["darwin", "windows"]

    upload_client = yt.YtClient(config=get_config(client=None))
    upload_client.config["proxy"]["content_encoding"] = "identity"
    upload_client.config["write_parallel"]["enable"] = False
    upload_client.config["write_retries"]["chunk_size"] = 4 * 1024**3

    for platform in platforms:
        platform_suffix = "." + platform if is_multiplatform_bin(args.kind) else ""
        platform_ext = ".exe" if platform == "windows" else ""
        filename = "{0}{1}-{2}{3}{4}".format(args.kind, platform_suffix, cypress_version, tags, platform_ext)
        cypress_path = "//sys/bin/{}/{}".format(args.kind, filename)
        bin_path = get_bin_path(args.src_dir, args.kind, platform=platform)
        print("[{}] Deploying {} to {}".format(platform, bin_path, cypress_path), file=sys.stderr)
        if not args.dry_run:
            upload_client.create("file", cypress_path, attributes=attributes, force=True)
            upload_client.write_file(cypress_path,
                                     open(bin_path, 'rb'),
                                     filename_hint=filename,
                                     file_writer={
                                         "enable_early_finish": True,
                                         "min_upload_replication_factor": 1,
                                         "upload_replication_factor": 3,
                                         "send_window_size": 4 * 1024**3,
                                         "sync_on_close": False,
                                     })

if __name__ == "__main__":
    main()
