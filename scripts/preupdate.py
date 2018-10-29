#!/usr/bin/env python

from subprocess import check_output, Popen, PIPE, STDOUT
from pprint import pprint
from time import sleep
from collections import Counter

import requests
import argparse
import sys
import os

import yt.wrapper

################################################################################

class Colors:
    GREEN = "\033[32m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    END = "\033[0m"

CHANGELOG_FILES = [
    ("yt/server/cell_master/serialize.cpp", ["masters"]),
    ("yt/server/tablet_node/serialize.cpp", ["nodes"]),
    ("yt/server/controller_agent/serialize.cpp", ["controller_agents"]),
    ("yt/server/hive/transaction_supervisor.cpp", ["masters", "nodes"]),
    ("Changelog", ["general"]),
]

PRECAUTION_MESSAGES = {
    "masters": "Master snapshot with --set-read-only must be built",
    "nodes": "Tablet cells must be deleted and created from scratch",
    "controller_agents": "Controller agent snapshot version changed",
}

COMPONENTS = ["nodes", "masters", "schedulers", "controller_agents", "rpc_proxies", "proxies"]

################################################################################
# git helpers

def is_valid_revision(revision):
    return Popen(
        ["/usr/bin/git", "rev-parse", "--verify", revision],
        stderr=STDOUT,
        stdout=open(os.devnull, "w")
    ).wait() == 0

def git_rev_parse(revision):
    return check_output([
        "/usr/bin/git",
        "rev-parse",
        revision
    ]).strip()

def file_exists_at_revision(revision, full_path):
    p = Popen(["/usr/bin/git", "cat-file", "--batch-check"], stdin=PIPE, stdout=PIPE)
    return "missing" not in p.communicate("{}:{}".format(revision, full_path))[0]

def git_diff(from_revision, to_revision, full_path):
    if not file_exists_at_revision(to_revision, full_path):
        print>>sys.stderr, 'WARNING: File "{}" does not exist at target revision {}'.format(full_path, to_revision)
        return []
    elif not file_exists_at_revision(from_revision, full_path):
        lines = check_output([
         "/usr/bin/git",
         "cat-file",
         "blob",
         "{}:{}".format(to_revision, full_path)
        ]).strip().split("\n")

        return [ Colors.GREEN + "+" + line + Colors.END for line in lines ]

    diff = check_output([
        "/usr/bin/git",
        "diff",
        "--color=always",
        "{}:{}".format(from_revision, full_path),
        "{}:{}".format(to_revision, full_path),
        "-U0", # no context
    ]).strip().split("\n")
    if len(diff) > 1:
        assert len(diff) > 5
        return diff[4:]
    else:
        return []

def common_base(revisions):
    if len(revisions) == 1:
        return revisions[0]
    return check_output([
        "/usr/bin/git",
        "merge-base",
        "--",
        revisions[0],
        common_base(revisions[1:])
    ]).strip()

################################################################################
# version helpers

RETRIES=5
RETRY_TIMEOUT=1

def get_versions_from_cluster(cluster_name):
    print>>sys.stderr, "Fetching versions from {}...".format(cluster_name)

    for retry in range(RETRIES):
        headers = {}
        token = yt.wrapper.http_helpers.get_token(client=yt.wrapper.YtClient())
        if token is None:
            print>>sys.stderr, "Cannot fetch YT token, fetching versions as guest."
        else:
            headers["Authorization"] = "OAuth " + token

        rsp = requests.get(
            "https://{}.yt.yandex.net/api/v3/_discover_versions".format(cluster_name),
            headers=headers)
        if not rsp.ok:
            print>>sys.stderr, rsp.json()
            sleep(RETRY_TIMEOUT)
            continue

        components = {}
        for component_name, instances in rsp.json().items():
            component_name = str(component_name)
            versions = set()
            unknown_versions = Counter()
            for host, attr in instances.items():
                try:
                    version = str(attr["version"])
                    if "~" in version:
                        split_version = version.split("~")[1]
                        if split_version in versions or split_version in unknown_versions:
                            continue
                        if is_valid_revision(split_version):
                            versions.add(split_version)
                        else:
                            unknown_versions[version] += 1
                    else:
                        unknown_versions[version] += 1
                except Exception, e:
                    unknown_versions["CANNOT_FETCH_VERSION"] += 1
            components[component_name] = list(versions)

            if unknown_versions:
                total_count = len(instances)
                error_count = sum(unknown_versions.itervalues())
                print>>sys.stderr, "{}WARNING: Some {} versions are not known revisions ({} out of {}):{}".format(
                    Colors.YELLOW,
                    component_name,
                    error_count,
                    total_count,
                    Colors.END)
                for version, count in unknown_versions.iteritems():
                    print>>sys.stderr, "  {} ({} instances)".format(version, count)

        components["masters"] = list(set(components.pop("primary_masters") + components.pop("secondary_masters")))

        print>>sys.stderr

        return components

    print>>sys.stderr, "Cannot fetch versions from {}".format(cluster_name)
    exit(1)

################################################################################

def get_revision_by_components(cluster, components):
    assert len(components) > 0

    versions = get_versions_from_cluster(cluster)
    needed = set()
    for component in components:
        try:
            needed.update(versions[component])
        except:
            print>>sys.stderr, 'WARNING: No such component "{}"'.format(component)
    if not all(is_valid_revision(version) for version in needed):
        print>>sys.stderr, "ERROR: Some versions cannot be fetched or parsed."
        print>>sys.stderr, "Known versions:"
        pprint(versions, sys.stderr)
        print>>sys.stderr, "Consider running\n    {} -f <from_revision> ...".format(sys.argv[0])
        exit(1)

    return common_base(list(needed))

def build_precautions(from_revision, to_revision, show_diff=False, components_to_update=COMPONENTS):
    affected = {}
    general = ""
    total_diff = ""
    for path, components in CHANGELOG_FILES:
        diff = git_diff(from_revision, to_revision, path)
        if not diff:
            continue
        for component in components:
            if component == "general":
                general += "\n".join(diff)
            else:
                affected[component] = affected.get(component, []) + [path]
                if show_diff and component in components_to_update:
                    total_diff += "\n\n" + path + "\n" + "\n".join(diff)

    result = ""
    for component, reason in sorted(affected.items()):
        if component != "general" and component in components_to_update:
            result += Colors.RED + PRECAUTION_MESSAGES[component] + Colors.END +\
                " (reason: diff in {}).\n".format(", ".join(reason))

    if general:
        result += "Consider these special requests (diff in Changelog):\n" + general + "\n"

    if show_diff:
        result += total_diff

    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check necessary actions prior to cluster update.")
    parser.add_argument("-t", "--to-revision", default="HEAD", help="revision to be deployed")
    parser.add_argument("-d", "--show-diff", action="store_true", help="show verbose diff")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("-f", "--from-revision", help="current revision on the cluster")
    g.add_argument("-C", "--cluster", help="cluster name")
    parser.add_argument("-c", "--components",
        default=["all"],
        action="store",
        choices=COMPONENTS + ["all"],
        nargs="*",
        metavar="comp",
        help="components to be updated ('all' or any of {}) (default: all)".format(", ".join(COMPONENTS)))

    args = parser.parse_args()

    components = COMPONENTS if "all" in args.components else args.components

    if args.from_revision is not None:
        from_revision = args.from_revision
    else:
        from_revision = get_revision_by_components(args.cluster, components)

    to_revision = args.to_revision

    for revision in from_revision, to_revision:
        if not is_valid_revision(revision):
            print>>sys.stderr, 'ERROR: Invalid git revision: "{}"'.format(revision)
            exit(1)

    def pretty_revision_name(revision):
        sha1 = git_rev_parse(revision)
        return revision if sha1.startswith(revision) else "{} ({})".format(revision, sha1[:10])

    print "Updating {} from revision {} to {}.\nComponents: {}.".format(
        "cluster" if args.cluster is None else args.cluster,
        pretty_revision_name(from_revision),
        pretty_revision_name(to_revision),
        ", ".join(components))

    message = build_precautions(from_revision, to_revision, show_diff=args.show_diff, components_to_update=components)
    if message:
        print message
        if not args.show_diff:
            print "Run {} with -d flag to see full diff.".format(sys.argv[0])
    else:
        print "Update is probably safe, none of"
        for file in CHANGELOG_FILES:
            print "    {}".format(file[0])
        print "were changed."
