#!/usr/bin/env python

import os
import sys
import copy

try:
    import subprocess32 as subprocess
except ImportError:
    if sys.version_info[:2] <= (2, 6):
        print >>sys.stderr, "Script may not work properly on python of version <= 2.6 " \
                            "because subprocess32 library is not installed."
    import subprocess

_seeds = {
    "9aa642a8a5b73710faad906b75b605eba7521f03": 1144,  # 2011-09-02, branch out from SVN
    "b4cdcdb8aa59b321809072043cd465e4f7cb82ae": 8223,  # 2013-01-17, just a linearization point
    "cec75b5addb26f4c8e5f4d1f403b15216b2355df": 10089,  # stable/0.14
    "b5bdd1e6fc0ecd681412e6f720130ea96a1335f1": 10743,  # stable/0.15
    "e150d55a6eb565a08d0980d15900d5475b6fa055": 11369,  # stable/0.16
    "cada334f4e7d9cef222612748d8510da771f270f": 15000,  # just a round number
    "e894f3638299b75c9251f5d1fcb81b19fdc9e18d": 1,  # rootless commit
    "c5605d44efde66b90366bdbcc4fd511276f57d3b": 1,  # rootless commit
    "7dcfc452fa0ca5f60dd3755ac154945d61b7b385": 1,  # rootless commit
    "8ca18d293d4df3cd548b6bd874035486094c1506": 1,  # zstd subtree update commit
}


def get_depth_pygit2(path, sha1):
    import pygit2
    cache = copy.copy(_seeds)

    def _impl(commit):
        key = str(commit.id)
        if key not in cache:
            parents = commit.parents
            if len(parents) == 0:
                value = 0
            else:
                value = max(_impl(parent) for parent in commit.parents)
            cache[key] = 1 + value
        return cache[key]

    repo = pygit2.Repository(pygit2.discover_repository(path))
    head = repo.get(sha1)
    return _impl(head)


def get_depth_subprocess(path):
    cache = copy.copy(_seeds)

    history = subprocess.check_output(["git", "log", "--full-history", "--format=%H %P", "HEAD"])
    graph = {}
    for line in history.split("\n"):
        values = line.split()
        if values:
            graph[values[0]] = values[1:]

    def _impl(commit):
        if commit not in cache:
            parents = graph.get(commit, None)
            if not parents:
                raise RuntimeError("Not enough seeds: missing information for commit %s" % commit)
            else:
                depth = max(_impl(parent) for parent in parents)
            cache[commit] = 1 + depth
        return cache[commit]

    head = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip()
    return _impl(head)


def main():
    sys.setrecursionlimit(200000)
    current_path = os.getcwd()

    if len(sys.argv) > 1:
        print get_depth_pygit2(current_path, sys.argv[1])
    else:
        print get_depth_subprocess(current_path)

if __name__ == "__main__":
    main()
