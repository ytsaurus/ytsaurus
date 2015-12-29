#!/usr/bin/env python

import os
import sys
import copy

_seeds = {
    "9aa642a8a5b73710faad906b75b605eba7521f03": 1144,  # 2011-09-02, branch out from SVN
    "b4cdcdb8aa59b321809072043cd465e4f7cb82ae": 8223,  # 2013-01-17, just a linearization point
    "cec75b5addb26f4c8e5f4d1f403b15216b2355df": 10089,  # stable/0.14
    "b5bdd1e6fc0ecd681412e6f720130ea96a1335f1": 10743,  # stable/0.15
    "e150d55a6eb565a08d0980d15900d5475b6fa055": 11369,  # stable/0.16
    "cada334f4e7d9cef222612748d8510da771f270f": 15000,  # just a round number
    "12859118f3cf3b5c55e5afc4a89c41f888a8a5d6": 11238,  # cut-point
    "8840bcf61983f867ecded657fb028b7c17e05222": 11244,  # cut-point
    "59b5ab489d843d5f7875f2e65b683e23b072ddbb": 14991,  # cut-point
    "d5fdb443b95a11bccc2297134ee63a4a871a8f2e": 11286,  # cut-point
    "d0fbb7d9599e37fead45ab7631d1da3e1ed2e285": 11319,  # cut-point
    "1a333505d8112bb117103942d76f3f6941127e1d": 11352,  # cut-point
    "7b30cc342f279cf27d75a71aa91b7d5f09f6bd4c": 11355,  # cut-point
    "ab638fb2576676b6fc8d64840b1088c717b2f4ae": 11364,  # cut-point
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
    import subprocess
    cache = copy.copy(_seeds)

    revlog = ["HEAD"] + map(lambda x: "^" + x, cache)
    history = subprocess.check_output(["git", "log", "--full-history", "--format=%H %P"] + revlog)

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
