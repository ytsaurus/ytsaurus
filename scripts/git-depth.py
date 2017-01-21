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
    "28becff27082ec4e50ee844fb31bbb6ac1a2a05c": 10000,  # round number to prune search space
    "b13e0b9a25f907fc06157f0b1dbdf12e1f25777d": 10000,  # round number to prune search space
    "cada334f4e7d9cef222612748d8510da771f270f": 15000,  # round number to prune search space
    "71eba9ebd4f582603d0f87e5070632c6049c719e": 20000,  # round number to prune search space
    "b5f2820594ec6a5859dd24fe7fb26d4936f16f38": 20000,  # round number to prune search space
    "e894f3638299b75c9251f5d1fcb81b19fdc9e18d": 1,  # rootless commit
    "c5605d44efde66b90366bdbcc4fd511276f57d3b": 1,  # rootless commit
    "7dcfc452fa0ca5f60dd3755ac154945d61b7b385": 1,  # rootless commit
    "7842db634296e5d70cce4a6cdb7114f921ca1081": 1,  # rootless commit
    "fa3a02ad37c5a7bac734051615442a0f5f96e61a": 1,  # rootless commit
    "8ca18d293d4df3cd548b6bd874035486094c1506": 1,  # zstd subtree update commit
    "9b6843d82eff222a9ae20af284eb2d9a5d291588": 1,  # zstd subtree update commit
    "d1eb750837a4538d301c59397a4b8d4b1ceda14b": 1,  # 18.4 init commit of YT in Arcadia
}


def get_depth_pygit2(path, sha1):
    import pygit2
    cache = copy.copy(_seeds)

    def _impl(commit):
        stack = [(commit, None, False)]
        while stack:
            commit, child, calculated = stack.pop()
            if commit.id in cache:
                calculated = True
            if calculated:
                if child is not None:
                    cache[child.id] = max(cache.get(child.id, 0), cache[commit.id] + 1)
            else:
                stack.append((commit, child, True))
                parents = commit.parents
                if not parents:
                    cache[commit.id] = 0
                else:
                    for parent in parents:
                        stack.append((parent, commit, False))
        return cache[commit.id]

    repo = pygit2.Repository(pygit2.discover_repository(path))
    head = repo.get(sha1)
    depth = _impl(head)
    return depth


def get_depth_subprocess(path):
    cache = copy.copy(_seeds)

    history = subprocess.check_output(["git", "log", "--full-history", "--format=%H %P", "HEAD"])
    graph = {}
    for line in history.split("\n"):
        values = line.split()
        if values:
            graph[values[0]] = values[1:]

    def _impl(commit):
        stack = [(commit, None, False)]
        while stack:
            commit, child, calculated = stack.pop()
            if commit in cache:
                calculated = True
            if calculated:
                if child is not None:
                    cache[child] = max(cache.get(child, 0), cache[commit] + 1)
            else:
                stack.append((commit, child, True))
                parents = graph.get(commit, None)
                if not parents:
                    cache[commit] = 0
                else:
                    for parent in parents:
                        stack.append((parent, commit, False))
        return cache[commit]

    head = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip()
    depth = _impl(head)
    return depth


def main():
    sys.setrecursionlimit(32768)
    current_path = os.getcwd()

    if len(sys.argv) > 1:
        print get_depth_pygit2(current_path, sys.argv[1])
    else:
        print get_depth_subprocess(current_path)

if __name__ == "__main__":
    main()
