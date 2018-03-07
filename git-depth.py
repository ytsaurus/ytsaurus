#!/usr/bin/env python

import os
import sys

try:
    import subprocess32 as subprocess
except ImportError:
    if sys.version_info[:2] <= (2, 6):
        print >>sys.stderr, "Script may not work properly with Python <= 2.6" \
                            " because subprocess32 library is not installed."
    import subprocess


def get_depth_impl(head, id_fn, parents_fn):
    cache = {}
    stack = [(head, None, False)]
    while stack:
        commit, child, calculated = stack.pop()
        commit_id = id_fn(commit)
        if commit_id in cache:
            calculated = True
        if calculated:
            if child is not None:
                child_id = id_fn(child)
                cache[child_id] = max(
                    cache.get(child_id, 0),
                    cache[commit_id] + 1)
        else:
            stack.append((commit, child, True))
            parents = parents_fn(commit)
            if not parents:
                cache[commit_id] = 0
            else:
                for parent in parents:
                    stack.append((parent, commit, False))
    return cache[id_fn(head)]


def get_depth_pygit2(path, sha1):
    import pygit2

    def id_fn(commit):
        return commit.id

    def parents_fn(commit):
        return commit.parents

    repo = pygit2.Repository(pygit2.discover_repository(path))
    head = repo.get(sha1)
    depth = get_depth_impl(head, id_fn, parents_fn)
    return depth


def get_depth_subprocess(path):
    graph = {}
    history = subprocess.check_output(
        ["git", "log", "--full-history", "--format=%H %P", "HEAD"])
    for line in history.split("\n"):
        values = line.split()
        if values:
            graph[values[0]] = values[1:]

    def id_fn(commit):
        return str(commit)

    def parents_fn(commit):
        return graph.get(commit, None)

    head = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip()
    depth = get_depth_impl(head, id_fn, parents_fn)
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
