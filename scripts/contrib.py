#!/usr/bin/env python

import argparse
import os
import json
import sys
import subprocess

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
DATABASE_PATH = os.path.join(SCRIPT_PATH, "contrib.json")
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_PATH, ".."))


def die(msg, *args):
    print "ERROR: " + (msg % args)
    sys.exit(1)


def db_read():
    if os.path.exists(DATABASE_PATH):
        with open(DATABASE_PATH, "r") as handle:
            return json.load(handle)
    else:
        return {}


def db_write(data):
    with open(DATABASE_PATH, "w") as handle:
        json.dump(data, handle, sort_keys=True, indent=2)


def db_get(key):
    return db_read().get(key, None)


def db_put(key, value):
    data = db_read()
    data[key] = value
    db_write(data)


def do_add(path, repository, branch, ref):
    if os.path.exists(os.path.join(PROJECT_PATH, path)):
        die("Path '%s' already exist", path)

    spec = db_get(path)
    if spec is not None:
        die("Path '%s' already registered as subtree", path)

    ref = ref or branch
    assert ref
    subprocess.check_call(
        [os.path.join(PROJECT_PATH, "scripts", "git-subtree.sh"),
         "add", "--squash", "--prefix", path, repository, ref],
        cwd=PROJECT_PATH)

    spec = {"repository": repository, "branch": branch}
    db_put(path, spec)


def do_pull(path, ref):
    spec = db_get(path)
    if spec is None:
        die("Path '%s' is not registered", path)

    repository = spec["repository"]
    branch = spec.get("branch", None)

    ref = ref or branch
    assert ref
    subprocess.check_call(
        [os.path.join(PROJECT_PATH, "scripts", "git-subtree.sh"),
         "pull", "--squash", "--prefix", path, repository, ref],
        cwd=PROJECT_PATH)


def do_push(path, ref):
    spec = db_get(path)
    if spec is None:
        die("Path '%s' is not registered", path)

    repository = spec["repository"]
    branch = spec.get("branch", None)

    ref = ref or branch
    assert ref
    subprocess.check_call(
        [os.path.join(PROJECT_PATH, "scripts", "git-subtree.sh"),
         "push", "--annotate", "yt", "--prefix", path, repository, ref],
        cwd=PROJECT_PATH)


def main():
    def add_thunk(args):
        do_add(args.path, args.repository, args.branch, args.ref)

    def pull_thunk(args):
        do_pull(args.path, args.ref)

    def push_thunk(args):
        do_push(args.path, args.ref)

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_add = subparsers.add_parser(
        "add", help="add new repo")
    parser_add.set_defaults(func=add_thunk)
    parser_add.add_argument("path")
    parser_add.add_argument("repository")
    parser_add.add_argument("--branch")
    parser_add.add_argument("--ref")

    parser_pull = subparsers.add_parser(
        "pull", help="pull all changes from repo master branch")
    parser_pull.add_argument("path")
    parser_pull.add_argument("--ref")
    parser_pull.set_defaults(func=pull_thunk)

    parser_push = subparsers.add_parser(
        "push", help="push new changes to repo master branch")
    parser_push.add_argument("path")
    parser_push.add_argument("--ref")
    parser_push.set_defaults(func=push_thunk)

    args = parser.parse_args()
    args.path = os.path.relpath(os.path.realpath(args.path), PROJECT_PATH)
    args.func(args)


if __name__ == "__main__":
    main()
