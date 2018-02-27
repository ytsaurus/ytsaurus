#!/usr/bin/env python

from __future__ import print_function

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "git-svn"))

from git_svn_lib import (Git, Svn, init_git_svn, fetch_git_svn, push_git_svn, pull_git_svn,
                         check_git_version, check_git_working_tree, check_svn_url)

import logging
import argparse

ARCADIA_URL = "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/yt/python/yt"

GIT_SVN_REMOTE_NAME = "arcadia_yt_python_yt"

def configure_logging():
    logger = logging.getLogger("Yt.GitSvn")
    logger.propagate = False
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)-15s\t%(levelname)s\t%(message)s")
        handler.setFormatter(formatter)
        logger.handlers.append(handler)

def main():
    parser = argparse.ArgumentParser(description="Scripts performs Git <-> SVN (Arcadia) synchronization. "
                                                 "Handle with care!")

    subparsers = parser.add_subparsers(metavar="command", dest="command")
    # Init
    subparsers.add_parser("init", help="initializes git-svn remotes in current repository")
    # Fetch
    subparsers.add_parser("fetch", help="fetches all fresh revisions from svn repository to git-svn repository")
    # Push
    push_parser = subparsers.add_parser("push", help="pushes difference between svn and git tree to svn repository")
    push_parser.add_argument("--dry-run", action="store_true", help="do not perform push, just print all commands to console")
    push_parser.add_argument("--force", action="store_true", help="perform force commit")
    push_parser.add_argument("--review", "-r", nargs="?", default=None,
                             help="review commit (you may provide the review id)")
    # Pull
    pull_parser = subparsers.add_parser("pull", help="pulls changes from svn to git")
    pull_parser.add_argument("--revision", help="revision to pull")

    args = parser.parse_args()

    git = Git(repo=os.getcwd())
    svn = Svn()
    check_git_version(git)
    check_git_working_tree(git)
    check_svn_url(svn, ARCADIA_URL)

    if args.command == "init":
        init_git_svn(git, GIT_SVN_REMOTE_NAME, ARCADIA_URL)
    elif args.command == "fetch":
        fetch_git_svn(git, svn, GIT_SVN_REMOTE_NAME, one_by_one=True)
    elif args.command == "push":
        push_git_svn(
            git,
            svn,
            ARCADIA_URL,
            GIT_SVN_REMOTE_NAME,
            "yt/",
            "yt/python/yt/",
            review=args.review,
            force=args.force,
            dry_run=args.dry_run)
    elif args.command == "pull":
        pull_git_svn(
            git,
            svn,
            ARCADIA_URL,
            GIT_SVN_REMOTE_NAME,
            "yt/",
            "yt/python/yt/",
            revision=args.revision)
    else:
        assert False, "Unknown command " + args.command

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print('Exception "{0}": {1}'.format(err.__class__.__name__, str(err)))
        sys.exit(1)
