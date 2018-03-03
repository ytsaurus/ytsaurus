#!/usr/bin/env python

"""
## Introduction

This script provides the assistance functions to synchronize YT in Arcadia and in GitHub.

The main source tree is composed of different directories with two main synchronization strategies.

First, `contrib/`, `library/` and `util/` are mirrored from Arcadia to GitHub, and YT pins
a snapshot + a patchset. These directories are mirrored with `git-svn` to the `upstream` branch in GitHub.
The patchset is typically applied on the `master` branch, and the `master` branch is a fork of
the `upstream` branch.  When updating these directories, one usually pins the old `master` branch
under the name `old/YYYY_MM_DD__HH_mm_ss` (to make sure that all the old commits are reachable)
and rebases the `master` branch on top of the `upstream` branch.

Second, `yt/` is mirrored from GitHub to Arcadia as a snapshot. `git-svn` is used to commit the appropriate
subtree into SVN, and the lineage is preserved with the commit marks. Namely, every push commit in SVN
contains the textual reference to the original Git commit. This information is used to properly pull changes
from SVN to Git.

## Glossary

**(Git) Commit**. Changeset for a repository with lineage information. Identified by a SHA1 hash.

**(Git) Reference**. Named pointed to a particular commit. Fully qualified reference starts with `refs/`.
Examples: `HEAD`, `origin/master`, `refs/remotes/origin/master`, `branch`, `refs/heads/branch`.

**(Svn) Revision**. Changeset for a repository. Identified by a natural number. Revisions are totally ordered.

## How To Add New Arcadia Submodule

(1) Create a Git repository on GitHub.
(2) Add submodule to list below (search for: `SUBMODULES`)
(3) Call `git submodule add`
(4) Call this script with `submodule-init` and `submodule-fetch` commands
(5) Call `git submodule add` (again!)# {}
(6) Create subdirectory in `cmake/` and write `CMakeLists.txt`
(7) Add `add_subdirectory` in root `CMakeLists.txt`
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "git-svn"))

from git_svn_lib import (Git, Svn, CheckError, make_remote_ref, abbrev, git_dry_run, make_head_ref,
                         get_all_symbolic_refs, check_git_version, check_git_working_tree, check_svn_url,
                         extract_git_svn_revision_to_commit_mapping_as_list,
                         extract_git_svn_revision_to_commit_mapping_as_dict,
                         init_git_svn, fetch_git_svn, push_git_svn, pull_git_svn)

import os
import argparse
import logging
import re
import time

from collections import namedtuple

logger = logging.getLogger("Yt.GitSvn")

ARC = "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"
GH = "git@github.yandex-team.ru:yt"

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_PATH, ".."))

def get_abi_major_minor_from_git_branch(git):
    ref = git.call("rev-parse", "--abbrev-ref", "HEAD").strip()
    match = re.match(r"^(?:pre)?stable/(\d+).(\d+)$", ref)
    if not match:
        raise CheckError("Current branch must be either 'prestable/X.Y' or 'stable/X.Y'")
    major, minor = map(int, [match.group(1), match.group(2)])
    return major, minor

def stitch_git_svn(git, ref, svn_remote, svn_url):
    """
    Stitches the committed and de-facto SVN histories together.

    Rarely (but still!) SVN history gets rewritten.
    While the commit patches remain the same, their commit messages may vary.
    We assume that the latest remote history is the correct one,
    and enumerate all the commits that are now not reachable from the remote branch reference.
    """
    logger.debug(
        "Stitching commits reachable from '%s' and commits in git-svn remote '%s'",
        ref,
        svn_remote)

    head_mapping = extract_git_svn_revision_to_commit_mapping_as_list(
        git, svn_url, ref)
    remote_mapping = extract_git_svn_revision_to_commit_mapping_as_dict(
        git, svn_url, make_remote_ref(svn_remote))

    for revision, head_commit in head_mapping:
        remote_commit = remote_mapping.get(revision, None)
        if not remote_commit:
            logger.warning(
                "SVN commit for revision %s (%s) is present in history '%s' but missing in remote '%s'",
                revision, head_commit, ref, svn_remote)
            continue
        if head_commit != remote_commit:
            replaced_commit = git.resolve_ref("refs/replace/%s" % head_commit)
            if replaced_commit:
                if replaced_commit != remote_commit:
                    raise CheckError("Git is screwed up badly ://")
            else:
                logger.warn(
                    "SVN commit for revision %s was rewritten: %s -> %s",
                    revision, head_commit, remote_commit)
                git.call("replace", head_commit, remote_commit)

    git.call("pack-refs", "--all")

def action_init(ctx, args):
    init_git_svn(ctx.git, ctx.arc_git_remote, ctx.arc_url)

def action_fetch(ctx, args):
    fetch_git_svn(ctx.git, ctx.svn, ctx.arc_git_remote, one_by_one=True)

def action_stitch(ctx, args):
    stitch_git_svn(ctx.git, "HEAD", ctx.arc_git_remote, ctx.arc_url)

def action_push(ctx, args):
    push_git_svn(
        ctx.git,
        ctx.svn,
        ctx.arc_url,
        ctx.arc_git_remote,
        "yt/",
        "yt/%s/" % ctx.abi,
        review=args.review,
        force=args.force,
        dry_run=not args.yes)

def action_pull(ctx, args):
    pull_git_svn(
        ctx.git,
        ctx.svn,
        ctx.arc_url,
        ctx.arc_git_remote,
        "yt/",
        "yt/%s/" % ctx.abi,
        revision=args.revision)

def snapshot_main(args):
    class Ctx(namedtuple("Ctx", ["git", "svn", "abi_major", "abi_minor"])):
        @property
        def abi(self):
            return "%s_%s" % (self.abi_major, self.abi_minor)

        @property
        def arc_url(self):
            return "%s/yt/%s/yt" % (ARC, self.abi)

        @property
        def arc_git_remote(self):
            return "arcadia_svn_%s" % (self.abi)

    git = Git(repo=PROJECT_PATH)
    svn = Svn()

    if args.check_git_version:
        check_git_version(git)
    if args.check_git_working_tree:
        check_git_working_tree(git)
    abi_major, abi_minor = get_abi_major_minor_from_git_branch(git)

    ctx = Ctx(git, svn, abi_major, abi_minor)
    if args.check_svn_url:
        check_svn_url(ctx.svn, ctx.arc_url)

    args.action(ctx, args)

def action_submodule_init(ctx, args):
    init_git_svn(ctx.git, ctx.arc_git_remote, ctx.arc_url)

    remotes = ctx.git.call("remote").split()
    if ctx.gh_git_remote in remotes:
        ctx.git.call("remote", "remove", ctx.gh_git_remote)
    ctx.git.call("remote", "add", "-m", ctx.gh_arc_branch, ctx.gh_git_remote, ctx.gh_url)
    ctx.git.call("remote", "update", ctx.gh_git_remote)
    ctx.git.call(
        "config", "--local", "remote.%s.push" % ctx.gh_git_remote,
        "+%s:%s" % (ctx.arc_git_remote_ref, make_head_ref(ctx.gh_arc_branch)))
    ctx.git.call(
        "config", "--local", "remote.%s.mirror" % ctx.gh_git_remote,
        "false")

    arc_branch_ref = ctx.gh_git_remote_ref + "/" + ctx.gh_arc_branch
    arc_branch_commit = ctx.git.resolve_ref(arc_branch_ref)
    if arc_branch_commit:
        ctx.git.call("update-ref", ctx.arc_git_remote_ref, arc_branch_commit)


def action_submodule_fetch(ctx, args):
    fetch_git_svn(ctx.git, ctx.svn, ctx.arc_git_remote, one_by_one=False)
    ctx.git.call("fetch", ctx.gh_git_remote)
    ctx.git.call("remote", "prune", ctx.gh_git_remote)

    old_head = ctx.git.resolve_ref(ctx.gh_git_remote_ref + "/" + ctx.gh_arc_branch)
    new_head = ctx.git.resolve_ref(ctx.arc_git_remote_ref)

    assert new_head is not None

    push = False
    if old_head is None:
        push = True
    elif old_head == new_head:
        logger.info(
            "'%s' is up-to-date: %s is latest commit in '%s'",
            ctx.name, abbrev(old_head), ctx.arc_git_remote)
    elif ctx.git.is_ancestor(old_head, new_head):
        push = True
    else:
        logger.warning(
            "Upstream has diverged in '%s'! %s is not parent for %s!",
            ctx.name, new_head, old_head)

    if push:
        logger.info(
            "Updating '%s': %s -> %s",
            ctx.name, abbrev(old_head), abbrev(new_head))
        ctx.git.call("push", ctx.gh_git_remote, "%s:%s" % (new_head, make_head_ref(ctx.gh_arc_branch)))


def action_submodule_stitch(ctx, args):
    if ctx.name in ["contrib-libs-protobuf"]:
        return

    refs = get_all_symbolic_refs(ctx.git)

    for _, ref in refs:
        if not ref.startswith(ctx.gh_git_remote_ref):
            continue
        stitch_git_svn(ctx.git, ref, ctx.arc_git_remote, ctx.arc_url)
    stitch_git_svn(ctx.git, "HEAD", ctx.arc_git_remote, ctx.arc_url)


def check_pinning_required(git, ref, prefixes):
    for _, symbolic_ref in get_all_symbolic_refs(git):
        if not any(symbolic_ref.startswith(prefix) for prefix in prefixes):
            continue
        if git.is_ancestor(ref, symbolic_ref):
            return True, symbolic_ref
    return False, None


def action_submodule_pin(ctx, args):
    head_ref = args.commit  # assume references are passed via args
    head_commit = ctx.git.resolve_ref(args.commit)

    logger.info("Pinning commits that are reachable from '%s' (%s)", head_ref, abbrev(head_commit))

    holder_prefixes = [
        ctx.gh_git_remote_ref + "/old",
        ctx.gh_git_remote_ref + "/" + ctx.gh_arc_branch]
    held, holder_ref = check_pinning_required(ctx.git, head_commit, holder_prefixes)

    if held:
        logger.info("Commit %s is already held by reference '%s'", abbrev(head_commit), holder_ref)
    else:
        pin = "old/" + time.strftime("%Y_%m_%d__%H_%M_%S")
        ctx.git.call("push", ctx.gh_git_remote, "%s:%s" % (head_commit, make_head_ref(pin)))


def action_submodule_fast_pull(ctx, args):
    old_head = ctx.git.resolve_ref("HEAD")
    new_head = ctx.git.resolve_ref(ctx.arc_git_remote_ref)

    assert old_head is not None
    assert new_head is not None

    if old_head == new_head:
        logger.info(
            "'%s' is up-to-date: %s is latest commit in '%s'",
            ctx.name, abbrev(old_head), ctx.arc_git_remote)
    elif ctx.git.is_ancestor(new_head, old_head):
        logger.info(
            "'%s' is up-to-date: %s superseedes latest commit %s in '%s'",
            ctx.name, abbrev(old_head), abbrev(new_head), ctx.arc_git_remote)
    elif ctx.git.is_ancestor(old_head, new_head):
        logging.info(
            "Checking out '%s': %s -> %s",
            ctx.name, abbrev(old_head), abbrev(new_head))
    else:
        if ctx.git.test("rebase", "--quiet", new_head):
            rebased_head = ctx.git.resolve_ref("HEAD")
            logger.info(
                "'%s' has been updated with the rebase: %s -> %s over %s",
                ctx.name, abbrev(old_head), abbrev(rebased_head), abbrev(new_head))
        else:
            ctx.git.call("rebase", "--abort")
            logger.warning("Manual pull is required in '%s'!", ctx.name)


def git_dry_run(flag, ctx, *args):
    if flag:
        ctx.git.call(*args, capture=False)
    else:
        def _escape(s):
            if re.match(r"^[a-zA-Z0-9_-]*$", s):
                return s
            if "'" in s:
                return '"' + s.replace('"', '\\"') + '"'
            else:
                return "'" + s + "'"
        print("git " + " ".join(map(_escape, args)))


def action_submodule_fast_push(ctx, args):
    old_head = ctx.git.resolve_ref(ctx.gh_git_remote_ref + "/" + ctx.gh_branch)
    new_head = ctx.git.resolve_ref("HEAD")

    assert new_head is not None

    push = False
    if old_head is None:
        push = True
    elif old_head == new_head:
        logger.info(
            "'%s' is up-to-date: %s is latest commit in '%s/%s'",
            ctx.name, abbrev(old_head), ctx.gh_git_remote, ctx.gh_branch)
    else:
        holder_prefixes = [
            ctx.gh_git_remote_ref + "/old",
            ctx.gh_git_remote_ref + "/" + ctx.gh_arc_branch]
        held, holder_ref = check_pinning_required(ctx.git, old_head, holder_prefixes)

        if held or ctx.git.is_ancestor(old_head, new_head):
            push = True
        else:
            logger.warning("Manual push is required in '%s'!", ctx.name)

    if push:
        logger.info(
            "Pushing '%s' to '%s/%s': %s -> %s",
            ctx.name, ctx.gh_git_remote, ctx.gh_branch, abbrev(old_head), abbrev(new_head))
        git_dry_run(
            not args.yes, ctx,
            "push", "--force", ctx.gh_git_remote, "%s:%s" % (new_head, make_head_ref(ctx.gh_branch)))


def submodule_main(args):
    class Ctx(namedtuple("Ctx", ["git", "svn", "name"])):
        @property
        def splitname(self):
            assert "!" not in self.name
            return self.name.replace("-", "!").replace("!!", "-").split("!")

        @property
        def relpath(self):
            return os.path.join(*self.splitname)

        @property
        def arc_url(self):
            return "%s/%s" % (ARC, self.relpath)

        @property
        def arc_git_remote(self):
            return "arcadia"

        @property
        def arc_git_remote_ref(self):
            return make_remote_ref(self.arc_git_remote)

        @property
        def gh_url(self):
            return "%s/arcadia-%s.git" % (GH, self.name)

        @property
        def gh_git_remote(self):
            return "origin"

        @property
        def gh_git_remote_ref(self):
            return make_remote_ref(self.gh_git_remote)

        @property
        def gh_branch(self):
            return "master"

        @property
        def gh_arc_branch(self):
            return "upstream"

    if not args.submodules:
        logger.info("No submodules specified; use `--submodule ...` or `--all`")

    for submodule in args.submodules:
        logger.debug("Processing submodule '%s'", submodule)

        ctx = Ctx(git=None, svn=None, name=submodule)
        git = Git(repo=os.path.join(PROJECT_PATH, ctx.relpath))
        svn = Svn()
        ctx = ctx._replace(git=git, svn=svn)

        args.action(ctx, args)


SUBMODULES = """
contrib-libs-base64
contrib-libs-brotli
contrib-libs-c--ares
contrib-libs-double--conversion
contrib-libs-farmhash
contrib-libs-gmock
contrib-libs-grpc
contrib-libs-gtest
contrib-libs-libbz2
contrib-libs-lz4
contrib-libs-lzmasdk
contrib-libs-minilzo
contrib-libs-nanopb
contrib-libs-openssl
contrib-libs-protobuf
contrib-libs-re2
contrib-libs-snappy
contrib-libs-sparsehash
contrib-libs-yajl
contrib-libs-zlib
contrib-libs-cctz
library-colorizer
library-getopt
library-http
library-lfalloc
library-malloc-api
library-openssl
library-streams-brotli
library-streams-lz
library-streams-lzop
library-string_utils-base64
library-threading-future
mapreduce-yt-interface-protos
util
""".split()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-git-version-check", action="store_false", dest="check_git_version", default=True,
        help="(dangerous, do not use)")
    parser.add_argument(
        "--skip-git-working-tree-check", action="store_false", dest="check_git_working_tree", default=True,
        help="(dangerous, do not use)")
    parser.add_argument(
        "--skip-svn-url", action="store_false", dest="check_svn_url", default=True,
        help="(dangerous, do not use)")

    logging_parser = parser.add_mutually_exclusive_group()
    logging_parser.add_argument(
        "-s", "--silent", action="store_const", help="minimize logging",
        dest="log_level", const=logging.WARNING)
    logging_parser.add_argument(
        "-v", "--verbose", action="store_const", help="maximize logging",
        dest="log_level", const=logging.DEBUG)
    logging_parser.set_defaults(log_level=logging.INFO)

    subparsers = parser.add_subparsers()

    def add_parser(*args, **kwargs):
        parser = subparsers.add_parser(*args, **kwargs)
        parser.set_defaults(main=snapshot_main)
        return parser

    init_parser = add_parser(
        "init", help="prepare the main repository for further operations")
    init_parser.set_defaults(action=action_init)

    fetch_parser = add_parser(
        "fetch", help="fetch svn revisions from the remote repository")
    fetch_parser.set_defaults(action=action_fetch)

    stitch_parser = add_parser(
        "stitch", help="stitch svn revisions to converge git-svn histories")
    stitch_parser.set_defaults(action=action_stitch)

    pull_parser = add_parser(
        "pull", help="initiate a merge from arcadia to github")
    pull_parser.add_argument("--revision", "-r", help="revision to merge", type=int)
    pull_parser.set_defaults(action=action_pull)

    push_parser = add_parser(
        "push", help="initiate a merge from github to arcadia")
    push_parser.add_argument("--force", "-f", action="store_true", default=False,
                             help="force commit")
    push_parser.add_argument("--review", "-r", nargs="?", default=None,
                             help="review commit (you may provide the review id)")
    push_parser.add_argument("--yes", "-y", action="store_true", default=False,
                             help="do something indeed")
    push_parser.set_defaults(action=action_push)

    def add_submodule_parser(*args, **kwargs):
        parser = subparsers.add_parser(*args, **kwargs)
        parser.set_defaults(main=submodule_main, submodules=[])
        submodule_parser = parser.add_mutually_exclusive_group()
        submodule_parser.add_argument(
            "--all", action="store_const", help="apply to all submodules",
            dest="submodules", const=SUBMODULES)
        submodule_parser.add_argument(
            "--submodule", action="append", help="apply to the particular submodule",
            dest="submodules", metavar="SUBMODULE", choices=SUBMODULES)
        return parser

    submodule_init_parser = add_submodule_parser(
        "submodule-init", help="prepare the submodule for further operations")
    submodule_init_parser.set_defaults(action=action_submodule_init)

    submodule_fetch_parser = add_submodule_parser(
        "submodule-fetch", help="fetch svn revisions from the remote repository")
    submodule_fetch_parser.set_defaults(action=action_submodule_fetch)

    submodule_stitch_parser = add_submodule_parser(
        "submodule-stitch", help="(advanced) stitch svn revisions to converge git-svn histories")
    submodule_stitch_parser.set_defaults(action=action_submodule_stitch)

    submodule_pin_parser = add_submodule_parser(
        "submodule-pin", help="(advanced) pin the git commit in the remote repository")
    submodule_pin_parser.add_argument("--commit", "-c", default="HEAD",
                                      help="commit to pin")
    submodule_pin_parser.set_defaults(action=action_submodule_pin)

    submodule_fast_pull_parser = add_submodule_parser(
        "submodule-fast-pull", help="pull the submodule up to the upstream revision")
    submodule_fast_pull_parser.set_defaults(action=action_submodule_fast_pull)

    submodule_fast_push_parser = add_submodule_parser(
        "submodule-fast-push", help="push the submodule to the 'master' branch")
    submodule_fast_push_parser.add_argument("--yes", "-y", action="store_true", default=False,
                                            help="do something indeed")
    submodule_fast_push_parser.set_defaults(action=action_submodule_fast_push)

    args = parser.parse_args()

    logger.setLevel(args.log_level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.handlers.append(handler)

    args.main(args)
