#!/usr/bin/env python

"""
## Introduction

This script provides the assistance functions to synchronize YT in Arcadia and in GitHub.

`yt/` is mirrored from GitHub to Arcadia as a snapshot. `git-svn` is used to commit the appropriate
subtree into SVN, and the lineage is preserved with the commit marks. Namely, every push commit in SVN
contains the textual reference to the original Git commit. This information is used to properly pull changes
from SVN to Git.

## Glossary

**(Git) Commit**. Changeset for a repository with lineage information. Identified by a SHA1 hash.

**(Git) Reference**. Named pointed to a particular commit. Fully qualified reference starts with `refs/`.
Examples: `HEAD`, `origin/master`, `refs/remotes/origin/master`, `branch`, `refs/heads/branch`.

**(Svn) Revision**. Changeset for a repository. Identified by a natural number. Revisions are totally ordered.

## Arcadia dependencies

Arcadia dependencies (like util/, library/, etc) are managed via arcup.py script.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "git-svn"))

from git_svn_lib import (
    CheckError,
    Git,
    Svn,
    check_git_version,
    check_git_working_tree,
    check_svn_url,
    extract_git_svn_revision_to_commit_mapping_as_dict,
    extract_git_svn_revision_to_commit_mapping_as_list,
    fetch_git_svn,
    get_svn_url_for_git_svn_remote,
    init_git_svn,
    make_remote_ref,
    parse_git_svn_correspondence,
    pull_git_svn,
    push_git_svn
)

import argparse
import collections
import filecmp
import itertools
import logging
import re
import shutil
import subprocess
import tempfile

from xml.etree import ElementTree

logger = logging.getLogger("Yt.GitSvn")

ARC = "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"
GH = "git@github.yandex-team.ru:yt"

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_PATH, ".."))

##### Block to be moved ###########################################
# TODO (ermolovd): this block should be moved to git-svn/

class LocalSvn(object):
    def __init__(self, root):
        self.root = os.path.realpath(root)
        self.ya = os.path.join(self.root, "ya")
        assert os.path.isdir(os.path.join(self.root, '.svn'))

    def iter_status(self, path):
        """
        possible statuses:
            - normal -- not changed
            - unversioned -- svn doesn't know about file
            - missing -- file removed localy but present in svn
            - added -- file is not present in svn and was added
            - modified -- file is present in svn and was modified in working copy
            ...
        """
        SvnStatusEntry = collections.namedtuple("SvnStatusEntry", ["abspath", "relpath", "status"])

        path = os.path.join(self.root, path)
        xml_status = subprocess.check_output([self.ya, 'svn', 'status', '--xml', '--verbose', path])
        tree = ElementTree.fromstring(xml_status)
        for item in tree.findall("target/entry"):
            abspath = item.get("path")
            relpath = os.path.relpath(abspath, self.root)
            wc_status, = item.findall("wc-status")
            yield SvnStatusEntry(abspath, relpath, wc_status.get("item"))

    def abspath(self, path):
        return os.path.join(self.root, path)

    def add(self, *paths):
        for p in paths:
            if p.startswith('/') or not os.path.exists(self.abspath(p)):
                raise ValueError("Path '{}' must be relative to svn root".format(p))
        subprocess.check_call([self.ya, "svn", "add", "--parents"] + [self.abspath(p) for p in paths])

    def remove(self, *paths):
        for p in paths:
            if p.startswith('/'):
                raise ValueError("Path '{}' must be relative to svn root".format(p))
        subprocess.check_call([self.ya, "svn", "remove"] + [self.abspath(p) for p in paths])

    def revert(self, path):
        subprocess.check_call([self.ya, "svn", "revert", "--recursive", self.abspath(path)])


def verify_recent_svn_revision_merged(git, git_svn_id):
    svn_url = get_svn_url_for_git_svn_remote(git, git_svn_id)
    recent_revision = svn_get_last_modified_revision(Svn(), svn_url)
    svn_url = get_svn_url_for_git_svn_remote(git, git_svn_id)
    git_log_pattern = "^git-svn-id: {}@{}".format(svn_url, recent_revision)
    log = git.call("log", "--grep", git_log_pattern)
    if not log.strip():
        raise CheckError("Svn revision {} is not merged to git.\n"
                         "Use --ignore-unmerged-svn-commits flag to skip this check.\n".format(recent_revision))

def svn_get_last_modified_revision(svn, url):
    xml_svn_info = svn.call("info", "--xml", url)
    tree = ElementTree.fromstring(xml_svn_info)
    commit_lst = tree.findall("entry/commit")
    assert len(commit_lst) == 1
    return commit_lst[0].get("revision")

def local_svn_iter_changed_files(local_svn, relpath):
    return (status for status in local_svn.iter_status(relpath) if status.status not in ("normal", "unversioned"))

def rmrf(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

def xargs(cmd_func, arg_list, batch_size=100):
    arg_list = list(arg_list)
    while arg_list:
        cmd_func(*arg_list[-batch_size:])
        del arg_list[-batch_size:]

def git_ls_files(git, pathspec):
    output = git.call("ls-files", "-z", "--full-name", pathspec)
    return output.strip('\000').split('\000')

def git_iter_files_to_sync(git, pathspec):
    for relpath in git_ls_files(git, pathspec):
        if os.path.islink(git_abspath(git, relpath)):
            warn = (
                "Skipping symlink file: `{}'\n"
                "(To be honest author of this script doubts that keeping symlink in a repo is a good idea.\n"
                "He encourages you to remove symlink file in order to mute this annoying warning.)").format(relpath)
            logger.warning(warn)
            continue
        yield relpath

def git_abspath(git, relpath):
    if git.work_dir is None:
        raise ValueError("git object instance doesn't have work_dir attribute")
    return os.path.join(git.work_dir, relpath)

def idented_lines(lines, ident_size=2):
    ident = " " * ident_size
    return "".join(ident + l + "\n" for l in lines)

def iter_relpath_translate(relpaths, base_relpath_from, base_relpath_to):
    if not base_relpath_from.endswith("/"):
        base_relpath_from += "/"
    if not base_relpath_to.endswith("/") and base_relpath_to:
        base_relpath_to += "/"

    for path in relpaths:
        if not path.startswith(base_relpath_from):
            raise RuntimeError("Expected relpath `{0}' to be inside directory `{1}'".format(path, base_relpath_from))
        yield base_relpath_to + path[len(base_relpath_from):]

def notify_svn(local_svn, project_relpath, file_relpaths):
    file_relpaths = frozenset(file_relpaths)

    svn_status = {}
    for item in local_svn.iter_status(project_relpath):
        svn_status[item.relpath] = item.status

    to_add = []
    for relpath in sorted(file_relpaths):
        if not relpath.startswith(project_relpath + '/'):
            raise RuntimeError("Expected relpath '{}' to be inside directory: '{}'".format(relpath, project_relpath))
        status = svn_status.get(relpath, "unversioned")

        if status == "unversioned":
            to_add.append(relpath)
        elif status not in ("normal", "modified"):
            raise RuntimeError("Unexpected svn status: '{}' for file '{}'".format(status, relpath))

    to_remove = []
    for relpath, status in svn_status.iteritems():
        if relpath in file_relpaths:
            continue
        if os.path.isdir(local_svn.abspath(relpath)):
            continue

        if status == "missing":
            to_remove.append(relpath)
        else:
            raise RuntimeError, "Don't know what to do with file: '{}' status: '{}'".format(relpath, status)

    xargs(lambda *args: local_svn.add(*args), to_add)
    xargs(lambda *args: local_svn.remove(*args), to_remove)

def git_verify_head_pushed(git):
    output = git.call("branch", "--remote", "--contains", "HEAD")
    if not output:
        raise CheckError("remote repo doesn't contain HEAD")

def verify_svn_match_git(git, git_relpath, local_svn, svn_relpath):
    git_rel_paths = set(iter_relpath_translate(git_iter_files_to_sync(git, ":/" + git_relpath), git_relpath, ""))
    svn_tracked_rel_paths = set(
        iter_relpath_translate(
            (
                item.relpath
                for item in local_svn.iter_status(svn_relpath)
                if (item.status in ["normal", "modified", "added"]
                    and not os.path.isdir(item.abspath))
            ),
            svn_relpath,
            ""))

    only_in_git = git_rel_paths - svn_tracked_rel_paths
    only_in_svn = svn_tracked_rel_paths - git_rel_paths
    if only_in_git or only_in_svn:
        raise CheckError(
            "svn working copy doesn't match git repo\n"
            "files that are in git and not in svn:\n\n"
            "{only_in_git}\n"
            "files that are in svn and not in git:\n\n"
            "{only_in_svn}".format(
                only_in_git=idented_lines(only_in_git),
                only_in_svn=idented_lines(only_in_svn),
            ))

    diffed = []
    for relpath in git_rel_paths:
        svn_path = local_svn.abspath(os.path.join(svn_relpath, relpath))
        git_path = git_abspath(git, os.path.join(git_relpath, relpath))
        if not filecmp.cmp(svn_path, git_path):
            diffed.append(relpath)
    if diffed:
        raise CheckError(
            "Some files in svn working copy differs from corresponding files from git repo:\n"
            "{diffed}\n".format(
                diffed=idented_lines(diffed)))

##### End of block to be moved #####################################

def get_abi_major_minor_from_git_branch(git):
    ref = git.call("rev-parse", "--abbrev-ref", "HEAD").strip()
    match = re.match(r"^(?:pre)?stable[^/]*/(\d+).(\d+)$", ref)
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

def action_pull(ctx, args):
    pull_git_svn(
        ctx.git,
        ctx.svn,
        ctx.arc_url,
        ctx.arc_git_remote,
        "yt/",
        "yt/%s/" % ctx.abi,
        revision=args.revision,
        recent_push=args.recent_push)

def action_copy_to_local_svn(ctx, args):
    local_svn = LocalSvn(args.arcadia)
    if args.check_unmerged_svn_commits:
        logger.info("check that svn doesn't have any commits that are not merged to github")
        verify_recent_svn_revision_merged(ctx.git, ctx.arc_git_remote)

    logger.info("check svn repository for local modifications")
    changed_files = list(local_svn_iter_changed_files(local_svn, ctx.svn_relpath))
    if changed_files and not args.ignore_svn_modifications:
        raise CheckError(
            "svn repository has unstaged changed:\n"
            "{changed_files}\n"
            "Use --ignore-svn-modifications to ignore them.\n".format(
                changed_files=idented_lines(["{0} {1}".format(s.status, s.relpath) for s in changed_files])))
    local_svn.revert(ctx.svn_relpath)

    logger.info("copying files to arcadia directory")
    rmrf(local_svn.abspath(ctx.svn_relpath))

    # Copy files
    git_rel_file_list = list(git_iter_files_to_sync(ctx.git, ":/" + ctx.git_relpath))
    svn_rel_file_list = list(iter_relpath_translate(git_rel_file_list, ctx.git_relpath, ctx.svn_relpath))
    assert len(git_rel_file_list) == len(svn_rel_file_list)
    for rel_git_file, rel_svn_file in itertools.izip(git_rel_file_list, svn_rel_file_list):
        git_file = git_abspath(ctx.git, rel_git_file)
        svn_file = local_svn.abspath(rel_svn_file)

        svn_dir = os.path.dirname(svn_file)
        if not os.path.exists(svn_dir):
            os.makedirs(svn_dir)
        shutil.copy2(git_file, svn_file)

    logger.info("notify svn about changes")
    notify_svn(local_svn, ctx.svn_relpath, svn_rel_file_list)

    logger.info("checking that HEAD is present at github")
    must_push_before_commit = False
    try:
        git_verify_head_pushed(ctx.git)
    except CheckError as e:
        must_push_before_commit = True

    print >>sys.stderr, (
        "====================================================\n"
        "All files have beed copied to svn working copy. Please go to\n"
        "  {arcadia_project_path}\n"
        "and check that everything is ok. Once you are done run:\n"
        " $ {script} svn-commit --arcadia {arcadia}"
    ).format(
        arcadia=local_svn.abspath(""),
        arcadia_project_path=local_svn.abspath(ctx.svn_relpath),
        script=sys.argv[0])

    if must_push_before_commit:
        print >>sys.stderr, "WARNING:", e
        print >>sys.stderr, "You can check for compileability but you will need to push changes to github before commit"

def action_svn_commit(ctx, args):
    local_svn = LocalSvn(args.arcadia)

    if args.check_unmerged_svn_commits:
        logger.info("check that svn doesn't have any commits that are not merged to github")
        verify_recent_svn_revision_merged(ctx.git, ctx.arc_git_remote)

    logger.info("checking that HEAD is present at github")
    git_verify_head_pushed(ctx.git)

    logger.info("comparing svn copy and git copy")
    verify_svn_match_git(ctx.git, ctx.git_relpath, local_svn, ctx.svn_relpath)

    logger.info("prepare commit")
    head = ctx.git.resolve_ref("HEAD")
    fd, commit_message_file_name = tempfile.mkstemp("-yp-commit-message", text=True)
    print commit_message_file_name
    with os.fdopen(fd, 'w') as outf:
        outf.write(
            "Push {svn_path}/ to arcadia\n"
            "\n"
            "__BYPASS_CHECKS__\n"
            "yt:git_commit:{head}\n".format(
                svn_path=ctx.svn_relpath,
                head=head))
        if args.review:
            outf.write("\nREVIEW:new\n")

    print >>sys.stderr, "Commit is prepared, now run:\n"
    print >>sys.stderr, "$ {ya_path} svn commit {arcadia_yp_path} -F {commit_message_file_name}".format(
        ya_path=os.path.join(args.arcadia, "ya"),
        arcadia_yp_path=local_svn.abspath(ctx.svn_relpath),
        commit_message_file_name=commit_message_file_name)

def snapshot_main(args):
    class Ctx(collections.namedtuple("Ctx", ["git", "svn", "abi_major", "abi_minor"])):
        @property
        def abi(self):
            return "%s_%s" % (self.abi_major, self.abi_minor)

        @property
        def svn_relpath(self):
            return "yt/{0}/yt".format(self.abi)

        @property
        def git_relpath(self):
            return "yt"

        @property
        def arc_url(self):
            return "{0}/{1}".format(ARC, self.svn_relpath)

        # TODO (ermolovd): rename to `git_svn_remote_id'
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
    pull_parser.add_argument(
        "--recent-push",
        metavar="<svn-revision>:<git-commit>",
        type=parse_git_svn_correspondence,
        help="recent push svn revision and corresponding git-commit (by default it is determined automatically)")
    pull_parser.set_defaults(action=action_pull)

    def add_arcadia_argument(p):
        p.add_argument("-a", "--arcadia", required=True, help="path to local svn working copy")

    def add_ignore_unmerged_svn_commits_argument(p):
        p.add_argument("--ignore-unmerged-svn-commits", dest="check_unmerged_svn_commits", default=True, action="store_false",
                       help="do not complain when svn has commits that are not merged into github")

    copy_to_local_svn_parser = add_parser("copy-to-local-svn", help="push current git snapshot to svn working copy")
    copy_to_local_svn_parser.add_argument("--ignore-svn-modifications", default=False, action="store_true",
                                          help="ignore and override changes in svn working copy")
    add_arcadia_argument(copy_to_local_svn_parser)
    add_ignore_unmerged_svn_commits_argument(copy_to_local_svn_parser)
    copy_to_local_svn_parser.set_defaults(action=action_copy_to_local_svn)

    svn_commit_parser = add_parser("svn-commit", help="prepare commit of yt snapshot to svn")
    add_arcadia_argument(svn_commit_parser)
    add_ignore_unmerged_svn_commits_argument(svn_commit_parser)
    svn_commit_parser.add_argument("--no-review", dest='review', default=True, action='store_false',
                                   help="do not create review, commit right away")
    svn_commit_parser.set_defaults(action=action_svn_commit)

    args = parser.parse_args()

    logger.setLevel(args.log_level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.handlers.append(handler)

    args.main(args)
