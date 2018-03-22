#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import collections
import filecmp
import logging
import os
import re
import subprocess
import shutil
import sys
import tempfile

from xml.etree import ElementTree

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "git-svn"))
from git_svn_lib import (Git, Svn, init_git_svn, fetch_git_svn, pull_git_svn)

logger = logging.getLogger("Yt.GitSvn")

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_PATH, ".."))

YP_GIT_PATH = "yp"
YP_GIT_PATHSPEC = ":/" + YP_GIT_PATH
YP_SVN_PATH = "yp"
ARCADIA_URL = "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia" 
YP_ARCADIA_URL = ARCADIA_URL + "/" + YP_SVN_PATH

YP_GIT_SVN_REMOTE_ID = "arcadia_yp"

class YpSyncError(RuntimeError):
    pass

def idented_lines(lines, ident_size=2):
    ident = " " * ident_size
    return "".join(ident + l + "\n" for l in lines)

def git_verify_tree_clean(git, pathspec):
    status_output = git.call("status", "--porcelain", "--untracked-files=no", pathspec)
    if status_output:
        raise YpSyncError("git repository has unstaged changed:\n{}".format(status_output))

def git_verify_head_pushed(git):
    output = git.call("branch", "--remote", "--contains", "HEAD")
    if not output:
        raise YpSyncError("remote repo doesn't contain HEAD")

def svn_verify_tree_clean(svn, path):
    changed_item_list = [status for status in svn.iter_status(path) if status.status not in ("normal", "unversioned")]
    if changed_item_list:
        changed_files = idented_lines(["{} {}".format(s.status, s.relpath) for s in changed_item_list])
        raise YpSyncError("svn repository has unstaged changed:\n"
                          "{changed_files}\n".format(changed_files=changed_files))

def rmrf(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

class LocalGit(object):
    def __init__(self, root):
        self.root = os.path.realpath(root)
        assert os.path.isdir(os.path.join(self.root, '.git'))
        self.git = Git(root)

    def ls_files(self, pathspec):
        output = self.git.call("ls-files", "-z", "--full-name", pathspec)
        return output.strip('\000').split('\000')

    def abspath(self, path):
        return os.path.realpath(os.path.join(self.root, path))

    def rev_parse(self, rev="HEAD"):
	ret = self.git.call("rev-parse", rev).rstrip('\n')
	assert re.match('^[0-9a-f]{40}$', ret)
        return ret


class LocalSvn(object):
    def __init__(self, root):
        self.root = os.path.realpath(root)
        assert os.path.isdir(os.path.join(self.root, '.svn'))

    def iter_status(self, path):
        """
        possible statuses:
            - normal -- not changed
            - unversioned -- svn doesn't know about file
        """
        SvnStatusEntry = collections.namedtuple("SvnStatusEntry", ["abspath", "relpath", "status"])

        path = os.path.join(self.root, path)
        xml_status = subprocess.check_output(['svn', 'status', '--xml', '--verbose', path])
        tree = ElementTree.fromstring(xml_status)
        for item in tree.findall("target/entry"):
            abspath = item.get("path")
            relpath = os.path.relpath(abspath, self.root)
            wc_status, = item.findall("wc-status")
            yield SvnStatusEntry(abspath, relpath, wc_status.get("item"))

    def abspath(self, path):
        return os.path.realpath(os.path.join(self.root, path))

    def revert(self, path):
        subprocess.check_call(["svn", "revert", "--recursive", self.abspath(path)])

    def add(self, path):
        if path.startswith('/') or not os.path.exists(self.abspath(path)):
            raise ValueError("Path '{}' must be relative to svn root".format(path))
        subprocess.check_call(["svn", "add", "--parents", self.abspath(path)])

def notify_svn(svn, base_path, rel_files):
    rel_files = frozenset(rel_files)

    svn_status = {}
    for item in svn.iter_status(base_path):
        svn_status[item.relpath] = item.status

    for relpath in sorted(rel_files):
        assert relpath.startswith(base_path + '/'), "Expected relpath '{}' to be inside directory: '{}'".format(relpath, base_path)
        status = svn_status.get(relpath, "unversioned")

        if status == "unversioned":
            svn.add(relpath)
        elif status not in ("normal", "modified"):
            raise RuntimeError("Unexpected svn status: '{}' for file '{}'".format(item.status, relpath))

    for relpath, status in svn_status.iteritems():
        if relpath in rel_files:
            continue
        if os.path.isdir(svn.abspath(relpath)):
            continue
        raise RuntimeError, "Don't know what to do with file: '{}' status: '{}'".format(file, status)

def verify_svn_match_git(git, svn):
    git_rel_paths = set(git.ls_files(YP_GIT_PATHSPEC))
    svn_status = list(svn.iter_status(YP_SVN_PATH))

    svn_tracked_rel_paths = set(item.relpath
                                for item in svn_status
                                if (
                                    item.status in ["normal", "modified", "added"]
                                    and not os.path.isdir(item.abspath)))

    only_in_git = git_rel_paths - svn_tracked_rel_paths
    only_in_svn = svn_tracked_rel_paths - git_rel_paths
    if only_in_git or only_in_svn:
        raise YpSyncError(
            "svn working copy doesn't match git repo\n"
            "files that are in git and not in svn:\n\n"
            "{only_in_git}\n"
            "files that are in svn and not in git:\n\n"
            "{only_in_svn}").format(
                only_in_git=idented_lines(only_in_git),
                only_in_svn=idented_lines(only_in_svn),
            )

    # 1. Смотрим на файлы в git'е
    # 2. Убеждаемся, что все файлы гита добавлены в svn.
    # 3. Убеждаемся, что нет файлов в svn'е не содержащихся в git'е.
    # 4. Сравниваем файлы на совпадение.
    diffed = []
    for relpath in git_rel_paths:
        svn_path = svn.abspath(relpath)
        git_path = git.abspath(relpath)
        if not filecmp.cmp(svn_path, git_path):
            diffed.append(relpath)
    if diffed:
        raise YpSyncError(
            "Some files in svn working copy differs from corresponding files from git repo:\n"
            "{diffed}\n".format(
                diffed=idented_lines(diffed)))

def subcommand_copy_to_local_svn(args):
    git = Git(PROJECT_PATH)
    local_git = LocalGit(PROJECT_PATH)
    svn = LocalSvn(args.arcadia)

    logger.info("check for local modifications in git repo")
    git_verify_tree_clean(git, YP_GIT_PATHSPEC)

    logger.info("check svn repository for local modifications")
    try:
        svn_verify_tree_clean(svn, YP_SVN_PATH)
    except YpSyncError:
        if not args.force:
            raise
        svn.revert(YP_SVN_PATH)

    logger.info("copying files to arcadia directory")
    rmrf(svn.abspath(YP_SVN_PATH))
    # Copy files
    rel_files = set(local_git.ls_files(YP_GIT_PATHSPEC))
    for rel_file in rel_files:
        git_file = local_git.abspath(rel_file)
        svn_file = svn.abspath(rel_file)

        svn_dir = os.path.dirname(svn_file)
        if not os.path.exists(svn_dir):
            os.makedirs(svn_dir)
        shutil.copy2(git_file, svn_file)

    logger.info("notify svn about changes")
    notify_svn(svn, YP_SVN_PATH, rel_files)

    logger.info("checking that HEAD is present at github")
    must_push_before_commit = False
    try:
        git_verify_head_pushed(git)
    except YpSyncError as e:
        must_push_before_commit = True

    print >>sys.stderr, (
        "====================================================\n"
        "YP has beed copied to svn working copy. Please go to\n"
        "  {arcadia_yp}\n"
        "and check that everything is ok. Once you are done run:\n"
        " $ {script} svn-commit --arcadia {arcadia}"
    ).format(
        arcadia=svn.abspath(""),
        arcadia_yp=svn.abspath(YP_SVN_PATH),
        script=sys.argv[0])

    if must_push_before_commit:
        print >>sys.stderr, "WARNING:", e
        print >>sys.stderr, "You can check for compileability but you will need to push changes to github before commit"


def subcommand_svn_commit(args):
    git = Git(PROJECT_PATH)
    local_git = LocalGit(PROJECT_PATH)
    svn = LocalSvn(args.arcadia)

    logger.info("check for local modifications in git repo")
    git_verify_tree_clean(git, YP_GIT_PATHSPEC)

    logger.info("checking that HEAD is present at github")
    git_verify_head_pushed(git)

    logger.info("comparing svn copy and git copy")
    verify_svn_match_git(local_git, svn)

    logger.info("prepare commit")
    # Создать времменный файл,
    head = local_git.rev_parse()
    fd, commit_message_file_name = tempfile.mkstemp("-yp-commit-message", text=True)
    print commit_message_file_name
    with os.fdopen(fd, 'w') as outf:
        outf.write(
            "Push yp to arcadia\n"
            "\n"
            "__BYPASS_CHECKS__\n"
            "yt:git_commit:{head}\n".format(head=head))
        if args.review:
            outf.write("\nREVIEW:new\n")

    print >>sys.stderr, "Commit is prepared, now run:\n"
    print >>sys.stderr, "$ svn commit {arcadia_yp_path} -F {commit_message_file_name}".format(
        arcadia_yp_path=svn.abspath(YP_SVN_PATH),
        commit_message_file_name=commit_message_file_name)

def subcommand_init(args):
    git = Git(PROJECT_PATH)
    init_git_svn(git, YP_GIT_SVN_REMOTE_ID, YP_ARCADIA_URL)

def subcommand_fetch(args):
    git = Git(PROJECT_PATH)
    svn = Svn()
    fetch_git_svn(git, svn, YP_GIT_SVN_REMOTE_ID, one_by_one=True)

def subcommand_pull(args):
    git = Git(PROJECT_PATH)
    svn = Svn()
    pull_git_svn(git, svn, YP_ARCADIA_URL, YP_GIT_SVN_REMOTE_ID, YP_GIT_PATH, YP_SVN_PATH, revision=args.revision)

def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()

    # copy-to-local-svn subcommand
    copy_to_local_svn_parser = subparsers.add_parser("copy-to-local-svn")
    copy_to_local_svn_parser.add_argument("--force", default=False, action="store_true")
    copy_to_local_svn_parser.add_argument("-a", "--arcadia", required=True)
    copy_to_local_svn_parser.set_defaults(subcommand=subcommand_copy_to_local_svn)

    # local-svn-commit
    svn_commit_parser = subparsers.add_parser("svn-commit")
    svn_commit_parser.add_argument("-a", "--arcadia", required=True)
    svn_commit_parser.add_argument("--no-review", dest='review', default=True, action='store_false')
    svn_commit_parser.set_defaults(subcommand=subcommand_svn_commit)

    # init
    init_parser = subparsers.add_parser("init")
    init_parser.set_defaults(subcommand=subcommand_init)

    # fetch 
    fetch_parser = subparsers.add_parser("fetch")
    fetch_parser.set_defaults(subcommand=subcommand_fetch)

    # pull 
    pull_parser = subparsers.add_parser("pull")
    pull_parser.add_argument(
        "-r", "--revision",
        help="revision to merge (by default most recent revision will be merged)", type=int)
    pull_parser.set_defaults(subcommand=subcommand_pull)

    # Logging options
    logging_parser = parser.add_mutually_exclusive_group()
    logging_parser.add_argument(
        "-q", "--quiet", action="store_const", help="minimize logging",
        dest="log_level", const=logging.WARNING)
    logging_parser.add_argument(
        "-v", "--verbose", action="store_const", help="maximize logging",
        dest="log_level", const=logging.DEBUG)
    logging_parser.set_defaults(log_level=logging.INFO)

    args = parser.parse_args()

    # Set up logger
    logger.setLevel(args.log_level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.handlers.append(handler)

    # Run subcommand
    try:
        args.subcommand(args)
    except YpSyncError as e:
        print >>sys.stderr, "ERROR:", e
        print >>sys.stderr, "Error occurred exiting..."
        exit(1)

if __name__ == "__main__":
    main()
