#!/usr/bin/env python2
# -*- encoding: utf8 -*-

"""
https://wiki.yandex-team.ru/yt/internal/arcadia-sync-ng/
"""

import argparse
import collections
import filecmp
import itertools
import json
import logging
import os
import re
import shutil
import stat
import sys
import tempfile
import time

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

from xml.etree import ElementTree
from contextlib import contextmanager

logger = logging.getLogger("Yt.GitSvn")

ARC = "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"
GH = "git@github.yandex-team.ru:yt"

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_PATH, "../../"))

GIT_EXCLUDES = ["yt/scripts/teamcity-build", "yt/scripts/nanny-releaselib"]

ARGV0 = sys.argv[0]


class ArcadiaSyncError(RuntimeError):
    pass


###################################################################################################################
## Helper functions
###################################################################################################################


def is_treeish(s):
    """Check if string looks like a git treeish."""
    if not isinstance(s, str):
        return False
    if len(s) != 40:
        return False
    if not re.match(r"^[0-9a-f]+$", s):
        return False
    return True


def make_remote_ref(name):
    """Make fully qualified Git remote reference."""
    if name.startswith("refs/"):
        return name
    else:
        return "refs/remotes/%s" % name


def make_head_ref(name):
    """Make fully qualified Git head reference."""
    if name.startswith("refs/"):
        return name
    else:
        return "refs/heads/%s" % name


def abbrev(commit):
    """Abbreviate the commit hash."""
    if commit:
        return commit[:8]
    else:
        return "(null)"


def get_review_url(review_id):
    return "https://a.yandex-team.ru/review/{}".format(review_id)


def parse_git_svn_correspondence(s):
    GitSvnCorrespondence = collections.namedtuple("GitSvnCorrespondence", ["svn_revision", "git_commit"])

    m = re.match("^r?([0-9]+):([0-9a-f]+)$", s.lower())
    if not m:
        raise ArcadiaSyncError("String '{0}' doesn't look like <svn-revision>:<git-revision>".format(s))
    return GitSvnCorrespondence(
        svn_revision=int(m.group(1)),
        git_commit=m.group(2))


def check_git_working_tree(git, can_be_disabled=True):
    git.call("update-index", "-q", "--refresh")

    error_msg = "Git working tree has local modifications.\n"
    if can_be_disabled:
        error_msg += "Use --skip-git-working-tree-check flag to skip this check.\n"

    if (
        not git.test("diff-index", "HEAD", "--exit-code", "--quiet", "--ignore-submodules=untracked")
        or not git.test("diff-index", "--cached", "HEAD", "--exit-code", "--quiet")
    ):
        raise ArcadiaSyncError(error_msg)


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


###################################################################################################################
## Commands
###################################################################################################################


class Command(object):
    Result = collections.namedtuple("Result", ["returncode", "stdout", "stderr"])

    def _impl(self, args, capture=True, input=None, cwd=None):
        logger.debug("<< Calling %r", args)
        stdfds = subprocess.PIPE if capture else None
        stdinfd = subprocess.PIPE if input is not None else None
        child = subprocess.Popen(args, bufsize=1, stdout=stdfds, stderr=stdfds, stdin=stdinfd, cwd=cwd)
        stdoutdata, stderrdata = child.communicate(input=input)
        logger.debug(
            ">> Call completed with return code %s; stdout=%r; stderr=%r",
            child.returncode, stdoutdata, stderrdata)
        return Command.Result(returncode=child.returncode,
                              stdout=stdoutdata, stderr=stderrdata)

    def call(self, *args, **kwargs):
        capture = kwargs.get("capture", True)
        input = kwargs.get("input", None)
        cwd = kwargs.get("cwd", None)
        raise_on_error = kwargs.get("raise_on_error", True)
        result = self._impl(args, capture=capture, input=input, cwd=cwd)
        if result.returncode != 0 and raise_on_error:
            if capture:
                raise ArcadiaSyncError("Call {} failed, stderr:\n{}\n".format(args, result.stderr))
            else:
                raise ArcadiaSyncError("Call {} failed, stderr is not captured".format(args))
        else:
            return result.stdout

    def test(self, *args):
        result = self._impl(args)
        return result.returncode == 0


class Git(Command):
    def __init__(self, repo=None, git_dir=None, work_dir=None):
        super(Git, self).__init__()
        if repo is not None:
            assert git_dir is None
            assert work_dir is None
            self.work_dir = os.path.abspath(repo)
            self.git_dir = os.path.join(self.work_dir, ".git")
        if git_dir is not None or work_dir is not None:
            assert repo is None
            self.work_dir = work_dir
            self.git_dir = git_dir

    def _impl(self, args, **kwargs):
        call_args = ["git"]
        if self.git_dir:
            call_args.append("--git-dir=" + str(self.git_dir))
        if self.work_dir:
            call_args.append("--work-tree=" + str(self.work_dir))
        call_args.extend(args)
        return super(Git, self)._impl(call_args, **kwargs)

    def resolve_ref(self, ref):
        result = self.call("rev-parse", "--quiet", "--verify", ref, raise_on_error=False).strip()
        if not result:
            return None
        assert is_treeish(result)
        return result

    def has_ref(self, ref):
        return self.resolve_ref(ref) is not None

    def is_ancestor(self, child, parent):
        return self.test("merge-base", "--is-ancestor", child, parent)


class Svn(Command):
    def __init__(self):
        super(Svn, self).__init__()
        self.last_call_at = 0.0

    def _impl(self, args, **kwargs):
        # it seems that Arcadia limits connection rate
        # so we cap it to 1 call/s
        now = time.time()
        delay = now - self.last_call_at
        if delay < 1.0:
            time.sleep(1.0 - delay)
        self.last_call_at = now
        call_args = ["svn"]
        call_args.extend(args)
        return super(Svn, self)._impl(call_args, **kwargs)


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

    def _svn_ci(self, msg, path_list):
        with tempfile.NamedTemporaryFile() as tmpf:
            cmd = [self.ya, "svn", "ci", "-m", msg]
            cmd += path_list
            retcode = subprocess.call(cmd, stderr=tmpf, cwd=self.root)
            tmpf.seek(0)
            stderr = tmpf.read()
            return retcode, stderr

    def create_review(self, msg, path_list):
        msg += "REVIEW:NEW"
        retcode, stderr = self._svn_ci(msg, path_list)
        assert retcode != 0, "Ooops, looks like commit was successful please revert it :("

        failed_to_create_review_msg = (
            "svn commit stderr:\n"
            "{stderr}"
            "\n"
            "Failed to create review.\n"
            .format(stderr=stderr)
        )
        m = re.search(r"https://a[.]yandex-team[.]ru/review/(\d+)", stderr)
        if m is None:
            raise ArcadiaSyncError(failed_to_create_review_msg)
        return int(m.group(1))

    def update_review(self, review_id, path_list):
        commit_message = (
            "__BYPASS_CHECKS__\n"
            "REVIEW_UPLOAD:{review_id}\n"
            .format(review_id=review_id)
        )
        retcode, stderr = self._svn_ci(commit_message, path_list)
        assert retcode != 0, "Ooops, looks like commit was successful please revert it :("

        failed_to_update_review_msg = (
            "svn commit stderr:\n"
            "{stderr}"
            "\n"
            "Failed to update review.\n"
            .format(stderr=stderr)
        )
        m = re.search(r"https://a[.]yandex-team[.]ru/review/(\d+)", stderr)
        if m is None:
            raise ArcadiaSyncError(failed_to_update_review_msg)
        assert int(m.group(1)) == review_id

    def merge_review(self, review_id, path_list):
        commit_message = (
            "__BYPASS_CHECKS__\n"
            "REVIEW_MERGE:{review_id}\n"
            .format(review_id=review_id)
        )
        retcode, stderr = self._svn_ci(commit_message, path_list)
        error_msg = (
            "svn commit stderr:\n"
            "{stderr}\n"
            "Failed to merge review."
            .format(stderr=stderr)
        )
        if retcode != 0:
            raise ArcadiaSyncError(error_msg)


###################################################################################################################
## Git / Svn library
###################################################################################################################


def get_svn_revisions(svn, url):
    """Returns all SVN revisions that affect the given SVN URL."""

    lines = svn.call("log", url).splitlines()

    revisions = []
    flag = False
    for line in lines:
        if line.startswith("r") and flag:
            revision = int(line.split()[0][1:])
            revisions.append(revision)
        if line == "------------------------------------------------------------------------":
            flag = True
        else:
            flag = False
    return revisions

def get_svn_last_changed_revision(svn, url):
    """Returns the last changed revision for the given SVN URL."""

    lines = svn.call("info", url).splitlines()

    revision = None
    for line in lines:
        if line.startswith("Last Changed Rev"):
            revision = int(line.split(":")[1])
    return revision

def get_svn_url_for_git_svn_remote(git, svn_remote):
    """Returns the SVN URL for the `git-svn` remote."""

    return git.call("config", "--local", "svn-remote.%s.url" % svn_remote).strip()

def translate_git_commit_to_svn_revision(git, arc_git_remote, ref):
    """Translates the single commit (given by the reference) into the revision."""

    return int(git.call("svn", "--svn-remote", arc_git_remote, "find-rev", ref).strip())

def translate_svn_revision_to_git_commit(git, arc_git_remote, revision):
    """Translates the single commit (given by the reference) into the revision."""
    if not isinstance(revision, int):
        raise "Revision must be integer"
    revision = "r{0}".format(revision)
    return git.call("svn", "--svn-remote", arc_git_remote, "find-rev", revision, arc_git_remote).strip()

def extract_pull_history(git, svn_prefix, ref):
    """
    Extracts the pull history.

    This function returns a list of pairs, (revision, pull commit), newest-to-oldest.
    """

    lines = git.call(
        "log",
        "--grep=^yt:svn_revision:",
        "--grep=^Pull %s" % svn_prefix,
        "--all-match",
        "--pretty=format:BEGIN %H%n%s%n%n%b%nEND%n",
        ref
    ).splitlines()

    mapping = []
    commit = None
    revision = None
    for line in lines:
        if line.startswith("BEGIN"):
            commit = line.split()[1]
        if line.startswith("yt:svn_revision:"):
            revision = int(line.split(":")[2])
        if line.startswith("END"):
            if commit and revision:
                mapping.append((revision, commit))
            commit = None
            revision = None
    return mapping

def extract_push_history(project_name, git, svn_prefix, ref):
    """
    Extracts the push history.

    Invokation of the `git svn commit-tree` command creates a new SVN revision
    which basically overrides the repository content to match the current tree.

    Therefore, during a next fetch from SVN, there will be a new, unmerged commit
    in the `git-svn` remote branch with its tree matching the committed tree.

    However, commit-wise, this new commit (called the 'push commit') does not have
    the 'base commit' (one containing the committed tree) as its ancestor, which
    makes a merge rather difficult.

    This function returns a list of pairs, (base commit, push commit),
    newest-to-oldest, which is used later to help merges.
    """

    git_log_output = git.call(
        "--no-replace-objects",
        "log",
        "--grep=^yt:git_commit:",
        "--all-match",
        "-z",
        "--pretty=format:%H%n%s%n%n%b%n",
        ref
    )
    commit_info_list = git_log_output.strip('\0').split('\0')

    project_pattern = "yt:arcadia-sync:project:{project}".format(project=project_name)

    mapping = []
    push_commit = None
    base_commit = None
    for commit_info in commit_info_list:
        push_commit, commit_message = commit_info.split('\n', 1)
        m = re.search(r"yt:git_commit:([0-9a-fA-F]+)$", commit_message, re.MULTILINE)
        assert m is not None
        base_commit = m.group(1)

        synced_projects = set()
        for m in re.finditer("^yt:arcadia-sync:project:(.*)$", commit_message, re.MULTILINE):
            synced_projects.add(m.group(1))

        # If synced_projects is empty this is probably old commit,
        # before we started to write projects in commit message.
        if not synced_projects or project_name in synced_projects:
            mapping.append((base_commit, push_commit))

    return mapping

def extract_git_svn_revision_to_commit_mapping_as_list(git, svn_url, ref):
    """
    Extracts the revision to commit mapping from the repository.
    Returns the list of pairs (revision, commit).
    """

    lines = git.call(
        "--no-replace-objects",
        "log", "--grep=^git-svn-id:",
        "--pretty=format:BEGIN %H%n%s%n%n%b%nEND%n",
        ref).splitlines()

    mapping = []
    commit = None
    revision = None
    for line in lines:
        if line.startswith("BEGIN"):
            commit = line.split()[1]
        if line.startswith("git-svn-id:"):
            url, rev = line.split()[1].split("@")
            if url == svn_url:
                revision = int(rev)
        if line.startswith("END"):
            if commit and revision:
                mapping.append((revision, commit))
            commit = None
            revision = None
    return mapping


def extract_git_svn_revision_to_commit_mapping_as_dict(git, svn_url, ref):
    """
    Extracts the revision to commit mapping from the repository.
    Returns the dictionary (revision -> commit).
    """
    mapping = extract_git_svn_revision_to_commit_mapping_as_list(git, svn_url, ref)
    if len(mapping) != len(set(map(lambda _: _[0], mapping))):
        raise ArcadiaSyncError("SVN revisions are not unique in the commit tree rooted at '%s' (WTF?)" % ref)
    mapping = dict(mapping)
    return mapping


###################################################################################################################
## High level routines.
###################################################################################################################


def init_git_svn(git, git_svn_remote, svn_url):
    """Sets up the `git-svn` remote in the Git repository."""

    logger.debug("Setting up the git-svn remote '%s' for '%s'", git_svn_remote, svn_url)

    if git.test("config", "--local", "--get", "svn-remote.%s.url" % git_svn_remote):
        git.call("config", "--local", "--remove-section", "svn-remote.%s" % git_svn_remote)

    git.call("config", "--local", "svn-remote.%s.url" % git_svn_remote, svn_url)
    git.call("config", "--local", "svn-remote.%s.fetch" % git_svn_remote, ":" + make_remote_ref(git_svn_remote))

    git.call("svn", "--svn-remote", git_svn_remote, "migrate", capture=False)


def fetch_git_svn(git, svn, git_svn_remote, one_by_one=False, force=False):
    """Fetches all SVN revisions for the given `git-svn` remote."""

    logger.debug("Fetching the `git-svn` remote '%s'")

    def _impl(from_revision, to_revision, log_window_size=1):
        if from_revision != "HEAD" and to_revision != "HEAD":
            mark = "from-%s-to-%s" % (from_revision, to_revision)
            mark = "svn-remote.%s.fetch-%s" % (git_svn_remote, mark)
        else:
            mark = None

        if not force and mark:
            if git.test("config", "--local", "--bool", "--get", mark):
                logger.debug(
                    "Skipping revisions %s:%s because they are marked as fetched",
                    from_revision, to_revision)
                return

        success = False
        for i in range(5):
            try:
                git.call(
                    "svn", "--svn-remote", git_svn_remote, "fetch", "--log-window-size", str(log_window_size),
                    "--revision", "%s:%s" % (from_revision, to_revision),
                    capture=False)
                success = True
                break
            except ArcadiaSyncError:
                logger.debug("Call failed, sleeping for 1s")
                time.sleep(1)

        if not success:
            raise ArcadiaSyncError("Failed to fetch revisions")

        logger.debug("Fetched revisions %s:%s", from_revision, to_revision)

        if not force and mark:
            git.call("config", "--local", "--bool", mark, "true")

    if one_by_one:
        svn_url = get_svn_url_for_git_svn_remote(git, git_svn_remote)
        svn_revisions = get_svn_revisions(svn, svn_url)
        revisions = sorted(svn_revisions)
    else:
        revisions = [174922, 907137, 2359113]  # revisions that break the history

    current_revision = 0
    for next_revision in revisions:
        _impl(current_revision, next_revision - 1, log_window_size=100000)
        _impl(next_revision - 1, next_revision, log_window_size=1)
        current_revision = next_revision
    _impl(current_revision, "HEAD", log_window_size=100000)

    logger.debug("Fetched git-svn remote '%s'", git_svn_remote)


def strip_tree_of_symlinks(git, treeish):
    tree_str = git.call("ls-tree", "-z", treeish, cwd=git.work_dir)
    tree_item_list = tree_str.strip('\0').split('\0')
    new_tree_str = ""
    for item in tree_item_list:
        meta, object_name = item.split('\t')
        mode, object_type, object_hash = meta.split()
        if stat.S_ISLNK(int(mode, 8)):
            assert object_type == "blob"
            continue
        if object_type == "tree":
            object_hash = strip_tree_of_symlinks(git, object_hash)
        new_tree_str += "{mode} {object_type} {object_hash}\t{object_name}\0".format(
            mode=mode,
            object_type=object_type,
            object_hash=object_hash,
            object_name=object_name,
        )
    stripped_tree_hash = git.call("mktree", "-z", input=new_tree_str).rstrip("\n")
    return stripped_tree_hash


def check_striped_symlink_tree_equal(git, treeish1, treeish2):
    msg = (
        "Trees {tree1} and {tree2} are different.\n"
        "Use following command to check the diff:\n"
        " $ git --git-dir {git_dir} diff {tree1} {tree2}\n".format(
            tree1=treeish1,
            tree2=treeish2,
            git_dir=git.git_dir,
        )
    )
    if strip_tree_of_symlinks(git, treeish1) != strip_tree_of_symlinks(git, treeish2):
        raise ArcadiaSyncError(msg)


def check_git_version(git):
    version = git.call("version")
    match = re.match(r"git version (\d+)\.(\d+)", version)
    if not match:
        raise ArcadiaSyncError("Unable to determine git version")
    major, minor = map(int, [match.group(1), match.group(2)])
    if (major, minor) < (1, 8):
        raise ArcadiaSyncError("git >= 1.8 is required")


def check_svn_url(svn, url):
    if not svn.test("info", url):
        raise ArcadiaSyncError("Cannot establish connection to SVN or URL is missing")


def verify_recent_svn_revision_merged(git, git_svn_id):
    svn_url = get_svn_url_for_git_svn_remote(git, git_svn_id)
    svn = Svn()
    recent_revision = svn_get_last_modified_revision(svn, svn_url)
    if "yt:git_commit:" in svn_get_log_message(svn, svn_url, recent_revision):
        # Recent revision is our push so everything is ok
        return
    svn_url = get_svn_url_for_git_svn_remote(git, git_svn_id)
    git_log_pattern = "^git-svn-id: {}@{}".format(svn_url, recent_revision)
    log = git.call("log", "--grep", git_log_pattern)
    if not log.strip():
        raise ArcadiaSyncError("Svn revision {} is not merged to git.\n"
                         "Use --ignore-unmerged-svn-commits flag to skip this check.\n".format(recent_revision))

def svn_get_last_modified_revision(svn, url):
    xml_svn_info = svn.call("info", "--xml", url)
    tree = ElementTree.fromstring(xml_svn_info)
    commit_lst = tree.findall("entry/commit")
    assert len(commit_lst) == 1
    return commit_lst[0].get("revision")

def svn_get_log_message(svn, url, revision):
    xml_svn_info = svn.call("log", "--xml", url, "--revision", str(revision))
    tree = ElementTree.fromstring(xml_svn_info)
    msg_list = tree.findall("logentry/msg")
    assert len(msg_list) == 1
    return msg_list[0].text


@contextmanager
def temporary_directory():
    d = tempfile.mkdtemp()
    logger.debug("created temporary directory: {}".format(d))
    try:
        yield d
    finally:
        shutil.rmtree(d)
        logger.debug("removing temporary directory: {}".format(d))


def local_svn_iter_changed_files(local_svn, relpath):
    return (status for status in local_svn.iter_status(relpath) if status.status not in ("normal", "unversioned"))

def git_ls_files(git, pathspec):
    output = git.call("ls-files", "-z", "--full-name", pathspec)
    return [path for path in output.strip('\000').split('\000') if path not in GIT_EXCLUDES]

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

def indented_lines(lines, ident_size=2):
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
        raise ArcadiaSyncError("remote repo doesn't contain HEAD")

def check_head_is_attached(git):
    msg = (
        "Repository '{repo_root}' is in 'detached HEAD' state.\n"
        "Please go to that repo and checkout some branch.\n").format(
            repo_root=git.work_dir)

    try:
        ref = subprocess.check_output([
            "git", "symbolic-ref", "HEAD"
        ], cwd=git.work_dir)
    except subprocess.CalledProcessError:
        raise ArcadiaSyncError(msg)

    if not ref.startswith("refs/heads/"):
        raise ArcadiaSyncError(msg)


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
        raise ArcadiaSyncError(
            "svn working copy doesn't match git repo\n"
            "files that are in git and not in svn:\n\n"
            "{only_in_git}\n"
            "files that are in svn and not in git:\n\n"
            "{only_in_svn}".format(
                only_in_git=indented_lines(only_in_git),
                only_in_svn=indented_lines(only_in_svn),
            ))

    diffed = []
    for relpath in git_rel_paths:
        svn_path = local_svn.abspath(os.path.join(svn_relpath, relpath))
        git_path = git_abspath(git, os.path.join(git_relpath, relpath))
        if not filecmp.cmp(svn_path, git_path):
            diffed.append(relpath)
    if diffed:
        raise ArcadiaSyncError(
            "Some files in svn working copy differs from corresponding files from git repo:\n"
            "{diffed}\n".format(
                diffed=indented_lines(diffed)))


class PushState(object):
    __slots__ = ["_data", "_arcadia_dir"]

    def __init__(self, arcadia_dir, projects=None):
        self._arcadia_dir = arcadia_dir
        self._data = {}
        if projects is not None:
            self.project_list = projects

    def _data_accessor_property(key):
        def get_func(self):
            return self._data.get(key, None)

        def set_func(self, value):
            self._data[key] = value

        return property(get_func, set_func)

    project_list = _data_accessor_property("project_list")
    commit = _data_accessor_property("commit")
    review_id = _data_accessor_property("review_id")
    review_path_list = _data_accessor_property("review_path_list")

    def save(self):
        file_name = self._get_file_name(self._arcadia_dir)
        tmp_file_name = file_name + ".tmp"
        with open(tmp_file_name, "w") as outf:
            json.dump(self._data, outf)
        os.rename(tmp_file_name, file_name)

    @classmethod
    def load(cls, arcadia_dir):
        filename = cls._get_file_name(arcadia_dir)

        copy_to_local_svn_cmd = "{ARGV0} copy-to-local-svn --arcadia {arcadia}".format(ARGV0=ARGV0, arcadia=arcadia_dir)
        file_missing_msg = (
            "File '{filename}' doesn't exist. Are you sure you have called:\n"
            " $ {cmd} \n"
            "???\n"
            .format(
                filename=filename,
                cmd=copy_to_local_svn_cmd,
            )
        )

        if not os.path.exists(filename):
            raise ArcadiaSyncError(file_missing_msg)

        result = PushState(arcadia_dir)
        with open(filename) as outf:
            result._data = json.load(outf)

        return result

    def drop(self):
        os.remove(self._get_file_name(self._arcadia_dir))

    @staticmethod
    def _get_file_name(arcadia_dir):
        return os.path.join(arcadia_dir, ".arcadia-sync.push-status.json")


def checked_get_common_git(ctx_list):
    """
    If contexts belongs to different git repositories raises ArcadiaSyncError.
    Otherwise return common git instance.
    """

    if not ctx_list:
        raise ValueError("ctx_list must be nonempty list")

    git_dir_map = {}
    for ctx in ctx_list:
        git_dir_map.setdefault(ctx.git.git_dir, []).append(ctx)

    if len(git_dir_map) == 1:
        return git_dir_map.values()[0][0].git

    first_ctx_list, second_ctx_list = git_dir_map.values()[:2]
    raise ArcadiaSyncError(
        "Projects '{first_name}' and '{second_name}' belong to different git repos.".format(
            first_name=first_ctx_list[0].name,
            second_name=second_ctx_list[0].name,
        )
    )


def get_abi_major_minor_from_git_branch(git):
    ref = git.call("rev-parse", "--abbrev-ref", "HEAD").strip()
    match = re.match(r"^(?:pre)?stable[^/]*.*/(\d+).(\d+)$", ref)
    if not match:
        raise ArcadiaSyncError("Current branch must be either 'prestable/X.Y' or 'stable/X.Y'")
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
                    raise ArcadiaSyncError("Git is screwed up badly ://")
            else:
                logger.warn(
                    "SVN commit for revision %s was rewritten: %s -> %s",
                    revision, head_commit, remote_commit)
                git.call("replace", head_commit, remote_commit)

    git.call("pack-refs", "--all")


###################################################################################################################
## Actions
###################################################################################################################


def action_init(ctx, args):
    init_git_svn(ctx.git, ctx.arc_git_remote, ctx.arc_url)

def action_fetch(ctx, args):
    fetch_git_svn(ctx.git, ctx.svn, ctx.arc_git_remote, one_by_one=True)

def action_stitch(ctx, args):
    stitch_git_svn(ctx.git, "HEAD", ctx.arc_git_remote, ctx.arc_url)

def action_pull(ctx, args):
    logger.info("Pulling SVN revisions")

    git = ctx.git

    git_svn_remote_ref = make_remote_ref(ctx.arc_git_remote)

    pull_history = extract_pull_history(git, ctx.svn_relpath, "HEAD")
    revision_to_commit = extract_git_svn_revision_to_commit_mapping_as_dict(git, ctx.arc_url, ctx.arc_git_remote)

    base_commit, push_revision, push_commit = get_commit_pair_for_push(ctx, args.recent_push)
    last_changed_revision = get_svn_last_changed_revision(ctx.svn, ctx.arc_url)

    if last_changed_revision in revision_to_commit:
        logger.info(
            "Last changed revision is %s (%s)",
            last_changed_revision, abbrev(revision_to_commit[last_changed_revision]))
    else:
        logger.warning(
            "Last changed revision is %s, but it is missing in git-svn log (try fetch latest revisions)",
            last_changed_revision)

    if pull_history:
        last_pull_revision, last_pull_commit = pull_history[0]
        logger.info(
            "Last pulled revision is %s (%s) in %s",
            last_pull_revision, abbrev(last_pull_commit), abbrev(last_pull_commit))
    else:
        last_pull_revision, last_pull_commit = 0, None
        logger.info(
            "No pulls from SVN to Git were detected")

    if args.revision:
        pull_revision = args.revision
    else:
        pull_revision = last_changed_revision

    if pull_revision == push_revision:
        raise ArcadiaSyncError("Nothing to pull: everything is up-to-date")

    if pull_revision <= push_revision or pull_revision > last_changed_revision:
        raise ArcadiaSyncError("Pulled revision %s is out of range; expected > last push %s and <= last changed %s" % (
            pull_revision, push_revision, last_changed_revision))

    if pull_revision <= last_pull_revision:
        raise ArcadiaSyncError("Pulled revision %s is already merged during pull %s in commit %s" % (
            pull_revision, last_pull_revision, last_pull_commit))

    if pull_revision not in revision_to_commit:
        raise ArcadiaSyncError("Pulled revision %s is missing in remote history" % pull_revision)

    pull_commit = revision_to_commit[pull_revision]

    logger.info(
        "Pulling revisions %s:%s (%s..%s)",
        push_revision, pull_revision, abbrev(push_commit), abbrev(pull_commit))

    if not git.is_ancestor(base_commit, "HEAD"):
        raise ArcadiaSyncError("Most recent push is not an ancestor of HEAD")

    if not git.is_ancestor(push_commit, git_svn_remote_ref):
        raise ArcadiaSyncError("Most recent push is missing in SVN history (diverged git-svn sync?)")

    merge_branch = "arcadia_merge_%s" % pull_revision
    head_branch = git.call("symbolic-ref", "--short", "HEAD").strip()

    if git.has_ref(make_head_ref(merge_branch)):
        raise ArcadiaSyncError("Merge branch '%s' already exists; delete it before pulling" % merge_branch)

    if last_pull_revision < push_revision:
        logger.debug("Using last push as merge base")
        git.call("branch", merge_branch, base_commit)
        git.call("checkout", merge_branch)
        graft_message = (
            "Graft Arcadia push-commit %s\n"
            "\n"
            "yt:git_base_commit:%s\n"
            "yt:git_push_commit:%s\n"
            "yt:svn_revision:%s\n"
        ) % (abbrev(push_commit), base_commit, push_commit, push_revision)
        git.call("merge", "-X", "subtree=" + ctx.git_relpath, "-m", graft_message, "--allow-unrelated-histories", push_commit)
    else:
        logger.debug("Using last pull as merge base")
        git.call("branch", merge_branch, last_pull_commit)
        git.call("checkout", merge_branch)

    merge_message = (
        "Pull %s from Arcadia revision %s\n"
        "\n"
        "yt:git_svn_commit:%s\n"
        "yt:svn_revision:%s\n"
    ) % (ctx.svn_relpath, pull_revision, pull_commit, pull_revision)

    git.call("merge", "-X", "subtree=" + ctx.git_relpath, "-m", merge_message, pull_commit)
    git.call("checkout", head_branch)

    print >>sys.stderr, (
        "Now, run:\n"
        " $ git merge {branch}\n"
    ).format(branch=merge_branch)


def action_cherry_pick(ctx, args):
    revision_to_commit = extract_git_svn_revision_to_commit_mapping_as_dict(ctx.git, ctx.arc_url, make_remote_ref(ctx.arc_git_remote))
    commit = revision_to_commit.get(args.revision, None)
    if commit is None:
        raise ArcadiaSyncError(
            "Cannot find revision {revision}. Make sure that you fetched it from arcadia:\n"
            " $ {argv0} fetch {project}\n"
            .format(
                revision=args.revision,
                argv0=ARGV0,
                project=args.project)
        )

    with temporary_directory() as tmp_dir_name:
        ctx.git.call(
            "format-patch",
            "--output-directory", tmp_dir_name,
            "{commit}^..{commit}".format(commit=commit)
        )

        file_list = []
        for name in os.listdir(tmp_dir_name):
            file_list.append(os.path.join(tmp_dir_name, name))
            
        ctx.git.call(
            "am",
            "--directory", ctx.git_relpath,
            *file_list
        )

def get_commit_pair_for_push(ctx, recent_push=None):
    """
    Return a namedtuple with items:
        - base_commit -- commit in git history that we pushed into arcadia
        - push_revision -- revision in svn that represents push of base_commit
        - push_commit -- commit in git-svn that corresponds to push_revision
    """
    PushCommitPair = collections.namedtuple("PushCommitPair", ["base_commit", "push_revision", "push_commit"])

    check_head_is_attached(ctx.git)

    git_svn_remote_ref = make_remote_ref(ctx.arc_git_remote)

    if recent_push:
        base_commit = recent_push.git_commit
        push_revision = recent_push.svn_revision
        push_commit = translate_svn_revision_to_git_commit(ctx.git, ctx.arc_git_remote, push_revision)
    else:
        push_history = extract_push_history(ctx.name, ctx.git, ctx.svn_relpath, git_svn_remote_ref)
        if not push_history:
            raise ArcadiaSyncError("No pushes from Git to SVN were detected")
        base_commit, push_commit = push_history[0]
        push_revision = translate_git_commit_to_svn_revision(ctx.git, ctx.arc_git_remote, push_commit)

    logger.info(
        "Most recent push was from commit %s to revision %s (%s)",
        abbrev(base_commit), push_revision, abbrev(push_commit))

    try:
        error_msg = (
            "Content of revision {svn_revision} in svn doesn't match content of git commit {git_commit}\n"
            "Please use --recent-push option to set svn revision and git commit that match each other\n"
        ).format(
            svn_revision=push_revision,
            git_commit=base_commit,
        )
        check_striped_symlink_tree_equal(ctx.git, base_commit + ":" + ctx.git_relpath, push_commit + ":")
    except ArcadiaSyncError as e:
        raise ArcadiaSyncError(error_msg + str(e))

    return PushCommitPair(base_commit, push_revision, push_commit)


def action_check_push(ctx, args):
    base_commit, push_revision, push_commit = get_commit_pair_for_push(ctx, args.recent_push)
    logger.debug("Found push, base_commit: {base_commit} push_commit: {push_commit}".format(
        base_commit=base_commit,
        push_commit=push_commit,
    ))

    svn = Svn()
    svn_commit_msg = svn.call("log", ctx.arc_url, "--revision", str(push_revision), "--limit=1")
    origin_master = "origin/master"
    if not ctx.git.is_ancestor(base_commit, origin_master):
        raise ArcadiaSyncError(
            "Found push:\n"
            "{svn_commit_msg}"
            "But push commit {base_commit} is not ancestor of {origin_master}.\n"
            "You need to merge it."
            .format(
                svn_commit_msg=indented_lines(svn_commit_msg.strip().split("\n")),
                origin_master=origin_master,
                base_commit=base_commit
            )
        )

    print >>sys.stderr, (
        "Found push:\n"
        "{svn_commit_msg}"
        "and it looks good."
        .format(
            svn_commit_msg=indented_lines(svn_commit_msg.strip().split("\n"))
        )
    )


class ArcadiaPush(object):
    def __init__(
        self,
        args,
        ctx_list=None,
        check_svn_is_clean=True,
        check_head_matches_push_state=True,
        check_svn_matches_git=True
    ):
        self.args = args
        self.local_svn = LocalSvn(args.arcadia)
        if ctx_list is None:
            self.push_state = PushState.load(args.arcadia)
            self.ctx_list = [create_project_context(p) for p in self.push_state.project_list]
        else:
            self.push_state = PushState(args.arcadia, [ctx.name for ctx in ctx_list])
            self.ctx_list = ctx_list

        self.common_git = checked_get_common_git(self.ctx_list)

        logging.info("checking that working tree is clean")
        check_git_working_tree(self.common_git, can_be_disabled=False)

        if check_head_matches_push_state:
            head = self.common_git.resolve_ref("HEAD")
            error_msg = (
                "Review\n"
                "  {url}\n"
                "was created from git commit\n"
                "  {expected_head}\n"
                "which is not current HEAD ({actual_head}).\n"
                "Please reset HEAD to that commit or recreate review request.\n"
                .format(
                    url=get_review_url(self.push_state.review_id),
                    expected_head=self.push_state.commit,
                    actual_head=head
                )
            )
            if head != self.push_state.commit:
                raise ArcadiaSyncError(error_msg)

        if getattr(args, "check_head_is_pushed", True):
            logger.info("checking that HEAD is present at github")
            git_verify_head_pushed(self.common_git)

        for ctx in self.ctx_list:
            if args.check_unmerged_svn_commits:
                logger.info("check that svn doesn't have any commits that are not merged to github (project: {project})".format(project=ctx.name))
                verify_recent_svn_revision_merged(ctx.git, ctx.arc_git_remote)

        if check_svn_is_clean:
            for ctx in self.ctx_list:
                logger.info("check svn repository for local modifications (project: {project})".format(project=ctx.name))
                changed_files = list(local_svn_iter_changed_files(self.local_svn, ctx.svn_relpath))
                if changed_files and not args.ignore_svn_modifications:
                    raise ArcadiaSyncError(
                        "svn repository has uncommited changed:\n"
                        "{changed_files}\n"
                        "Use --ignore-svn-modifications to ignore them.\n".format(
                            changed_files=indented_lines(["{0} {1}".format(s.status, s.relpath) for s in changed_files])))
                self.local_svn.revert(ctx.svn_relpath)

        if check_svn_matches_git:
            for ctx in self.ctx_list:
                logger.info("check svn content matches git (project: {project})".format(project=ctx.name))
                verify_svn_match_git(self.common_git, ctx.git_relpath, self.local_svn, ctx.svn_relpath)

    def copy_to_local_svn(self):
        for ctx in self.ctx_list:
            logger.info("copying files to arcadia directory (project: {project})".format(project=ctx.name))
            rmrf(self.local_svn.abspath(ctx.svn_relpath))

            # Copy files
            git_rel_file_list = list(git_iter_files_to_sync(ctx.git, ":/" + ctx.git_relpath))
            svn_rel_file_list = list(iter_relpath_translate(git_rel_file_list, ctx.git_relpath, ctx.svn_relpath))
            assert len(git_rel_file_list) == len(svn_rel_file_list)
            for rel_git_file, rel_svn_file in itertools.izip(git_rel_file_list, svn_rel_file_list):
                git_file = git_abspath(ctx.git, rel_git_file)
                svn_file = self.local_svn.abspath(rel_svn_file)

                svn_dir = os.path.dirname(svn_file)
                if not os.path.exists(svn_dir):
                    os.makedirs(svn_dir)
                shutil.copy2(git_file, svn_file)

            logger.info("notify svn about changes (project: {project})".format(project=ctx.name))
            notify_svn(self.local_svn, ctx.svn_relpath, svn_rel_file_list)
        self.push_state.save()

    
    def create_review(self, projects_only):
        head = self.common_git.resolve_ref("HEAD")
        commit_message = (
            "Push {svn_path_list} to arcadia\n"
            "\n"
            "__BYPASS_CHECKS__\n"
            "[run large tests]\n"
            "yt:git_commit:{head}\n"
            "YABS-666\n".format(
                svn_path_list=", ".join(ctx.svn_relpath + "/" for ctx in self.ctx_list),
                head=head
            )
        )
        for project in self.push_state.project_list:
            commit_message += "yt:arcadia-sync:project:{project}\n".format(project=project)

        path_list = []
        if projects_only:
            for ctx in self.ctx_list:
                path_list.append(ctx.svn_relpath)
        else:
            path_list.append(".")

        review_id = self.local_svn.create_review(commit_message, path_list=path_list)
        self.push_state.review_id = review_id
        self.push_state.commit = head
        self.push_state.review_path_list = path_list
        self.push_state.save()

    def update_review(self):
        self.local_svn.update_review(self.push_state.review_id, self.push_state.review_path_list)

    def merge_review(self):
        self.local_svn.merge_review(self.push_state.review_id, self.push_state.review_path_list)
        self.push_state.drop()

    def get_arcadia_abspath(self, relpath):
        return self.local_svn.abspath(relpath)

    def get_review_id(self):
        return self.push_state.review_id


def action_copy_to_local_svn(ctx_list, args):
    arcadia_push = ArcadiaPush(
        args,
        ctx_list=ctx_list,
        check_head_matches_push_state=False,
        check_svn_matches_git=False,
    )
    arcadia_push.copy_to_local_svn()

    print >>sys.stderr, (
        "====================================================\n"
        "All files have beed copied to svn working copy. Changed directories:\n"
        "{arcadia_project_path}"
        "Please go to svn working copy and make sure that everything is ok. Once you are done run:\n"
        " $ {script} create-review --arcadia {arcadia}\n"
        "to create review request."
    ).format(
        arcadia=arcadia_push.get_arcadia_abspath(""),
        arcadia_project_path=indented_lines(arcadia_push.get_arcadia_abspath(ctx.svn_relpath) for ctx in ctx_list),
        script=sys.argv[0]
    )


def action_create_review(args):
    arcadia_push = ArcadiaPush(
        args,
        check_svn_is_clean=False,
        check_head_matches_push_state=False
    )
    arcadia_push.create_review(args.projects_only)

    print >>sys.stderr, (
        "Review is created: {url}\n"
        "To commit it when all checks are done and 'Ship It' received use:\n"
        " $ {argv0} merge-review --arcadia {arcadia}\n"
        "To update review use:\n"
        " $ {argv0} update-review --arcadia {arcadia}\n"
        .format(
            url=get_review_url(arcadia_push.get_review_id()),
            argv0=ARGV0,
            arcadia=arcadia_push.get_arcadia_abspath(""),
        )
    )


def action_print_active_review(args):
    push_state = PushState.load(args.arcadia)
    if push_state.review_id is None:
        raise ArcadiaSyncError("Cannot find active review")
    print get_review_url(push_state.review_id)


def action_update_review(args):
    arcadia_push = ArcadiaPush(args, check_svn_is_clean=False)
    arcadia_push.update_review()
    print >>sys.stderr, "Review is updated: https://a.yandex-team.ru/review/{}".format(arcadia_push.get_review_id())


def action_merge_review(args):
    arcadia_push = ArcadiaPush(args, check_svn_is_clean=False)
    arcadia_push.merge_review()
    print >>sys.stderr, "Review is merged.".format(arcadia_push.get_review_id())


class BaseCtx(object):
    svn = Svn()

    @property
    def arc_url(self):
        return "{0}/{1}".format(ARC, self.svn_relpath)


class PythonCtx(BaseCtx):
    name = "python"

    arc_git_remote = "arcadia_svn_python"

    git_relpath = "yt/" # NB: this is path relative to python repo
    svn_relpath = "yt/python/yt" 

    def __init__(self):
        BaseCtx.__init__(self)
        self.git = Git(os.path.join(PROJECT_PATH, "python"))

    @classmethod
    def create(cls):
        return PythonCtx()


class YpCtx(BaseCtx):
    name = "yp"

    arc_git_remote = "arcadia_yp"
    git_relpath = "yp"
    svn_relpath = "yp"

    def __init__(self):
        BaseCtx.__init__(self)
        self.git = Git(os.path.join(PROJECT_PATH))

    @classmethod
    def create(cls):
        return YpCtx()


class YtCtx(BaseCtx):
    name = "yt"

    git_relpath = "yt"

    def __init__(self, abi_major, abi_minor):
        BaseCtx.__init__(self)
        self._abi_major = abi_major
        self._abi_minor = abi_minor
        self.git = Git(os.path.join(PROJECT_PATH))

    @property
    def _abi(self):
        return "%s_%s" % (self._abi_major, self._abi_minor)

    @property
    def svn_relpath(self):
        return "yt/{0}/yt".format(self._abi)

    @property
    def arc_git_remote(self):
        return "arcadia_svn_%s" % (self._abi)

    @classmethod
    def create(cls):
        git = Git(repo=PROJECT_PATH)
        return YtCtx(19, 4)


PROJECTS = {
    "python": PythonCtx,
    "yp": YpCtx,
    "yt": YtCtx,
}

def create_project_context(project):
    err = ArcadiaSyncError(
            "Invalid project: '{project}'.\n"
            "Choose one of:\n"
            "{project_list}".format(
                project=project, project_list=indented_lines(sorted(PROJECTS))))
    if project not in PROJECTS:
        raise err
    return PROJECTS[project].create()

def single_project_main(args):
    ctx = create_project_context(args.project)

    if args.check_git_version:
        check_git_version(ctx.git)
    if args.check_git_working_tree:
        check_git_working_tree(ctx.git)

    check_svn_url(ctx.svn, ctx.arc_url)

    args.action(ctx, args)

def multiple_project_main(args):
    ctx_list = []
    for p in sorted(set(args.project)):
        ctx = create_project_context(p)
        if args.check_git_version:
            check_git_version(ctx.git)
        if args.check_git_working_tree:
            check_git_working_tree(ctx.git)
        ctx_list.append(ctx)
    args.action(ctx_list, args)

def no_project_main(args):
    args.action(args)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-git-version-check", action="store_false", dest="check_git_version", default=True,
        help="DANGEROUS!!! do not check version of git binary")
    parser.add_argument(
        "--skip-git-working-tree-check", action="store_false", dest="check_git_working_tree", default=True,
        help="DANGEROUS!!! do not ensure that git repo doesn't have modifications")

    logging_parser = parser.add_mutually_exclusive_group()
    logging_parser.add_argument(
        "-s", "--silent", action="store_const", help="minimize logging",
        dest="log_level", const=logging.WARNING)
    logging_parser.add_argument(
        "-v", "--verbose", action="store_const", help="maximize logging",
        dest="log_level", const=logging.DEBUG)
    logging_parser.set_defaults(log_level=logging.INFO)

    subparsers = parser.add_subparsers()

    def add_single_project_parser(*args, **kwargs):
        parser = subparsers.add_parser(*args, **kwargs)
        parser.set_defaults(main=single_project_main)
        parser.add_argument("project", help="project to sync, choose among: python,yp,yt")
        return parser

    def add_multiple_project_parser(*args, **kwargs):
        parser = subparsers.add_parser(*args, **kwargs)
        parser.set_defaults(main=multiple_project_main)
        parser.add_argument("project", nargs="+", help="project to sync, choose among: python,yp,yt")
        return parser

    def add_no_projects_parser(*args, **kwargs):
        parser = subparsers.add_parser(*args, **kwargs)
        parser.set_defaults(main=no_project_main)
        return parser

    init_parser = add_single_project_parser(
        "init", help="prepare the main repository for further operations")
    init_parser.set_defaults(action=action_init)

    fetch_parser = add_single_project_parser(
        "fetch", help="fetch svn revisions from the remote repository")
    fetch_parser.set_defaults(action=action_fetch)

    stitch_parser = add_single_project_parser(
        "stitch", help="stitch svn revisions to converge git-svn histories")
    stitch_parser.set_defaults(action=action_stitch)

    pull_parser = add_single_project_parser(
        "pull", help="initiate a merge from arcadia to github")
    pull_parser.add_argument("--revision", "-r", help="svn revision to merge", type=int)
    pull_parser.add_argument(
        "--recent-push",
        metavar="<svn-revision>:<git-commit>",
        type=parse_git_svn_correspondence,
        help="recent push svn revision and corresponding git-commit (by default it is determined automatically)")
    pull_parser.set_defaults(action=action_pull)

    check_push_parser = add_single_project_parser(
        "check-push", help="check that recent push was good")
    check_push_parser.add_argument(
        "--recent-push",
        metavar="<svn-revision>:<git-commit>",
        type=parse_git_svn_correspondence,
        help="recent push svn revision and corresponding git-commit (by default it is determined automatically)")
    check_push_parser.set_defaults(action=action_check_push)

    cherry_pick_parser = add_single_project_parser(
        "cherry-pick", help="cherry pick given revision from arcadia")
    cherry_pick_parser.add_argument("--revision", "-r", help="svn revision to cherry-pick", type=int, required=True)
    cherry_pick_parser.set_defaults(action=action_cherry_pick)

    def add_arcadia_argument(p):
        p.add_argument("-a", "--arcadia", required=True, help="path to local svn working copy")

    def add_ignore_unmerged_svn_commits_argument(p):
        p.add_argument("--ignore-unmerged-svn-commits", dest="check_unmerged_svn_commits", default=True, action="store_false",
                       help="do not complain when svn has commits that are not merged into github")

    def add_ignore_not_pushed_head_argument(p):
        p.add_argument("--ignore-not-pushed-head", dest="check_head_is_pushed", default=True, action="store_false",
                       help="do not complain when HEAD is not pushed to github")

    copy_to_local_svn_parser = add_multiple_project_parser("copy-to-local-svn", help="push current git snapshot to svn working copy")
    copy_to_local_svn_parser.add_argument("--ignore-svn-modifications", default=False, action="store_true",
                                          help="ignore and override changes in svn working copy")
    add_arcadia_argument(copy_to_local_svn_parser)
    add_ignore_unmerged_svn_commits_argument(copy_to_local_svn_parser)
    add_ignore_not_pushed_head_argument(copy_to_local_svn_parser)
    copy_to_local_svn_parser.set_defaults(action=action_copy_to_local_svn)

    create_review_parser = add_no_projects_parser("create-review", help="create new review request")
    add_arcadia_argument(create_review_parser)
    add_ignore_unmerged_svn_commits_argument(create_review_parser)
    add_ignore_not_pushed_head_argument(create_review_parser)
    create_review_parser.add_argument("--projects-only", default=False, action="store_true",
                                          help="send to review only directories with selected projects (otherwise all arcadia is sent)")
    create_review_parser.set_defaults(action=action_create_review)

    print_active_review_parser = add_no_projects_parser("print-active-review", help="print link to the active review request")
    add_arcadia_argument(print_active_review_parser)
    print_active_review_parser.set_defaults(action=action_print_active_review)

    update_review_parser = add_no_projects_parser("update-review", help="update existing review request")
    add_arcadia_argument(update_review_parser)
    add_ignore_unmerged_svn_commits_argument(update_review_parser)
    add_ignore_not_pushed_head_argument(update_review_parser)
    update_review_parser.set_defaults(action=action_update_review)

    merge_review_parser = add_no_projects_parser("merge-review", help="commit review")
    add_arcadia_argument(merge_review_parser)
    add_ignore_unmerged_svn_commits_argument(merge_review_parser)
    merge_review_parser.set_defaults(action=action_merge_review)

    args = parser.parse_args()

    logger.setLevel(args.log_level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.handlers.append(handler)

    try:
        args.main(args)
    except ArcadiaSyncError as e:
        msg = str(e)
        print >>sys.stderr, msg
        if not msg.endswith("\n"):
            print >>sys.stderr, ""

        print >>sys.stderr, "Error occurred, exiting..."
        exit(1)

if __name__ == "__main__":
    main()
