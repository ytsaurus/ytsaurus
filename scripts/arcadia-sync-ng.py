#!/usr/bin/env python
# -*- encoding: utf8 -*-

"""
https://wiki.yandex-team.ru/yt/internal/arcadia-sync-ng/
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "git-svn"))

from git_svn_lib import (
    CheckError,
    Git,
    Svn,
    check_git_version,
    check_svn_url,
    extract_git_svn_revision_to_commit_mapping_as_dict,
    extract_git_svn_revision_to_commit_mapping_as_list,
    fetch_git_svn,
    get_svn_url_for_git_svn_remote,
    init_git_svn,
    make_remote_ref,
    parse_git_svn_correspondence,
    pull_git_svn,
)

import argparse
import collections
import filecmp
import json
import itertools
import logging
import re
import shutil
import subprocess
import tempfile

from xml.etree import ElementTree
from contextlib import contextmanager

logger = logging.getLogger("Yt.GitSvn")

ARC = "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"
GH = "git@github.yandex-team.ru:yt"

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_PATH, ".."))

ARGV0 = sys.argv[0]


class ArcadiaSyncError(RuntimeError):
    pass


def get_review_url(review_id):
    return "https://a.yandex-team.ru/review/{}".format(review_id)


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

    def _svn_ci(self, msg):
        with tempfile.NamedTemporaryFile() as tmpf:
            retcode = subprocess.call(
                [self.ya, "svn", "ci", "-m", msg],
                stderr=tmpf,
                cwd=self.root,
            )
            tmpf.seek(0)
            stderr = tmpf.read()
            return retcode, stderr

    def create_review(self, msg):
        msg += "REVIEW:NEW"
        retcode, stderr = self._svn_ci(msg)
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

    def update_review(self, review_id):
        commit_message = (
            "__BYPASS_CHECKS__\n"
            "REVIEW_UPLOAD:{review_id}\n"
            .format(review_id=review_id)
        )
        retcode, stderr = self._svn_ci(commit_message)
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
            raise ArcadiaSyncError(failed_to_create_review_msg)
        assert int(m.group(1)) == review_id

    def merge_review(self, review_id):
        commit_message = (
            "__BYPASS_CHECKS__\n"
            "REVIEW_MERGE:{review_id}\n"
            .format(review_id=review_id)
        )
        retcode, stderr = self._svn_ci(commit_message)
        error_msg = (
            "svn commit stderr:\n"
            "{stderr}\n"
            "Failed to merge review."
            .format(stderr=stderr)
        )
        if retcode != 0:
            raise ArcadiaSyncError(error_msg)


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
        raise CheckError("Svn revision {} is not merged to git.\n"
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
        raise CheckError("remote repo doesn't contain HEAD")

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
        raise CheckError(msg)

    if not ref.startswith("refs/heads/"):
        raise CheckError(msg)


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
        raise CheckError(
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
            raise CheckError(file_missing_msg)

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
    If contexts belongs to different git repositories raises CheckError.
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
    raise CheckError(
        "Projects '{first_name}' and '{second_name}' belong to different git repos.".format(
            first_name=first_ctx_list[0].name,
            second_name=second_ctx_list[0].name,
        )
    )


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
    check_head_is_attached(ctx.git)
    pull_git_svn(
        ctx.git,
        ctx.svn,
        ctx.arc_url,
        ctx.arc_git_remote,
        ctx.git_relpath,
        ctx.svn_relpath,
        revision=args.revision,
        recent_push=args.recent_push)


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


class ArcadiaPush(object):
    def __init__(
        self,
        args,
        ctx_list=None,
        check_svn_is_clean=True,
        check_head_matches_push_state=True,
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
                    raise CheckError(
                        "svn repository has uncommited changed:\n"
                        "{changed_files}\n"
                        "Use --ignore-svn-modifications to ignore them.\n".format(
                            changed_files=indented_lines(["{0} {1}".format(s.status, s.relpath) for s in changed_files])))
                self.local_svn.revert(ctx.svn_relpath)

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

    
    def create_review(self):
        head = self.common_git.resolve_ref("HEAD")
        commit_message = (
            "Push {svn_path_list} to arcadia\n"
            "\n"
            "__BYPASS_CHECKS__\n"
            "yt:git_commit:{head}\n".format(
                svn_path_list=", ".join(ctx.svn_relpath + "/" for ctx in self.ctx_list),
                head=head
            )
        )
        for project in self.push_state.project_list:
            commit_message += "yt:arcadia-sync:project:{project}\n".format(project=project)

        review_id = self.local_svn.create_review(commit_message)
        self.push_state.review_id = review_id
        self.push_state.commit = head
        self.push_state.save()

    def update_review(self):
        self.local_svn.update_review(self.push_state.review_id)

    def merge_review(self):
        self.local_svn.merge_review(self.push_state.review_id)
        self.push_state.drop()

    def get_arcadia_abspath(self, relpath):
        return self.local_svn.abspath(relpath)

    def get_review_id(self):
        return self.push_state.review_id


def action_copy_to_local_svn(ctx_list, args):
    arcadia_push = ArcadiaPush(
        args,
        ctx_list=ctx_list,
        check_head_matches_push_state=False
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
    arcadia_push.create_review()

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
        abi_major, abi_minor = get_abi_major_minor_from_git_branch(git)
        return YtCtx(abi_major, abi_minor)


PROJECTS = {
    "python": PythonCtx,
    "yp": YpCtx,
    "yt": YtCtx,
}

def create_project_context(project):
    err = CheckError(
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
    except (ArcadiaSyncError, CheckError) as e:
        msg = str(e)
        print >>sys.stderr, msg
        if not msg.endswith("\n"):
            print >>sys.stderr, ""

        print >>sys.stderr, "Error occurred, exiting..."
        exit(1)

if __name__ == "__main__":
    main()
