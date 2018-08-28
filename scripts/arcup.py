#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import collections
import contextlib
import datetime
import json
import logging
import os
import re
import sys
import subprocess
import shutil
import time

from xml.etree import ElementTree

logger = logging.getLogger("Yt.Yarc")

ARC = "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"
BUILD_DIR = "buildall"

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
SCRIPT_NAME = os.path.basename(__file__)
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_PATH, ".."))
ARCUP_WORKING_PATH = os.path.join(PROJECT_PATH, ".arcup")
ARGV0 = sys.argv[0]

YALL_BUILD_MODES = [
    ["--target-platform=linux", "-DYT_ENABLE_GDB_INDEX"],
    ["--target-platform=linux"],
    ["--target-platform=darwin"],
    ["--target-platform=linux", "--yall-asan-build"],
]

# TODO: to git-svn
def svn_get_last_modified_revision(url):
    svn = Svn()
    xml_svn_info = svn.call("info", "--xml", url)
    tree = ElementTree.fromstring(xml_svn_info)
    commit_lst = tree.findall("entry/commit")
    assert len(commit_lst) == 1
    return commit_lst[0].get("revision")

def git_submodule_staged_commit(git, submodule_path):
    with changed_dir(git.work_dir):
        out = git.call("submodule", "status", "--cached", ":/" + submodule_path)
    m = re.match("^(.)([^ ]*) .*$", out)
    if m is None:
        raise RuntimeError("Unexpected output of git submodule status:\n{}\n".format(out))
    return m.group(2)

def git_get_submodule_head_version(git, submodule_path):
    out = git.call("ls-tree", "HEAD", submodule_path).rstrip("\n")
    assert "\n" not in out
    m = re.match("^([^ ]*) ([^ ]*) ([^ ]*)\t.*$", out)
    if not m:
        raise RuntimeError("Unexpected output of git ls-tree:\n{}".format(out))
    if m.group(2) != "commit":
        raise ValueError, "{} is not a submodule".format(submodule_path)
    return m.group(3)

def iter_arcadia_submodules(git):
    ArcadiaSubmodule = collections.namedtuple("ArcadiaSubmodule", ["path", "gitrepo", "commit"])

    out = git.call("config", "--file", os.path.join(PROJECT_PATH, ".gitmodules"), "--list", capture=True)
    submodule_map = {}
    for m in re.finditer("^submodule[.]([^.]*)[.]([^=]*)=(.*)$", out, re.MULTILINE):
        submodule_name = m.group(1)
        key = m.group(2)
        value = m.group(3)
        submodule_map.setdefault(submodule_name, {})[key] = value

    result = []
    for m in submodule_map.itervalues():
        url = m["url"]
        path = m["path"]
        if url.startswith("git@github.yandex-team.ru:yt/arcadia-snapshot-"):
            commit = git_get_submodule_head_version(git, ":/" + path)
            result.append(ArcadiaSubmodule(path, url, commit))
    if not result:
        raise RuntimeError, "No arcadia submodules found"
    return result

class CheckError(Exception):
    pass

class Command(object):
    Result = collections.namedtuple("Result", ["returncode", "stdout", "stderr"])

    def _impl(self, args, capture=True):
        logger.debug("<< Calling %r", args)
        stdfds = subprocess.PIPE if capture else None
        child = subprocess.Popen(args, bufsize=1, stdout=stdfds, stderr=stdfds)
        stdoutdata, stderrdata = child.communicate()
        logger.debug(
            ">> Call completed with return code %s; stdout=%r; stderr=%r",
            child.returncode, stdoutdata, stderrdata)
        return Command.Result(returncode=child.returncode,
                              stdout=stdoutdata, stderr=stderrdata)

    def call(self, *args, **kwargs):
        capture = kwargs.get("capture", True)
        raise_on_error = kwargs.get("raise_on_error", True)
        result = self._impl(args, capture=capture)
        if result.returncode != 0 and raise_on_error:
            if capture:
                raise CheckError("Call failed:\n\n" + result.stderr)
            else:
                raise CheckError("Call failed")
        else:
            return result.stdout

    def test(self, *args):
        result = self._impl(args)
        return result.returncode == 0

class StepIncompleteError(RuntimeError):
    pass

class Step(object):
    def __init__(self, project_root):
        self.project_root = project_root

    @property
    def name(self):
        raise NotImplementedError

    def run_impl(self):
        raise NotImplementedError

    def has_result(self):
        return os.path.exists(self.step_result_file())

    def load_result(self):
        if not self.has_result():
            raise StepIncompleteError("Step {} is incomplete".format(self.name))
        with open(self.step_result_file()) as inf:
            return json.load(inf)

    def run(self):
        result_file = self.step_result_file()
        if not os.path.exists(result_file):
            result = self.run_impl()

            result_dir = os.path.dirname(result_file)
            if not os.path.exists(result_dir):
                os.mkdir(result_dir)
            tmp_file = result_file + ".tmp"
            with open(tmp_file, "w") as outf:
                json.dump(result, outf)
            os.rename(tmp_file, result_file)
        # we always return result from file to make sure it's parsable
        return self.load_result()

    def step_result_file(self):
        fname = self.name + ".step"
        assert re.match("^[-_.a-zA-Z0-9]*$", fname)
        return os.path.join(self.project_root, ".arcup", self.name + ".step")

class ArcupPlanStep(Step):
    name = "000-arcup-plan"
    def __init__(self, project_root, revision=None):
        super(ArcupPlanStep, self).__init__(project_root)
        if (
            not isinstance(revision, int)
            and revision is not None
        ):
            raise TypeError("revision must be either int or None")

        self.revision = revision

    def run_impl(self):
        result = {}
        if self.revision is "None":
            raise ValueError, "revision is unknown"

        git = Git(self.project_root)
        ensure_git_tree_clean(git)

        result["initial-git-commit"] = git.resolve_ref("HEAD")
        result["revision"] = self.revision
        result["tmp-branch-name"] = "tmp-yarcup"
        result["arcadia-svn-url"] = ARC
        now_str = date= datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        result["submodule-branch-suffix"] = "{revision}/{date}".format(revision=self.revision, date=now_str)

        arcadia_submodules = result.setdefault("arcadia-submodules", [])
        for item in iter_arcadia_submodules(git):
            json_item = {}
            json_item["path"] = item.path
            json_item["gitrepo"] = item.gitrepo
            json_item["commit"] = item.commit

            #
            # We also need to save content of .git-file since we'll remove it during update
            # and we want to restore it afterwards.
            #
            # NOTE: submodules doesn't have .git-directory they have .git-file instead, that .git-file
            # contains a link to directory inside our main repo .git-directory
            #
            submodule_git_file = os.path.join(self.project_root, item.path, ".git")
            if not os.path.isfile(submodule_git_file):
                raise RuntimeError("Path {} doesn't exist or not a file".format(submodule_git_file))
            with open(submodule_git_file) as inf:
                json_item[".git-file"] = inf.read()
            arcadia_submodules.append(json_item)

        result["arcadia-updatable-files"] = [
            "ya",
            ".arcadia.root",
        ]

        return result

class ReplaceSvnStuffStep(Step):
    """
    Step
        - removes all svn content
        - checks out all YT dependencies from svn using specified revision
        - checked out stuff is commited into submodules using newly created temporary branch
    """
    name = "010-pull-svn"

    def __init__(self, project_root):
        super(ReplaceSvnStuffStep, self).__init__(project_root)

    def run_impl(self):
        plan = ArcupPlanStep(self.project_root).load_result()

        revision = plan["revision"]
        tmp_branch_name = plan["tmp-branch-name"]
        arcadia_submodules = plan["arcadia-submodules"]
        arcadia_updatable_files = plan["arcadia-updatable-files"]
        arcadia_svn_url = plan["arcadia-svn-url"]

        result = {}
        with changed_dir(PROJECT_PATH):
            #
            # First of all we remove all stuff that are going to be updated from arcadia.
            #
            for submodule in arcadia_submodules:
                force_remove(submodule["path"])
            for filename in arcadia_updatable_files:
                force_remove(filename)

            #
            # Now when we are free from arcadia stuff we initialize svn repo but don't clone any actual content yet.
            # Then we selectivly update several files we mainly require `ya` file
            # Then we run `ya make` with option checkout, so it will pull all our dependencies.
            #
            force_remove(".svn")
            svn = Svn()
            svn.call("checkout", arcadia_svn_url, "--depth=empty", "--revision", str(revision), ".")
            svn.call("update", "--revision", str(revision), *arcadia_updatable_files)

            for additional_args in YALL_BUILD_MODES:
                subprocess.check_call([
                    "./yall",
                    "--threads=0",
                    "--checkout",
                    "--thin",
                ] + additional_args)

            #
            # Now it's time to put all the stuff that we checked out from svn to our git submodules.
            # we do following:
            #   - restore .git file
            #   - create orphan branch (so our commit will not have ancestors,
            #     check https://git-scm.com/docs/git-checkout#git-checkout---orphanltnewbranchgt)
            #   - commit all the stuff svn checkouted into this orphan branch
            #
            submodule_git_file_name = {}
            for submodule in arcadia_submodules:
                submodule_git_file_name[submodule["path"]] = submodule[".git-file"]

            git = Git(self.project_root)
            files_by_submodule = {}
            for item in svn_status("."):
                if item.status == "unversioned":
                    continue
                elif item.status != "normal":
                    raise RuntimeError, "Path {0} has unexpected svn status {1}".format(item.path, item.status)

                # git can't deal with directories so we just skip them
                if not os.path.isfile(item.path):
                    continue

                if '/' not in item.path:
                    if item.path not in ['ya.make', 'ya', '.arcadia.root']:
                        raise ArcupTodoError("ya delivered unknown file to root repo: {0}".format(item.name))
                    else:
                        git.call("add", ":/" + item.path)
                    continue

                submodule, path = item.path.split('/', 1)
                file_list = files_by_submodule.setdefault(submodule, [])
                file_list.append(path)

            new_head_by_submodule = result.setdefault("updated-submodules", {})
            for submodule in arcadia_submodules:
                submodule_path = submodule["path"]
                submodule_full_path = os.path.join(self.project_root, submodule_path)
                logger.info("commiting new content of {} submodule to git".format(submodule_path))
                if not os.path.isdir(submodule_full_path):
                    git.call("rm", submodule_path)
                    continue

                with open(os.path.join(submodule_full_path, ".git"), 'w') as outf:
                    outf.write(submodule_git_file_name[submodule_path])

                with changed_dir(submodule_full_path):
                    submodule_git = Git(submodule_full_path)
                    if submodule_git.call("branch", "--list", tmp_branch_name, capture=True):
                        submodule_git.call("branch", "-D", tmp_branch_name)
                    submodule_git.call("checkout", "--orphan", tmp_branch_name)
                    submodule_git.call("rm", "-r", "--cached", "--force", ".")

                    file_list = files_by_submodule.pop(submodule_path, None)
                    if not file_list:
                        raise ArcupTodoError(
                            "Looks like we don't need submodule {0} anymore.\n"
                            "Developers of {1} didn't expect such fortune, so you'll have to handle it yourself :(\n"
                            .format(submodule_path, SCRIPT_NAME))

                    chunk_size = 100
                    while file_list:
                        curchunk = min(chunk_size, len(file_list))
                        curargs = file_list[-curchunk:]
                        del file_list[-curchunk:]
                        submodule_git.call("add", *curargs)
                    submodule_git.call("commit", "--message", "arcadia-snapshot:{0}".format(revision))
                    new_head_by_submodule[submodule_path] = submodule_git.resolve_ref("HEAD")

            if files_by_submodule:
                unknown_submodules = "".join("  - {}\n".format(s) for s in sorted(files_by_submodule.keys()))
                raise ArcupError("Svn delivered unknown directory. New submodule have to be created for them.\n"
                                 "List of unknown directories:\n{unknown_submodules}\n"
                                 "What should you do:\n"
                                 "1. Abort current update\n"
                                 "  $ {argv0} abort\n"
                                 "2. For each directory listed above create a git repo at\n"
                                 "  github.yandex-team.ru:yt/arcadia-snapshot-<directory>\n"
                                 "3. Add newly created directory as submodule:\n"
                                 "  $ git submodule add git@github.yandex-team.ru:yt/arcadia-snapshot-<directory> <directory>\n"
                                 "4. Retry update.\n"
                                 .format(
                                     unknown_submodules=unknown_submodules,
                                     argv0=ARGV0))

            return result

class RebaseArcadiaSubmoduleStep(Step):
    def __init__(self, project_root, submodule_path):
        super(RebaseArcadiaSubmoduleStep, self).__init__(project_root)
        self.submodule_path = submodule_path
        self.revision_before_arcup = None
        self.new_arcadia_snapshot = None

    @property
    def name(self):
        return "020-rebase-submodule-{}".format(self.submodule_path)

    def submodule_full_path(self):
        return os.path.join(self.project_root, self.submodule_path)

    def run_impl(self, expect_added=False):
        replace_svn_result = ReplaceSvnStuffStep(self.project_root).load_result()
        updated_submodules = replace_svn_result["updated-submodules"]
        self.new_arcadia_snapshot = updated_submodules[self.submodule_path]

        plan = ArcupPlanStep(self.project_root).load_result()
        tmp_branch_name = plan["tmp-branch-name"]
        arcadia_submodules = plan["arcadia-submodules"]
        rebase_args = None

        for item in arcadia_submodules:
            if item["path"] == self.submodule_path:
                self.revision_before_arcup = item["commit"]
                break
        else:
            raise RuntimeError, "Submodule {} is not found".format(self.submodule_path)

        #
        # Check if current submodule is already added to stage
        global_git = Git(self.project_root)
        staged_revision = git_submodule_staged_commit(global_git, self.submodule_path)
        if staged_revision != self.revision_before_arcup:
            result = {
                "rebased-commit": staged_revision,
            }
            return result
        if expect_added:
            raise RuntimeError, "Expectation failure"

        #
        # Find out range of commit that we need to reapply
        git = Git(self.submodule_full_path())
        log_str = git.call("log", "--grep=^arcadia-snapshot:[r0-9]*$", "-z", "--format=format:%H", self.revision_before_arcup)
        arcadia_snapshot_commit = None
        if log_str:
            arcadia_snapshot_commit = log_str.split("\000", 1)[0]
        elif git.resolve_ref("master") == self.revision_before_arcup:
            pass
        else:
            self.raise_cannot_rebase("cannot find arcadia-snapshot commit")

        #
        # Rebase
        #
        if (
            arcadia_snapshot_commit is not None
            and arcadia_snapshot_commit != self.revision_before_arcup
        ):
            rebase_args = (self.new_arcadia_snapshot, arcadia_snapshot_commit, self.revision_before_arcup)
            try:
                git.call("rebase", "--onto", *rebase_args)
            except CheckError as e:
                msg = "`git rebase' failed with error:\n" + str(e)
                try:
                    git.call("rebase", "--abort")
                except CheckkError as e:
                    msg = "`git rebase --abort' also failed:\n" + str(e)
                self.raise_cannot_rebase(msg, rebase_args=rebase_args)

        git.call("update-ref", "refs/heads/" + tmp_branch_name, "HEAD")
        git.call("checkout", tmp_branch_name)
        global_git.call("add", self.submodule_path)
        return self.run_impl(expect_added=True)

    def raise_cannot_rebase(self, reason, rebase_args=None):
        def maybe_unknown(s):
            if s is None:
                return "<unknown>"
            return s

        if rebase_args is None:
            rebase_cmd = "unknown"
        else:
            rebase_cmd = "git rebase --onto {} {} {}".format(*rebase_args)
        msg = (
            "Rebase of submodule `{submodule}' failed:.\n"
            "{reason}\n"
            "Go to submodule directory `{submodule_full_path}' and resolve situation yourself.\n"
            "Rebase command:\n"
            " {rebase_cmd}\n"
            "Once you are done run:\n"
            " $ cd {project_root}\n"
            " $ git add {submodule}\n"
            "Then run:\n"
            " $ {cur_file} continue\n"
            "Useful info:\n"
            "  revision before arcup: {revision_before_arcup}\n"
            "  new arcadia snapshot commit: {new_arcadia_snapshot_commit}\n"
            .format(
                rebase_cmd=rebase_cmd,
                submodule=self.submodule_path,
                reason=reason,
                submodule_full_path=self.submodule_full_path(),
                project_root=self.project_root,
                cur_file=__file__,
                revision_before_arcup=maybe_unknown(self.revision_before_arcup),
                new_arcadia_snapshot_commit=maybe_unknown(self.new_arcadia_snapshot),
            ))

        raise ArcupError(msg)

class RebaseAllArcadiaSubmodulesStep(Step):
    """
    Result:
        {
            "rebased-submodules": {
                SUBMODULE: {
                    "rebased-commit": COMMIT
                },
                ...
            }
        }
    """
    name = "025-rebase-all-arcadia-submodules"

    def __init__(self, project_root):
        super(RebaseAllArcadiaSubmodulesStep, self).__init__(project_root)

    def run_impl(self):
        replace_svn_result = ReplaceSvnStuffStep(PROJECT_PATH).load_result()

        result = {}
        rebased_submodules = result.setdefault("rebased-submodules", {})
        for submodule in replace_svn_result["updated-submodules"]:
            cur_result = RebaseArcadiaSubmoduleStep(PROJECT_PATH, submodule).run()
            rebased_submodules[submodule] = {
                "rebased-commit": cur_result["rebased-commit"],
            }
        return result

class PushSubmoduleStep(Step):
    """
    Result:
        {}
    """
    def __init__(self, project_root, submodule, dry_run):
        super(PushSubmoduleStep, self).__init__(project_root)
        self.submodule = submodule
        self.dry_run = dry_run

    @property
    def name(self):
        return "030-push-submodule-{}".format(self.submodule)

    def run_impl(self):
        logger.info("Pushing submodule {}".format(self.submodule))
        plan = ArcupPlanStep(PROJECT_PATH).load_result()
        final_branch = "arcadia-fork/{submodule}/{suffix}".format(
            submodule=self.submodule, suffix=plan["submodule-branch-suffix"]
        )
        global_git = Git(self.project_root)
        submodule_git = Git(os.path.join(self.project_root, self.submodule))
        commit = git_submodule_staged_commit(global_git, self.submodule)

        if submodule_git.call("branch", "--list", final_branch, capture=True):
            submodule_git.call("branch", "-D", final_branch)
        submodule_git.call("branch", final_branch, commit)

        dry_run_args = []
        if self.dry_run:
            dry_run_args = ["--dry-run"]
        submodule_git.call("push", "origin", final_branch, *dry_run_args)
        return {}

class PushAllSubmodulesStep(Step):
    """
    Result:
        {}
    """

    name = "035-push-all-submodules"

    def __init__(self, project_root, dry_run):
        super(PushAllSubmodulesStep, self).__init__(project_root)
        self.dry_run = dry_run

    def run_impl(self):
        rebase_result = RebaseAllArcadiaSubmodulesStep(PROJECT_PATH).load_result()
        for submodule in rebase_result["rebased-submodules"]:
            PushSubmoduleStep(self.project_root, submodule, self.dry_run).run()
        return {}

class CommitUpdatesStep(Step):
    """
    Result:
        {}
    """
    name = "040-commit-updates"

    def __init__(self, project_root, dry_run):
        super(CommitUpdatesStep, self).__init__(project_root)
        self.dry_run = dry_run

    def run_impl(self):
        logger.info("commiting changes to main git repo")
        plan = ArcupPlanStep(PROJECT_PATH).load_result()
        revision = plan["revision"]
        git = Git(self.project_root)
        dry_run_args = []
        if self.dry_run:
            dry_run_args = ["--dry-run"]
        commit_message = "Updated arcadia dependencies to r{}".format(revision)
        git.call("commit", "--message", commit_message, *dry_run_args)
        return {}

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

class ArcupError(RuntimeError):
    pass

class ArcupTodoError(ArcupError):
    pass

def ensure_git_tree_clean(git):
    # git.call("update-index", "--refresh", "-q")
    if not git.test("diff-index", "--exit-code", "--ignore-submodules=untracked", "HEAD"):
        raise ArcupError("git repository has unstaged changes")
    # TODO(ermolovd) actually git diff-index exit code might be > 1 when git error occures
    # we should check it

def svn_status(path):
    SvnStatusEntry = collections.namedtuple("SvnStatusEntry", ["path", "status"])
    xml_status = subprocess.check_output(["svn", "status", "--xml", "--verbose", path])
    tree = ElementTree.fromstring(xml_status)
    for item in tree.findall("target/entry"):
        path = item.get("path")
        wc_status, = item.findall("wc-status")
        yield SvnStatusEntry(path, wc_status.get("item"))

@contextlib.contextmanager
def changed_dir(path):
    oldpath = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpath)

def force_remove(path):
    if not os.path.exists(path):
        return
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)

def do_continue():
    plan = ArcupPlanStep(PROJECT_PATH).load_result()
    git = Git(PROJECT_PATH)
    if git.call("merge-base", "HEAD", plan["initial-git-commit"]).strip() != plan["initial-git-commit"]:
        raise ArcupError(
            "Looks like you switched branch since arcadia dependencies update started.\n"
            "Not sure that it's a good idea.\n"
            "Please abort current update and start it over.\n")

    ReplaceSvnStuffStep(PROJECT_PATH).run()
    RebaseAllArcadiaSubmodulesStep(PROJECT_PATH).run()
    print >>sys.stderr, (
        "All dependencies are checkouted.\n"
        "Now you can use:\n"
        " $ {argv0} complete --confirm\n"
        "to push arcadia-dependency submodules and commit changes to main git repo.\n".format(argv0=ARGV0))


def do_complete(dry_run):
    try:
        RebaseAllArcadiaSubmodulesStep(PROJECT_PATH).load_result()
    except StepIncompleteError:
        raise ArcupError(
            "Cannot complete arcup: update is not finished.\n"
            "Use:\n"
            " $ {argv0} continue\n"
            "to continue update or\n"
            " $ {argv0} abort\n"
            "to abort it.\n".format(argv0=ARGV0))

    PushAllSubmodulesStep(PROJECT_PATH, dry_run).run()
    CommitUpdatesStep(PROJECT_PATH, dry_run).run()

    if not dry_run:
        try:
            logger.info("performing cleanup")
            do_cleanup(clean_working_copy=False)
        except:
            import traceback
            traceback.print_exc()
            raise YarcupError("Svn update was successful but exception was raised during cleanup.\n"
                              "Everything is probably ok, though cleanup should be completed manualy.\n")

def cleanup_svn_working_copy(repo_path):
    repo_path = os.path.realpath(repo_path)
    status = list(svn_status(repo_path))

    # We sort status in reverse so directory content goes before directory
    # and we are going to remove directory content before directory itself.
    status.sort(reverse=True, key=lambda x: x.path)
    for item in status:
        if item.path == repo_path:
            continue
        if item.status in ["unversioned", "missing"]:
            continue
        assert item.path.startswith('/')
        if os.path.isdir(item.path):
            if item.path == repo_path:
                continue
            git_link_file = os.path.join(item.path, ".git")
            if os.path.isfile(git_link_file):
                os.remove(git_link_file)
            shutil.rmtree(item.path)
        else:
            os.remove(item.path)

def do_cleanup(clean_working_copy=True):
    svn_dir = os.path.join(PROJECT_PATH, ".svn")
    if os.path.exists(svn_dir):
        if clean_working_copy:
            cleanup_svn_working_copy(PROJECT_PATH)
        shutil.rmtree(svn_dir)
    if os.path.exists(ARCUP_WORKING_PATH):
        shutil.rmtree(os.path.join(ARCUP_WORKING_PATH))

def subcommand_up(args):
    if args.revision is None:
        revision = int(svn_get_last_modified_revision(ARC))
    else:
        revision = args.revision

    plan_step = ArcupPlanStep(PROJECT_PATH, revision)
    if plan_step.has_result() and not args.reset:
        raise ArcupError(
            "Update in progress detected.\n"
            "Use `{argv0} up --reset' if you want to discard it.\n"
            "Use `{argv0} continue' if you want to continue it.\n".format(argv0=ARGV0))

    # We always do cleaup in the beginning in order to make sure that it's sane.
    do_cleanup()

    plan_step.run()
    assert plan_step.has_result()

    do_continue()

def subcommand_abort(args):
    planstep = ArcupPlanStep(PROJECT_PATH)
    if not planstep.has_result():
        raise ArcupError("There is no arcup in progress")
    plan = planstep.load_result()

    do_cleanup()

    git = Git(PROJECT_PATH)
    git.call("reset", "--hard", "HEAD")

    with changed_dir(PROJECT_PATH):
        # git submodule doesn't want to work when cwd is not PROJECT_PATH
        git.call("submodule", "update", "--init", "--recursive")

def subcommand_continue(args):
    do_continue()

def subcommand_complete(args):
    do_complete(args.dry_run)

def normalized_revision(rev):
    return int(rev.strip("r"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    up_parser = subparsers.add_parser("up", help="Update all arcadia dependencies")
    up_parser.add_argument("-r", "--revision", type=normalized_revision, help="arcadia svn revision to use")
    up_parser.add_argument("--reset", action="store_true", default=False, help="discard update in progress")
    up_parser.set_defaults(subcommand=subcommand_up)

    up_parser = subparsers.add_parser("continue", help="Continue updating")
    up_parser.set_defaults(subcommand=subcommand_continue)

    abort_parser = subparsers.add_parser("abort", help="Abort updating")
    abort_parser.set_defaults(subcommand=subcommand_abort)

    complete_parser = subparsers.add_parser("complete",
                                            help=("Complete updating:\n"
                                                  "push all arcadia-snapshot submodules to their origin and commit changes to local repo"))
    complete_confirm_group = complete_parser.add_mutually_exclusive_group()
    complete_confirm_group.add_argument(
        "--confirm", action="store_false", dest="dry_run", default=True,
        help="actually push submodules and commit changes (disabled by default)")
    complete_confirm_group.add_argument(
        "--dry-run", action="store_true", dest="dry_run",
        help="perform all checks but don't do commit and submodule push (useful for testing)")
    complete_parser.set_defaults(subcommand=subcommand_complete)

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
    logger.setLevel(args.log_level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.handlers.append(handler)

    # We change directory so behaviour of the script doesn't depend on the cwd.
    # (And we can't introduce bugs that fire only with particular cwd).
    os.chdir("/tmp")
    try:
        args.subcommand(args)
    except ArcupError as e:
        print >>sys.stderr, "ERROR:"
        print >>sys.stderr, e
        print >>sys.stderr, "Error occurred, exiting..."
        exit(1)
