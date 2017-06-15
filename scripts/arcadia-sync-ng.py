#!/usr/bin/env python

"""
## Introduction

This script provides assistance functions to synchronize YT in Arcadia and in GitHub.

Main source tree is composed of different directories with two main synchronization strategies.

First, `contrib/`, `library/` and `util/` are mirrored from Arcadia to GitHub,
and YT pins a snapshot + a patchset. These folders are mirrored with `git-svn` to `upstream` branch in GitHub.
Patchset is typically referenced as `master` branch, and `master` branch forks off `upstream`.
When updating these folders, one usually pins old `master` branch under `old/YYYY_MM_DD__HH_mm_ss` name
(to make sure that old commits are reachable) and rebases `master` on top of `upstream`.

Second, `yt/` is mirrored from GitHub to Arcadia as a snapshot. `git-svn` is used to commit appropriate
subtree into SVN, and lineage is preserved with commit marks. Namely, for every push commit in SVN
contains a textual reference to original Git commit. This information is used to properly pull changes
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
(5) Call `git submodule add` (again!)
(6) Create subdirectory in `cmake/` and write `CMakeLists.txt`
(7) Add `add_subdirectory` in root `CMakeLists.txt`
"""

import os
import argparse
import logging
import sys
import re
import time

from collections import namedtuple

try:
    import subprocess32 as subprocess
except ImportError:
    if sys.version_info[:2] <= (2, 6):
        print >>sys.stderr, "Script may not work properly with Python <= 2.6" \
                            " because subprocess32 library is not installed."
    import subprocess


def strip_margin(s):
    """
    Strips left margin from multiline strings.
    This is a helper function that improves readability, see usages in the file.
    """
    r = []
    for line in s.splitlines():
        i = line.find("|")
        if i > 0:
            r.append(line[i + 1:])
    return "\n".join(r)


def trim_for_logging(s):
    """
    Trims long strings to bound length of log messages.
    """
    if len(s) > 80:
        return s[:80] + "[trimmed].."
    else:
        return s


def make_remote_ref(name):
    if name.startswith("refs/"):
        return name
    else:
        return "refs/remotes/%s" % name


def make_head_ref(name):
    if name.startswith("refs/"):
        return name
    else:
        return "refs/heads/%s" % name


def abbrev(commit):
    if commit:
        return commit[:ABBREV]
    else:
        return "(null)"


class CheckError(Exception):
    pass


class Command(object):
    Result = namedtuple("Result", ["returncode", "stdout", "stderr"])

    def _impl(self, args, capture=True):
        logging.debug("<< Calling %r", args)
        stdfds = subprocess.PIPE if capture else None
        child = subprocess.Popen(args, bufsize=1, stdout=stdfds, stderr=stdfds)
        stdoutdata, stderrdata = child.communicate()
        logging.debug(
            ">> Call completed with return code %s; stdout=%r; stderr=%r",
            child.returncode, stdoutdata, stderrdata)
        return Command.Result(returncode=child.returncode,
                              stdout=stdoutdata, stderr=stderrdata)

    def call(self, *args, **kwargs):
        capture = kwargs.get("capture", True)
        raise_on_error = kwargs.get("raise_on_error", True)
        result = self._impl(args, capture=capture)
        if result.returncode != 0 and raise_on_error:
            raise CheckError("Call failed")
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
        assert len(result) == 40
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


ABBREV = 8
ARC = "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"
GH = "git@github.yandex-team.ru:yt"

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_PATH, ".."))


def check_git_version(git):
    version = git.call("version")
    match = re.match(r"git version (\d+)\.(\d+)", version)
    if not match:
        raise CheckError("Unable to determine git version")
    major, minor = map(int, [match.group(1), match.group(2)])
    if (major, minor) < (1, 8):
        raise CheckError("git >= 1.8 is required")


def check_git_working_tree(git):
    if not git.test("diff-index", "HEAD", "--exit-code", "--quiet"):
        raise CheckError("Working tree has local modifications")
    if not git.test("diff-index", "--cached", "HEAD", "--exit-code", "--quiet"):
        raise CheckError("Index has local modifications")


def get_abi_major_minor_from_git_branch(git):
    ref = git.call("rev-parse", "--abbrev-ref", "HEAD").strip()
    match = re.match(r"^(?:pre)?stable/(\d+).(\d+)$", ref)
    if not match:
        raise CheckError("Current branch must be either 'prestable/X.Y' or 'stable/X.Y'")
    major, minor = map(int, [match.group(1), match.group(2)])
    return major, minor


def check_svn_url(svn, url):
    if not svn.test("info", url):
        raise CheckError("Cannot establish connection to Arcadia or URL is missing")


def get_svn_revisions(svn, url):
    """
    Returns all SVN revisions that affect given SVN URL.

    `git-svn` may skip some revisions in rare circumstances that happen quite often in Arcadia.
    So we explicitly check that we fetch all necessary revisions.
    """

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
    lines = svn.call("info", url).splitlines()

    revision = None
    for line in lines:
        if line.startswith("Last Changed Rev"):
            revision = int(line.split(":")[1])
    return revision


def init_git_svn(git, svn_remote, svn_url):
    """
    Setup `git-svn` configuration in Git repository for a given SVN URL under a given remote name.
    """
    logging.debug("Setting up git-svn remote '%s' for '%s'", svn_remote, svn_url)

    if git.test("config", "--local", "--get", "svn-remote.%s.url" % svn_remote):
        git.call("config", "--local", "--remove-section", "svn-remote.%s" % svn_remote)
    git.call("config", "--local", "svn-remote.%s.url" % svn_remote, svn_url)
    git.call("config", "--local", "svn-remote.%s.fetch" % svn_remote, ":" + make_remote_ref(svn_remote))
    git.call("svn", "--svn-remote", svn_remote, "migrate", capture=False)


def fetch_git_svn(git, svn, svn_remote, svn_url, one_by_one=False, force=False):
    """
    Fetch revisions from SVN in a tidy manner.
    """
    logging.debug("Fetching git-svn remote '%s' from '%s'", svn_remote, svn_url)

    def _impl(from_revision, to_revision, log_window_size=1):
        if from_revision != "HEAD" and to_revision != "HEAD":
            mark = "from-%s-to-%s" % (from_revision, to_revision)
            mark = "svn-remote.%s.fetch-%s" % (svn_remote, mark)
        else:
            mark = None

        if not force and mark:
            if git.test("config", "--local", "--bool", "--get", mark):
                logging.debug(
                    "Skipping fetching revisions %s:%s because they are marked as fetched",
                    from_revision, to_revision)
                return

        success = False
        for i in range(5):
            try:
                git.call(
                    "svn", "--svn-remote", svn_remote, "fetch", "--log-window-size", str(log_window_size),
                    "--revision", "%s:%s" % (from_revision, to_revision),
                    capture=False)
                success = True
                break
            except CheckError:
                logging.debug("Call failed, sleeping for 1s")
                time.sleep(1)

        if not success:
            raise CheckError("Call failed")

        if not force and mark:
            git.call("config", "--local", "--bool", mark, "true")
            logging.debug(
                "Marking revisions %s:%s as fetched",
                from_revision, to_revision)

        logging.debug("Fetched revisions %s:%s", from_revision, to_revision)

    if one_by_one:
        revisions = sorted(get_svn_revisions(svn, svn_url))
    else:
        revisions = [174922, 907137, 2359113]

    current_revision = 0
    for next_revision in revisions:
        _impl(current_revision, next_revision - 1, log_window_size=100000)
        _impl(next_revision - 1, next_revision, log_window_size=1)
        current_revision = next_revision
    _impl(current_revision, "HEAD", log_window_size=100000)

    logging.debug("Fetched git-svn remote '%s' up to HEAD", svn_remote)


def extract_git_svn_revision_to_commit_mapping_as_list(git, svn_url, ref):
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
    mapping = extract_git_svn_revision_to_commit_mapping_as_list(git, svn_url, ref)
    if len(mapping) != len(set(map(lambda _: _[0], mapping))):
        raise CheckError("SVN revisions are not unique in commit tree rooted at '%s' (WTF?)" % ref)
    mapping = dict(mapping)
    return mapping


def stitch_git_svn(git, ref, svn_remote, svn_url):
    """
    Stitches committed and de-facto SVN histories together.

    Rarely (but still!) SVN history gets rewritten.
    While commit patches remain the same, messages may differ.
    We assume that latest remote history is the correct one, and enumerate all commits
    that are now not reachable from remote branch reference.
    """
    logging.debug(
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
            logging.warning(
                "SVN commit for revision %s (%s) is present in history '%s' but missing in remote '%s'",
                revision, head_commit, ref, svn_remote)
            continue
        if head_commit != remote_commit:
            replaced_commit = git.resolve_ref("refs/replace/%s" % head_commit)
            if replaced_commit:
                if replaced_commit != remote_commit:
                    raise CheckError("Git is screwed up badly ://")
            else:
                logging.warn(
                    "SVN commit for revision %s was rewritten: %s -> %s",
                    revision, head_commit, remote_commit)
                git.call("replace", head_commit, remote_commit)

    git.call("pack-refs", "--all")


def translate_git_commit_to_svn_revision(git, arc_git_remote, ref):
    return int(git.call("svn", "--svn-remote", arc_git_remote, "find-rev", ref).strip())


def extract_push_history(git, abi, ref):
    """
    Extracts Git-to-SVN commit mapping.

    Invokation of `git svn commit-tree` creates a new SVN revision
    which basically overrides repository content to match given tree.

    Therefore, during next fetch from SVN, there will be a new, unmerged commit
    in `git-svn` remote branch with its tree matching the committed tree.

    However, commit-wise this new commit (called 'push commit') does not have
    a 'base commit' (one containing committed tree) as its ancestor, which
    makes merges rather difficult.

    This function returns list of pairs, ('base commit', 'push commit'),
    newest-to-oldest, which is used later to help merges.
    """

    lines = git.call(
        "--no-replace-objects",
        "log", "--grep=^yt:git_commit:", "--grep=^Push yt/%s/" % abi, "--all-match",
        "--pretty=format:BEGIN %H%n%s%n%n%b%nEND%n", ref).splitlines()

    mapping = []
    push_commit = None
    base_commit = None
    for line in lines:
        if line.startswith("BEGIN"):
            push_commit = line.split()[1]
        if line.startswith("yt:git_commit:"):
            base_commit = line.split(":")[2]
        if line.startswith("END"):
            if push_commit and base_commit:
                mapping.append((base_commit, push_commit))
            push_commit = None
            base_commit = None
    return mapping


def extract_pull_history(git, abi, ref):
    lines = git.call(
        "log", "--grep=^yt:svn_revision:", "--grep=^Pull yt/%s/" % abi, "--all-match",
        "--pretty=format:BEGIN %H%n%s%n%n%b%nEND%n", ref).splitlines()

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


def get_all_symbolic_refs(git):
    lines = git.call("show-ref").splitlines()
    refs = []
    for line in lines:
        commit, ref = line.split()
        refs.append((commit, ref))
    return refs


def action_init(ctx, args):
    init_git_svn(ctx.git, ctx.arc_git_remote, ctx.arc_url)


def action_fetch(ctx, args):
    fetch_git_svn(ctx.git, ctx.svn, ctx.arc_git_remote, ctx.arc_url, one_by_one=True)


def action_stitch(ctx, args):
    stitch_git_svn(ctx.git, "HEAD", ctx.arc_git_remote, ctx.arc_url)


def action_pull(ctx, args):
    logging.info("Pulling SVN revisions")

    pull_history = extract_pull_history(ctx.git, ctx.abi, "HEAD")
    push_history = extract_push_history(ctx.git, ctx.abi, ctx.arc_git_remote_ref)
    revision_to_commit = extract_git_svn_revision_to_commit_mapping_as_dict(
        ctx.git, ctx.arc_url, ctx.arc_git_remote_ref)

    if not push_history:
        raise CheckError("No pushes from Git to SVN were detected")

    base_commit, push_commit = push_history[0]
    push_revision = translate_git_commit_to_svn_revision(ctx.git, ctx.arc_git_remote, push_commit)

    logging.info(
        "Most recent push was from commit %s to revision %s (%s)",
        abbrev(base_commit), push_revision, abbrev(push_commit))

    last_changed_revision = get_svn_last_changed_revision(ctx.svn, ctx.arc_url)

    logging.info(
        "Last changed revision is %s (%s)",
        last_changed_revision, abbrev(revision_to_commit[last_changed_revision]))

    if pull_history:
        last_pull_revision, last_pull_commit = pull_history[0]
        logging.info(
            "Last pulled revision is %s (%s) in %s",
            last_pull_revision, abbrev(last_pull_commit), abbrev(last_pull_commit))
    else:
        last_pull_revision, last_pull_commit = 0, None
        logging.info(
            "No pulls from SVN to Git were detected")

    if args.revision:
        pull_revision = args.revision
    else:
        pull_revision = last_changed_revision

    if pull_revision <= push_revision or pull_revision > last_changed_revision:
        raise CheckError("Pulled revision %s is out of range; expected > last push %s and <= last changed %s" % (
            pull_revision, push_revision, last_changed_revision))

    if pull_revision <= last_pull_revision:
        raise CheckError("Pulled revision %s is already merged during pull %s in commit %s" % (
            pull_revision, last_pull_revision, last_pull_commit))

    if pull_revision not in revision_to_commit:
        raise CheckError("Pulled revision %s is missing in remote history" % pull_revision)

    pull_commit = revision_to_commit[pull_revision]

    logging.info(
        "Pulling revisions %s:%s (%s..%s)",
        push_revision, pull_revision, abbrev(push_commit), abbrev(pull_commit))

    if not ctx.git.is_ancestor(base_commit, "HEAD"):
        raise CheckError("Most recent push is not an ancestor of HEAD")

    if not ctx.git.is_ancestor(push_commit, ctx.arc_git_remote_ref):
        raise CheckError("Most recent push is missing in SVN history (diverged git-svn sync?)")

    merge_branch = "arcadia_merge_%s" % pull_revision
    head_branch = ctx.git.call("symbolic-ref", "--short", "HEAD").strip()

    if ctx.git.has_ref(make_head_ref(merge_branch)):
        raise CheckError("Merge branch '%s' already exists; delete it before pulling" % merge_branch)

    if last_pull_revision < push_revision:
        logging.debug("Using last push as merge base")
        ctx.git.call("branch", merge_branch, base_commit)
        ctx.git.call("checkout", merge_branch)
        graft_message = """
        |Graft Arcadia push-commit %s
        |
        |yt:git_base_commit:%s
        |yt:git_push_commit:%s
        |yt:svn_revision:%s
        """ % (abbrev(push_commit), base_commit, push_commit, push_revision)
        graft_message = strip_margin(graft_message)
        ctx.git.call("merge", "-X", "subtree=yt", "-m", graft_message, push_commit)
    else:
        logging.debug("Using last pull as merge base")
        ctx.git.call("branch", merge_branch, last_pull_commit)
        ctx.git.call("checkout", merge_branch)

    merge_message = """
    |Pull yt/%s/ from Arcadia revision %s\n
    |
    |yt:git_svn_commit:%s
    |yt:svn_revision:%s
    |""" % (ctx.abi, pull_revision, pull_commit, pull_revision)
    merge_message = strip_margin(merge_message)

    ctx.git.call("merge", "-X", "subtree=yt", "-m", merge_message, pull_commit)
    ctx.git.call("checkout", head_branch)

    logging.info("Now, run 'git merge %s'" % merge_branch)


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

        @property
        def arc_git_remote_ref(self):
            return make_remote_ref(self.arc_git_remote)

    git = Git(repo=PROJECT_PATH)
    svn = Svn()

    check_git_version(git)
    check_git_working_tree(git)
    abi_major, abi_minor = get_abi_major_minor_from_git_branch(git)

    ctx = Ctx(git, svn, abi_major, abi_minor)
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
    fetch_git_svn(ctx.git, ctx.svn, ctx.arc_git_remote, ctx.arc_url, one_by_one=False)
    ctx.git.call("fetch", ctx.gh_git_remote)
    ctx.git.call("remote", "prune", ctx.gh_git_remote)

    old_head = ctx.git.resolve_ref(ctx.gh_git_remote_ref + "/" + ctx.gh_arc_branch)
    new_head = ctx.git.resolve_ref(ctx.arc_git_remote_ref)

    assert new_head is not None

    push = False
    if old_head is None:
        push = True
    elif old_head == new_head:
        logging.info(
            "'%s' is up-to-date: %s is latest commit in '%s'",
            ctx.name, abbrev(old_head), ctx.arc_git_remote)
    elif ctx.git.is_ancestor(old_head, new_head):
        push = True
    else:
        logging.warning(
            "Upstream has diverged in '%s'! %s is not a parent for %s!",
            ctx.name, new_head, old_head)

    if push:
        logging.info(
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

    logging.info("Pinning commits reachable from '%s' (%s)", head_ref, abbrev(head_commit))

    holder_prefixes = [
        ctx.gh_git_remote_ref + "/old",
        ctx.gh_git_remote_ref + "/" + ctx.gh_arc_branch]
    held, holder_ref = check_pinning_required(ctx.git, head_commit, holder_prefixes)

    if held:
        logging.info("Commit %s is already held by reference '%s'", abbrev(head_commit), holder_ref)
    else:
        pin = "old/" + time.strftime("%Y_%m_%d__%H_%M_%S")
        ctx.git.call("push", ctx.gh_git_remote, "%s:%s" % (head_commit, make_head_ref(pin)))


def action_submodule_fast_pull(ctx, args):
    old_head = ctx.git.resolve_ref("HEAD")
    new_head = ctx.git.resolve_ref(ctx.arc_git_remote_ref)

    assert old_head is not None
    assert new_head is not None

    if old_head == new_head:
        logging.info(
            "'%s' is up-to-date: %s is latest commit in '%s'",
            ctx.name, abbrev(old_head), ctx.arc_git_remote)
    elif ctx.git.is_ancestor(new_head, old_head):
        logging.info(
            "'%s' is up-to-date: %s superseedes latest commit %s in '%s'",
            ctx.name, abbrev(old_head), abbrev(new_head), ctx.arc_git_remote)
    elif ctx.git.is_ancestor(old_head, new_head):
        logging.info(
            "Checking out '%s': %s -> %s",
            ctx.name, abbrev(old_head), abbrev(new_head))
        ctx.git.call("checkout", new_head)
    else:
        logging.warning("Manual pull required in '%s'!", ctx.name)


def git_dry_run(flag, ctx, *args):
    if flag:
        pass
        #ctx.git.call(*args)
    else:
        def _escape(s):
            if re.match(r"^[a-zA-Z0-9_]*$", s):
                return s
            if "'" in s:
                return '"' + s.replace('"', '\\"') + '"'
            else:
                return "'" + s + "'"
        print("git", map(_escape, args))


def action_submodule_fast_push(ctx, args):
    old_head = ctx.git.resolve_ref(ctx.gh_git_remote_ref + "/" + ctx.gh_branch)
    new_head = ctx.git.resolve_ref("HEAD")

    assert new_head is not None

    push = False
    if old_head is None:
        push = True
    elif old_head == new_head:
        logging.info(
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
            logging.warning("Manual push required in '%s'!", ctx.name)

    if push:
        logging.info(
            "Pushing '%s' to '%s/%s': %s -> %s",
            ctx.name, ctx.gh_git_remote, ctx.gh_branch, abbrev(old_head), abbrev(new_head))
        git_dry_run(
            args.yes, ctx,
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
        logging.info("No submodules specified; use `--submodule ...` or `--all`")

    for submodule in args.submodules:
        logging.debug("Processing submodule '%s'", submodule)

        ctx = Ctx(git=None, svn=None, name=submodule)
        git = Git(repo=os.path.join(PROJECT_PATH, ctx.relpath))
        svn = Svn()
        ctx = ctx._replace(git=git, svn=svn)

        args.action(ctx, args)


SUBMODULES = """
contrib-libs-base64
contrib-libs-c--ares
contrib-libs-double--conversion
contrib-libs-gmock
contrib-libs-grpc
contrib-libs-gtest
contrib-libs-libbz2
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
library-colorizer
library-getopt
library-http
library-lfalloc
library-malloc-api
library-openssl
library-streams-lz
library-streams-lzop
library-string_utils-base64
library-threading-future
mapreduce-yt-interface-protos
util
""".split()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)

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
        "init", help="prepare main repository for further operations")
    init_parser.set_defaults(action=action_init)

    fetch_parser = add_parser(
        "fetch", help="fetch svn revisions from remote repository")
    fetch_parser.set_defaults(action=action_fetch)

    stitch_parser = add_parser(
        "stitch", help="stitch svn revisions due to diverged git-svn history")
    stitch_parser.set_defaults(action=action_stitch)

    pull_parser = add_parser(
        "pull", help="initiate merge from arcadia to github")
    pull_parser.add_argument("--revision", "-r", help="revision to merge", type=int)
    pull_parser.set_defaults(action=action_pull)

    def add_submodule_parser(*args, **kwargs):
        parser = subparsers.add_parser(*args, **kwargs)
        parser.set_defaults(main=submodule_main, submodules=[])
        submodule_parser = parser.add_mutually_exclusive_group()
        submodule_parser.add_argument(
            "--all", action="store_const", help="apply to all submodules",
            dest="submodules", const=SUBMODULES)
        submodule_parser.add_argument(
            "--submodule", action="append", help="apply to a particular submodule",
            dest="submodules", metavar="SUBMODULE", choices=SUBMODULES)
        return parser

    submodule_init_parser = add_submodule_parser(
        "submodule-init", help="prepare submodule for further operations")
    submodule_init_parser.set_defaults(action=action_submodule_init)

    submodule_fetch_parser = add_submodule_parser(
        "submodule-fetch", help="fetch svn revisions from remote repository")
    submodule_fetch_parser.set_defaults(action=action_submodule_fetch)

    submodule_stitch_parser = add_submodule_parser(
        "submodule-stitch", help="(advanced) stitch svn revisions in diverged git-svn histories")
    submodule_stitch_parser.set_defaults(action=action_submodule_stitch)

    submodule_pin_parser = add_submodule_parser(
        "submodule-pin", help="(advanced) pin current git commit in remote repository")
    submodule_pin_parser.add_argument("--commit", "-c", default="HEAD",
                                      help="commit to pin")
    submodule_pin_parser.set_defaults(action=action_submodule_pin)

    submodule_fast_pull_parser = add_submodule_parser(
        "submodule-fast-pull", help="pull submodule up to upstream revision")
    submodule_fast_pull_parser.set_defaults(action=action_submodule_fast_pull)

    submodule_fast_push_parser = add_submodule_parser(
        "submodule-fast-push", help="push submodule to master branch")
    submodule_fast_push_parser.add_argument("--yes", "-y", action="store_true", default=False,
                                            help="do something indeed")
    submodule_fast_push_parser.set_defaults(action=action_submodule_fast_push)

    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=args.log_level)

    args.main(args)
