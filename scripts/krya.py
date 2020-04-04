#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
krya.py: Krya is Remote YA
"""

import argparse
import collections
import json
import logging
import os
import pathlib
import random
import re
import shlex
import subprocess
import sys
import tempfile

logging.basicConfig(
    level=logging.INFO,
    format="krya :: %(asctime)s %(levelname)s %(message)s"
)

DEFAULT_CONFIG = """\
// Lines that starts with double slash are comments.
{
    // When "local_build" is set to true krya invokes ya / yall on the local machine.
    "local_build": false,

    // Host that will be used for remote build.
    // "host": "build02-myt.yt.yandex.net",
    "host": null,

    // Directory on the remote host that will be used for building.
    // NOTE: it's better to put it on the ssd or nvme disk
    // "remote_directory": "/home/username/krya-yt",
    "remote_directory": null,

    // Project filter used with when invoking "ya ide clion"
    // By default we don't include contrib/ directory.
    // This significantly speeds up indexing and build start time.
    "clion_project_filters": [
        "yt",
        "yp",
        "util",
        "library"
    ],

    // Enables YT cache that lives on Freud.
    "enable_yt_store": true,

    // Arguments that will be used when yall is invoked.
    // Example:
    // "yall_arguments": ["--do-not-force-vcs-info"],
    "yall_arguments": [],

    // Environment variables of yall.
    // Example:
    // "yall_env": {"YT_BUILD_PYTHON_VERSION": "3.6"}

    "yall_env": {}
}
"""

DID_YOU_KNOWN_THAT = [
    "you can use `logrep' (<yt-repo>/scripts/logrep) to get list of all Freud masters:\n"
    " $ logrep /freud/master\n"
    "more info at https://ya.cc/4Waoc\n",

    "you can use `logrep' (<yt-repo>/scripts/logrep) to find Hume active scheduler:\n"
    " $ logrep /hume/scheduler/@active\n"
    "more info at https://ya.cc/4Waoc\n",

    "you can use `logrep' (<yt-repo>/scripts/logrep) to find list of all Freud control proxies:\n"
    " $ logrep /hume/freud/@control\n"
    "more info at https://ya.cc/4Waoc\n",

    "you can use `logrep' (<yt-repo>/scripts/logrep) to grep Hahn secondary master logs:\n"
    " $ logrep /hahn/master/92e-640756-32de0066-b4ff2e9c/@leading --time now --grep 'Replication job scheduled'\n"
    "more info at https://ya.cc/4Waoc\n",
]


class KryaError(RuntimeError):
    pass


def get_repo_root():
    path = pathlib.PosixPath(__file__).resolve().parent
    prev = None
    while path != prev:
        if (path / ".arcadia.root").exists():
            return path
        prev = path
        path = path.parent
    raise KryaError("Cannot find repo root")

REPO_ROOT = get_repo_root()
CONFIG_FILE = REPO_ROOT / ".krya.json"
os.chdir(REPO_ROOT)


def log_command(args):
    logging.info("Running: {}".format(" ".join(shlex.quote(a) for a in args)))


def create_default_config():
    if CONFIG_FILE.exists():
        logging.info("Configuration file {} already exists. Skipping creating default config".format(CONFIG_FILE))
        return
    with open(CONFIG_FILE, "w") as outf:
        outf.write(DEFAULT_CONFIG)


def load_config():
    if not CONFIG_FILE.exists():
        raise KryaError("Cannot find krya config file, file {} doesnot exist.".format(CONFIG_FILE))

    with open(CONFIG_FILE) as inf:
        text = "".join(re.sub("^\s*//.*$", "", line, flags=re.MULTILINE) for line in inf)
    try:
        cfg = json.loads(text)
    except Exception as e:
        raise KryaError(
            "Cannot parse json configuration from {}\n"
            "{}\n".format(CONFIG_FILE, e))

    config_cls = collections.namedtuple("Config", [
        "local_build",
        "host",
        "remote_directory",
        "clion_project_filters",
        "enable_yt_store",
        "enable_dist_build",
        "yall_arguments",
        "yall_env",
    ])
    return config_cls(
        local_build=cfg.get("local_build", False),
        host=cfg.get("host", None),
        remote_directory=cfg.get("remote_directory", None),
        clion_project_filters=cfg.get("clion_project_filters", []),
        enable_yt_store=cfg.get("enable_yt_store", False),
        enable_dist_build=cfg.get("enable_dist_build", False),
        yall_arguments=cfg.get("yall_arguments", []),
        yall_env=cfg.get("yall_env", {})
    )


class DirectorySwitcher:
    def __init__(self, local_dir, remote_dir):
        self.local_dir = pathlib.PosixPath(local_dir)
        self.remote_dir = pathlib.PosixPath(remote_dir)

    def to_local(self, path):
        return replace_dir(path, self.remote_dir, self.local_dir)

    def to_remote(self, path):
        return replace_dir(path, self.local_dir, self.remote_dir)

    def is_in_local(self, path):
        return self._is_path_child_of(path, self.local_dir)

    def is_in_remote(self, path):
        return self._is_path_child_of(path, self.local_dir)

    @staticmethod
    def _is_path_child_of(path, parent):
        if isinstance(path, str):
            path = pathlib.PosixPath(path)
        if not path.is_absolute():
            return False

        prev = None
        while path != prev:
            if path == parent:
                return True
            prev = path
            path = path.parent
        return False


def replace_dir(path, from_dir, to_dir):
    if not path.startswith("/"):
        return path
    resolved_path = pathlib.PosixPath(path)
    prev = None
    p = resolved_path
    while p != prev:
        if p == from_dir:
            rel_path = resolved_path.relative_to(p)
            return str(to_dir / rel_path)
        prev = p
        p = p.parent
    return path


def create_build_command(build_cmd, args, rest_args, remote):
    cfg = load_config()
    ya_args = build_cmd
    if args.output:
        if remote:
            ya_args += ["--output", replace_dir(args.output, REPO_ROOT, cfg.remote_directory)]
        else:
            ya_args += ["--output", args.output]
    if remote:
        dir_switcher = DirectorySwitcher(REPO_ROOT, cfg.remote_directory)
        for arg in rest_args:
            if dir_switcher.is_in_local(arg):
                arg = dir_switcher.to_remote(arg)
            ya_args.append(arg)
    else:
        ya_args += rest_args

    if cfg.enable_yt_store:
        ya_args += [
            "--yt-store",
            "--yt-proxy=freud",
            "--yt-dir=//home/yt-teamcity-build/cache",
        ]

    if cfg.enable_dist_build:
        if build_cmd[0] != "./ya":
            ya_args += [
                "--yall-enable-dist-build"
            ]
        else:
            ya_args += [
                "--dist",
                "-E"
            ]

    if cfg.yall_arguments and build_cmd[0] == "./yall":
        ya_args += cfg.yall_arguments

    if cfg.yall_env and build_cmd[0] == "./yall":
        env_args = ["env"]
        for k, v in cfg.yall_env.items():
            env_args.append("{}={}".format(k,v))
        ya_args = env_args + ya_args

    return ya_args


def local_build(build_cmd, args, rest_args):
    args = create_build_command(build_cmd, args, rest_args, remote=False)
    log_command(args)
    os.execlp(args[0], *args)


def invoke_build(build_cmd, args, rest_args):
    cfg = load_config()

    if cfg.local_build:
        if not build_cmd:
            print("Nothing to push in local mode", file=sys.stderr)
            exit(0)
        local_build(build_cmd, args, rest_args)
        # Actualy local_build function never returns
        assert False

    remote_directory = cfg.remote_directory
    remote_host = cfg.host
    if remote_host is None:
        raise KryaError("'host' is not specified in configuration file {}".format(CONFIG_FILE))
    if cfg.remote_directory is None:
        raise KryaError("'remote_directory' is not specified in configuration file {}".format(CONFIG_FILE))

    rsync_dest = "{host}:{directory}".format(host=remote_host, directory=remote_directory)

    logging.info("Creating remote directory")
    subprocess.check_call([
        "ssh", remote_host, "mkdir", "-p", remote_directory
    ])
    logging.info("Syncing files to remote machine")
    with tempfile.TemporaryFile() as tmpf:
        subprocess.check_call(
            ["git", "ls-files", "--recurse-submodule"],
            stdout=tmpf)
        tmpf.seek(0)
        subprocess.check_call([
            "rsync",
            "--progress",
            "--del",
            "--compress",
            "-a",
            "--files-from=-",
            ".",
            rsync_dest],
            stdin=tmpf)

    if not build_cmd:
        return

    build_args = create_build_command(build_cmd, args, rest_args, remote=True)
    ssh_yall_args = [
        "ssh", remote_host,
        "-o", "ServerAliveInterval=10",
        "cd {directory} && realpath . && stdbuf -oL -eL {build_args}".format(
            directory=remote_directory,
            build_args=" ".join(map(shlex.quote, build_args))
        )
    ]
    log_command(ssh_yall_args)
    p = subprocess.Popen(ssh_yall_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    stdout_iter = iter(p.stdout)

    # First line in stdout is realpath of directory read it separately
    first_line = next(stdout_iter, None)
    sys.stdout.buffer.write(first_line)
    sys.stdout.flush()
    bytes_realpath_remote_directory = first_line.strip()

    bytes_repo_root = str(REPO_ROOT).encode("utf-8")
    for line in stdout_iter:
        line = line.replace(bytes_realpath_remote_directory, bytes_repo_root)
        sys.stdout.buffer.write(line)
        sys.stdout.flush()
    p.wait()

    dir_switcher = DirectorySwitcher(REPO_ROOT, cfg.remote_directory)
    if args.output:
        if dir_switcher.is_in_local(args.output):
            local_output_dir = args.output
            remote_output_dir = dir_switcher.to_remote(local_output_dir)
            rsync_src = "{}:{}".format(remote_host, remote_output_dir)
            args = [
                "rsync",
                "--include=*.h",
                "--include=*.cc",
                "--include=*.cpp",
                "--include=*/",
                "--exclude=*",
                "--compress",
                "-a",
                rsync_src + "/",
                local_output_dir + "/",
            ]
            log_command(args)
            subprocess.check_call(args)

    if p.returncode != 0:
        exit(p.returncode)

    if random.randint(0, 5) == 0:
        logging.info("showing tip of a day")
        print("\033[32m", file=sys.stderr) # switch to green color
        print("Did you known that...", file=sys.stderr)
        print(random.choice(DID_YOU_KNOWN_THAT), file=sys.stderr)
        print("\033[0m", file=sys.stderr) # switch back to neutral color


def invoke_ya_make(args, rest_args):
    return invoke_build(["./ya", "make"], args, rest_args)


def invoke_yall(args, rest_args):
    return invoke_build(["./yall"], args, rest_args)

def invoke_push(args, rest_args):
    return invoke_build([], args, None)

def create_clion_project(args, rest_args):
    os.chdir(REPO_ROOT)
    create_default_config()

    cfg = load_config()
    args = [
        "./ya",
        "ide",
        "clion",
        "-T", "YT-Server",
    ]
    for f in cfg.clion_project_filters:
        args += ["--filter", f]
    log_command(args)
    subprocess.check_call(args)

    with open("CMakeLists.txt") as inf:
        text = inf.read()

    with open("CMakeLists.txt", "w") as f:
        f.write(text.replace("COMMAND ${PROJECT_SOURCE_DIR}/ya make", "COMMAND ${PROJECT_SOURCE_DIR}/yt/scripts/krya.py ya-make"))
        f.write("""add_custom_target(yall COMMAND ${PROJECT_SOURCE_DIR}/yt/scripts/krya.py yall --build=${CMAKE_BUILD_TYPE} --output=${PROJECT_OUTPUT_DIR} --add-result=.h --add-result=.cpp --add-result=.cc --add-result=.c --add-result=.cxx --add-result=.C --no-src-links -T --no-emit-status)\n""")
        f.write("""add_custom_target(yall-yt-server COMMAND ${PROJECT_SOURCE_DIR}/yt/scripts/krya.py yall --yall-build-only=yt-server --build=${CMAKE_BUILD_TYPE} --output=${PROJECT_OUTPUT_DIR} --add-result=.h --add-result=.cpp --add-result=.cc --add-result=.c --add-result=.cxx --add-result=.C --no-src-links -T --no-emit-status)\n""")
        f.write("""add_custom_target(yall-python COMMAND ${PROJECT_SOURCE_DIR}/yt/scripts/krya.py yall --yall-build-only=python --build=${CMAKE_BUILD_TYPE} --output=${PROJECT_OUTPUT_DIR} --add-result=.h --add-result=.cpp --add-result=.cc --add-result=.c --add-result=.cxx --add-result=.C --no-src-links -T --no-emit-status)\n""")
        f.write("""add_custom_target(push COMMAND ${PROJECT_SOURCE_DIR}/yt/scripts/krya.py push)\n""")


def main():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=None)
    subparsers = parser.add_subparsers()

    push_subparser = subparsers.add_parser("push", help="Push source files to remote machine and exit")
    push_subparser.set_defaults(func=invoke_push)

    ya_make_subparser = subparsers.add_parser("ya-make", help="Invoke ya make on the remote machine")
    ya_make_subparser.set_defaults(func=invoke_ya_make)

    yall_subparser = subparsers.add_parser("yall", help="Invoke yall on the remote machine")
    yall_subparser.set_defaults(func=invoke_yall)

    clion_project_subparser = subparsers.add_parser("clion-project", help="Generate clion project that will use krya")
    clion_project_subparser.set_defaults(func=create_clion_project)

    for p in [ya_make_subparser, yall_subparser]:
        p.add_argument("-o", "--output", help=argparse.SUPPRESS)

    args, rest_args = parser.parse_known_args()

    if args.func is None:
        parser.print_help()
        exit(1)

    args.func(args, rest_args)

if __name__ == "__main__":
    try:
        main()
    except KryaError as e:
        print(e, file=sys.stdout)
        print("Error occurred, exitting...")
        exit(1)
