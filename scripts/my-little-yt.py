#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Helper script to control local yt.
"""

import argparse
import collections
import curses
import getpass
import logging
import os
import pathlib
import random
import shlex
import socket
import subprocess
import sys

import toml

ARGV0 = sys.argv[0]

Configuration = collections.namedtuple("Configuration", ["working_directory", "default_profile", "profiles"])

ProfileConfiguration = collections.namedtuple("ProfileConfiguration", [
    "yt_source_directory",
    "port",
    "use_ytserver_all",
])

class MyLittleYtError(RuntimeError):
    pass

def find_repo_root():
    path = pathlib.Path(__file__).resolve()

    while True:
        arcadia_root = path / ".arcadia.root"
        if arcadia_root.exists():
            return path
        parent = path.parent
        if parent == path:
            return None
        path = parent


def run(func, cmd):
    cmd_str = " ".join(shlex.quote(s) for s in cmd)
    logging.info("Running: $ {}".format(cmd_str))
    try:
        return func(cmd)
    except subprocess.CalledProcessError as e:
        raise MyLittleYtError(
            "Command:\n"
            "\n"
            "  $ {cmd}\n"
            "\n"
            "has exit with nonzero exit code: {code}"
            .format(
                cmd=cmd_str,
                code=e.returncode
            )
        )

def generate_configuration():
    yt_repo = find_repo_root()
    if yt_repo is None:
        yt_repo = "~/yt-src"
    return """\
# Working directory of the local YT
working_directory = "~/my-little-yt"

# Profile that will be used if no profile is specified
default_profile = "default"

# Default profile
[profile.default]
# Http port of the controller
port = {port}

# When set to true only ytserver-all binary is required.
# Otherwise all binaries: ytserver-master, ytserver-scheduler etc are required.
use_ytserver_all = false

# Directory with YT sources.
# When not specified it will be determined automatically assuming my-little-yt.py
# is inside YT repo.
# yt_source_directory = "{yt_repo}"

# # You can define other profiles, e.g
# [profile.my_other_profile]
# yt_source_directory = "~/source_dir2"
# port = 8042
    """.format(
        port=random.randrange(8000, 9000),
        yt_repo=yt_repo,
    )


def indented_lines(string_list, indent=2):
    indent_str = " " * indent
    return "\n".join(indent_str + s for s in string_list)

def get_configuration_path():
    return pathlib.Path.home() / ".my-little-yt.toml"

def get_or_generate_profile_configuration(profile):
    cfg_path = get_configuration_path()
    if not cfg_path.exists():
        if os.isatty(0) and os.isatty(1):
            answer = None
            while answer is None:
                answer_str = input(
                    "Configuration ({path}) is not found.\n"
                    "Do you want to generate default configuration? (y/n)\n"
                    .format(path=cfg_path)
                )
                if answer_str.strip().startswith("y"):
                    answer = True
                elif answer_str.strip().startswith("n"):
                    answer = False
                else:
                    print("'y' or 'n' please.")
                    print("")
            if answer:
                with open(cfg_path, 'w') as outf:
                    print(generate_configuration(), file=outf)
    result = get_configuration()
    if result:
        if not result.working_directory.exists():
            os.mkdir(result.working_directory)
    return result


def get_configuration():
    cfg_path = get_configuration_path()
    if not cfg_path.exists():
        raise MyLittleYtError(
            "No configuration found at {cfg_path}. Use:\n"
            "  $ {argv0} generate-configuration\n"
            "to generate default configuration."
            .format(
                argv0=ARGV0,
                cfg_path=cfg_path
            )
        )

    with open(get_configuration_path()) as inf:
        cfg = toml.load(inf)

    def get_type_checked(path, t, required=True):
        r = cfg
        for i,p in enumerate(path):
            if p not in r:
                if not required:
                    return None
                raise MyLittleYtError("Configuration error: {key} is not found".format(key=".".join(path[:i+1])))
            r = r[p]
        if not isinstance(r, t):
            raise MyLittleYtError(
                "Configuration error: {profile}.{key} is not a {type}"
                .format(
                    profile=profile,
                    key=key,
                    type=t
                )
            )
        return r

    working_directory = get_type_checked(["working_directory"], str)
    default_profile = get_type_checked(["default_profile"], str, required=False)
    if default_profile is None:
        default_profile = "default"
    try:
        profiles_node = get_type_checked(["profile"], dict)
    except MyLittleYtError:
        raise MyLittleYtError("No profiles are found in configuration")

    profiles = {}
    for profile_name, profile_node in profiles_node.items():
        port = get_type_checked(["profile", profile_name, "port"], int)
        use_ytserver_all = get_type_checked(["profile", profile_name, "use_ytserver_all"], bool, required=False)
        if use_ytserver_all is None:
            use_ytserver_all = False
        yt_source_directory = get_type_checked(["profile", profile_name, "yt_source_directory"], str, required=False)
        if yt_source_directory is None:
            yt_source_directory = find_repo_root()
        profiles[profile_name] = ProfileConfiguration(
            port=port,
            yt_source_directory=pathlib.Path(yt_source_directory).expanduser(),
            use_ytserver_all = use_ytserver_all
        )

    return Configuration(
        working_directory=pathlib.Path(working_directory).expanduser(),
        default_profile=default_profile,
        profiles=profiles,
    )


class LocalYt:
    def __init__(self, cfg, profile=None):
        self.cfg = cfg
        if profile:
            self.profile = profile
        else:
            self.profile = self.cfg.default_profile
        try:
            self.profile_cfg = self.cfg.profiles[self.profile]
        except KeyError:
            raise MyLittleYtError("Profile {profile} is not found".format(self.profile))
        self.source_directory = self.profile_cfg.yt_source_directory
        self.bin = self.source_directory / "python" / "yt" / "local" / "bin" / "yt_local"

    def list_instances(self):
        Instance = collections.namedtuple("Instance", ["id", "status", "status_line"])
        out = self._run_impl(subprocess.check_output, ["list"])
        result = []
        for line in out.decode("utf8").strip("\n").split("\n"):
            if not line:
                continue
            fields = line.split("\t")
            id = fields[0]
            status = fields[1].split()[1]
            result.append(Instance(id, status, line))

        return result

    def get_instance(self):
        lst = self.list_instances()
        for instance in lst:
            if instance.id == self.profile:
                return instance
        return None

    def start(self):
        args = [
            "start",
            "--enable-debug-logging",
            "--proxy-port", self.profile_cfg.port,
            "--fqdn", socket.getfqdn(),
            "--id", self.profile,
        ]
        self._run_impl(subprocess.check_call, args)

    def stop(self, instance_id=None):
        if instance_id is None:
            instance_id = self.profile
        args = [
            "stop",
            instance_id
        ]
        self._run_impl(subprocess.check_call, args)

    def delete(self):
        args = [
            "delete",
            self.profile
        ]
        self._run_impl(subprocess.check_call, args)

    def _run_impl(self, func, args):
        build_dir = self.profile_cfg.yt_source_directory / "ya-build"
        if not self.profile_cfg.use_ytserver_all:
            bin_dir = build_dir
        else:
            bin_dir = self.cfg.working_directory / "bin" / self.profile
            if not bin_dir.exists():
                bin_dir.mkdir(parents=True)
            for name in [
                "ytserver-master",
                "ytserver-clock",
                "ytserver-node",
                "ytserver-job-proxy",
                "ytserver-exec",
                "ytserver-proxy",
                "ytserver-http-proxy",
                "ytserver-tools",
                "ytserver-scheduler",
                "ytserver-controller-agent",
            ]:
                bin_name = bin_dir / name
                if bin_name.exists():
                    bin_name.unlink()
                bin_name.symlink_to(build_dir / "ytserver-all")

        cmd = [
            "env",
            "PATH={}".format(
                ":".join([
                    str(bin_dir),
                    os.environ["PATH"]
                ])
            ),
            "PYTHONPATH={}".format(self.profile_cfg.yt_source_directory / "python"),
            "HOME={}".format(pathlib.Path.home()),
            "USER={}".format(getpass.getuser()),

            "python",
            self.bin,
            "--path", self.cfg.working_directory,
        ] + args
        cmd = list(map(str, cmd))
        return run(func, cmd)

def invoke_start(args):
    cfg = get_or_generate_profile_configuration(args.profile)
    local_yt = LocalYt(cfg, args.profile)
    local_yt.start()

def invoke_stop(args):
    cfg = get_configuration()
    profile = args.profile
    if profile not in cfg.profiles:
        # If we trying to stop instance that is not in our profile list it's ok
        # Use default configuration to find local_yt
        profile = cfg.default_profile
    local_yt = LocalYt(cfg, profile)
    local_yt.stop(args.profile)

def invoke_restart(args):
    cfg = get_or_generate_profile_configuration(args.profile)
    local_yt = LocalYt(cfg, args.profile)
    instance = local_yt.get_instance()
    if instance:
        if instance.status == "running":
            local_yt.stop()
        if args.reset:
            local_yt.delete()
    local_yt.start()

def invoke_status(args):
    cfg = get_configuration()
    local_yt = LocalYt(cfg)
    instances = local_yt.list_instances()
    if not instances:
        print("<no instances are found>")
    for instance in instances:
        print(instance.status_line)

def invoke_generate_configuration(args):
    if args.stdout:
        print(generate_configuration(), file=sys.stdout)
        return

    config_path = get_configuration_path()
    if config_path.exists():
        raise MyLittleYtError(
            "Configuration already exists. Use:\n"
            "  $ {argv0} generate-configuration --stdout > {path}\n"
            "to override it."
            .format(
                argv0=ARGV0,
                path=config_path
            )
        )

    with open(config_path) as outf:
        print(generate_configuration(), file=outf)

def invoke_killall(args):
    output = run(subprocess.check_output, ["ps", "x", "-U", getpass.getuser()]).decode("utf-8")
    pid_list = []
    for line in output.strip().split("\n"):
        if "ytserver-" not in line:
            continue
        pid = int(line.split()[0])
        logging.warn("Killing {}".format(pid))
        os.kill(pid, 9)

def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.set_defaults(func=invoke_status, profile=None)

    parser.add_argument("-v", "--verbose", help="Be verbose", action="store_true")

    subparsers = parser.add_subparsers()

    def add_profile_argument(p):
        p.add_argument("profile", nargs='?', default=None, help="Profile to use")

    start_subparser = subparsers.add_parser("start", help="Start local yt")
    start_subparser.set_defaults(func=invoke_start)
    add_profile_argument(start_subparser)

    stop_subparser = subparsers.add_parser("stop", help="Stop local yt")
    stop_subparser.set_defaults(func=invoke_stop)
    add_profile_argument(stop_subparser)

    restart_subparser = subparsers.add_parser("restart", help="Restart local yt")
    restart_subparser.set_defaults(func=invoke_restart)
    restart_subparser.add_argument("--reset", action="store_true", help="Remove all local data and start brand new instance of local YT")
    add_profile_argument(restart_subparser)

    generate_configuration_subparser = subparsers.add_parser("generate-configuration", help="Generate default configuration")
    generate_configuration_subparser.add_argument("--stdout", action="store_true", help="Print configuration to stdout instead of writing it to configuration file")
    generate_configuration_subparser.set_defaults(func=invoke_generate_configuration)

    killall_subparser = subparsers.add_parser("killall", help="Kills all YT processes of current user")
    killall_subparser.set_defaults(func=invoke_killall)

    for kw in ["status", "list"]:
        status_subparser = subparsers.add_parser(kw, help="Print statuses of local yt instances")
        status_subparser.set_defaults(func=invoke_status)

    args = parser.parse_args()

    level = logging.WARN
    if args.verbose:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="my-little-yt :: %(asctime)s %(levelname)s %(message)s"
    )

    args.func(args)
    

if __name__ == "__main__":
    try:
        main()
    except MyLittleYtError as e:
        print("", file=sys.stderr)
        print("Error occurred...", file=sys.stderr)
        print("", file=sys.stderr)
        print(str(e).rstrip('\n'), file=sys.stderr)
        exit(1)
