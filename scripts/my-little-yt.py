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

ProfileConfiguration = collections.namedtuple("ProfileConfiguration", [
    "working_directory",
    "yt_source_directory",
    "port",
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

def generate_configuration():
    yt_repo = find_repo_root()
    if yt_repo is None:
        yt_repo = "~/yt-src"
    return """\
[default]

# Http port of the controller
port = {port}

# Working directory of the local YT
working_directory = "~/my-little-yt"

# Directory with YT sources.
# When not specified it will be determined automatically assuming my-little-yt.py
# is inside YT repo.
# yt_source_directory = "{yt_repo}"

# You can define other profiles
# e.g
# [my_other_profile]
# work_directory = "~/workdir2"
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
    result = get_profile_configuration(profile)
    if result:
        if not result.working_directory.exists():
            os.mkdir(result.working_directory)
    return result


def get_profile_configuration(profile):
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

    if profile not in cfg:
        raise MyLittleYtError("Profile {} is not found in my-little-yt configuration.".format(profile))

    profile_cfg = cfg[profile]

    def get_type_checked(key, t, required=True):
        if not key in profile_cfg:
            if not required:
                return None
            raise MyLittleYtError(
                "Configuration error: {profile}.{key} is not found"
                .format(
                    profile=profile,
                    key=key
                )
            )
        r = profile_cfg[key]
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

    port = get_type_checked("port", int)
    working_directory = get_type_checked("working_directory", str)
    yt_source_directory = get_type_checked("yt_source_directory", str, required=False)
    if yt_source_directory is None:
        yt_source_directory = find_repo_root()
    return ProfileConfiguration(
        port=port,
        yt_source_directory=pathlib.Path(yt_source_directory).expanduser(),
        working_directory=pathlib.Path(working_directory).expanduser(),
    )


class LocalYt:
    def __init__(self, cfg):
        self.cfg = cfg
        self.bin = self.cfg.yt_source_directory / "python" / "yt" / "local" / "bin" / "yt_local"
        self.env = {
            "PATH": "{}:{}".format(
                self.cfg.yt_source_directory / "ya-build",
                os.environ["PATH"]
            ),
            "PYTHONPATH": str(self.cfg.yt_source_directory / "python"),
            "HOME": pathlib.Path.home(),
            "USER": getpass.getuser(),
        }

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

    def get_instance(self, required=False):
        lst = self.list_instances()
        if len(lst) == 0:
            if required:
                raise MyLittleYtError("No instances of local YT are found")
            return None
        if len(lst) > 1:
            raise MyLittleYtError(
                "Too many instance of local YT are found at {directory}:\n"
                "{instance_list}"
                .format(
                    directory=self.cfg.working_directory,
                    instance_list=indented_lines(l.status_line for l in lst)
                )
            )
        return lst[0]

    def start(self, instance_id):
        args = [
            "start",
            "--enable-debug-logging",
            "--proxy-port", self.cfg.port,
            "--fqdn", socket.getfqdn()
        ]
        if instance_id:
            args += ["--id", instance_id]
        self._run_impl(subprocess.check_call, args)

    def stop(self, instance_id):
        args = [
            "stop",
            instance_id
        ]
        self._run_impl(subprocess.check_call, args)

    def delete(self, instance_id):
        args = [
            "delete",
            instance_id
        ]
        self._run_impl(subprocess.check_call, args)

    def _run_impl(self, func, args):
        cmd = [
            "env"
        ]
        for k,v in self.env.items():
            cmd += ["{}={}".format(k,v)]

        cmd += [
            "python",
            self.bin,
            "--path", self.cfg.working_directory,
        ] + args
        cmd = list(map(str, cmd))
        logging.info("Running: $ {}".format(" ".join(shlex.quote(s) for s in cmd)))
        try:
            return func(cmd)
        except subprocess.CalledProcessError as e:
            raise MyLittleYtError("Local yt script has exit with error code {}".format(e.returncode))


def invoke_start(args):
    cfg = get_or_generate_profile_configuration(args.profile)
    local_yt = LocalYt(cfg)
    instance = local_yt.get_instance()
    if instance is None:
        logging.info("Found not instance of local yt going to create a new one")
        id = None
    else:
        logging.info("Going to use local yt {}".format(instance.id))
        id = instance.id
    local_yt.start(id)

def invoke_stop(args):
    cfg = get_profile_configuration(args.profile)
    local_yt = LocalYt(cfg)
    instance = local_yt.get_instance(required=True)
    local_yt.stop(instance.id)

def invoke_restart(args):
    cfg = get_or_generate_profile_configuration(args.profile)
    local_yt = LocalYt(cfg)
    instance = local_yt.get_instance()
    if instance and instance.status == "running":
        local_yt.stop(instance.id)
    if args.reset and instance:
        local_yt.delete(instance.id)
    local_yt.start(instance.id)


def invoke_status(args):
    cfg = get_profile_configuration(args.profile)
    local_yt = LocalYt(cfg)
    instance = local_yt.get_instance(required=True)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.set_defaults(func=invoke_status, profile="default")

    parser.add_argument("-v", "--verbose", help="Be verbose", action="store_true")

    subparsers = parser.add_subparsers()

    def add_profile_argument(p):
        p.add_argument("profile", nargs='?', default="default", help="Profile to use")

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
