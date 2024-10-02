from __future__ import print_function

from yt.orm.library.cli_helpers import SvnVersionAction

from yt.wrapper.cli_helpers import ParseStructuredArgument
from yt.wrapper.common import generate_uuid

from abc import ABCMeta, abstractmethod

from argparse import ArgumentParser

import os


class LocalCli(object):
    __metaclass__ = ABCMeta

    def __init__(self, name):
        self._name = name

    @abstractmethod
    def make_instance(self, path, **kwargs):
        raise NotImplementedError

    def get_master_config_arg_name(self):
        return "--{}-master-config".format(self._name.lower())

    def start(self, id, **kwargs):
        id = id if id else generate_uuid()
        path = os.path.join(os.getcwd(), id)
        instance = self.make_instance(path, **kwargs)
        instance.start()

    def stop(self, id):
        path = os.path.join(os.getcwd(), id)
        instance = self.make_instance(path)
        instance.stop()

    def main(self):
        parser = ArgumentParser(description="Tool for managing local {}".format(self._name))
        parser.add_argument(
            "--svn-version", action=SvnVersionAction, help="show program svn version and exit"
        )
        subparsers = parser.add_subparsers(metavar="command")
        subparsers.required = True

        start_parser = subparsers.add_parser("start", help="start local {}".format(self._name))
        start_parser.set_defaults(func=self.start)
        start_parser.add_argument("--id", help="instance id")
        start_parser.add_argument(
            self.get_master_config_arg_name(),
            help="{} master config in yson format".format(self._name),
            action=ParseStructuredArgument,
        )
        start_parser.add_argument(
            "--local-yt-options",
            help="local YT options in yson format",
            action=ParseStructuredArgument,
        )
        start_parser.add_argument("--port-locks-path", help="path to port locks location")

        stop_parser = subparsers.add_parser("stop", help="stop local {}".format(self._name))
        stop_parser.set_defaults(func=self.stop)
        stop_parser.add_argument("id", help="instance id")

        args = parser.parse_args()
        func_args = dict(vars(args))
        func_args.pop("svn_version")
        func_args.pop("func")

        args.func(**func_args)
