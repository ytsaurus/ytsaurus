#!/usr/bin/env python

from __future__ import print_function

from yt.common import copy_docstring_from

import yt.wrapper.yson as yson
from yt.wrapper.cli_helpers import run_main, ParseStructuredArgument
import yt.wrapper.completers as completers

from yt.packages.six.moves import builtins, map as imap

import yt.wrapper as yt

import yt.clickhouse as chyt

import os
import sys
import shlex
from argparse import ArgumentParser, Action, RawDescriptionHelpFormatter

DESCRIPTION = '''A lightweight part of YT CLI which contains only CHYT subcommands. 
"chyt ..." is equivalent to "yt clickhouse ..."'''

def fix_parser(parser):
    old_add_argument = parser.add_argument
    def add_argument(*args, **kwargs):
        help = []
        if kwargs.get("required", False):
            help.append("(Required) ")
        help.append(kwargs.get("help", ""))
        if kwargs.get("action") == "append":
            help.append(" Accepted multiple times.")
        kwargs["help"] = "".join(help)
        return old_add_argument(*args, **kwargs)
    parser.add_argument = add_argument
    return parser

class ParseStructuredArguments(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, builtins.list(imap(yson._loads_from_native_str, values)))

def add_argument(parser, name, help, description, **kwargs):
    if description:
        if help:
            help = "".join([help, ". ", description])
        else:
            help = description
    return parser.add_argument(name, help=help, **kwargs)

def add_structured_argument(parser, name, help="", **kwargs):
    description = "structured %s in %s format" % (name.strip("-"), "yson")
    add_argument(parser, name, help, description=description, action=ParseStructuredArgument, action_load_method=yson._loads_from_native_str, **kwargs)

def add_subparser(subparsers, params_argument=True):
    def extract_help(help, function):
        if not help:
            help = function.__doc__.split("\n")[0]
        pythonic_help = help.strip(" .")
        pythonic_help = pythonic_help[0].lower() + pythonic_help[1:]
        return pythonic_help

    def add_parser(command_name, function=None, help=None, pythonic_help=None, *args, **kwargs):
        if pythonic_help is None:
            pythonic_help = extract_help(help, function)
        parser = fix_parser(subparsers.add_parser(command_name, *args, description=help, help=pythonic_help, **kwargs))
        parser.set_defaults(func=function)

        if params_argument:
            add_structured_argument(parser, "--params", "specify additional params")
        return parser

    return add_parser

@copy_docstring_from(chyt.start_clique)
def start_clique_handler(*args, **kwargs):
    op = chyt.start_clique(*args, **kwargs)
    print(op.id)

# TODO(max42): move to common part.
def add_start_clique_parser(add_parser):
    parser = add_parser("start-clique", start_clique_handler)
    parser.add_argument("--instance-count", required=True, type=int)
    parser.add_argument("--operation-alias", required=True, help="Alias for clique")
    parser.add_argument("--cypress-ytserver-clickhouse-path")
    parser.add_argument("--cypress-clickhouse-trampoline-path")
    parser.add_argument("--cypress-ytserver-log-tailer-path")
    parser.add_argument("--cypress-base-config-path", default="//sys/clickhouse/config")
    parser.add_argument("--cpu-limit", type=int)
    parser.add_argument("--memory-limit", type=int)
    parser.add_argument("--cypress-geodata-path")
    parser.add_argument("--abort-existing", action="store_true", help="Abort existing operation under same alias")
    parser.add_argument("--uncompressed-block-cache-size", type=int, help="Size of uncompressed block cache")
    parser.add_argument("--artifact-path", help="path for artifact directory; by default equals to "
                                                "//sys/clickhouse/kolkhoz/<operation_alias>")
    add_structured_argument(parser, "--spec")
    add_structured_argument(parser, "--clickhouse-config", "ClickHouse configuration patch")

def main():
    config_parser = ArgumentParser(add_help=False)
    config_parser.add_argument("--proxy", help="specify cluster to run command, "
                                               "by default YT_PROXY from environment")
    config_parser.add_argument("--prefix", help="specify common prefix for all relative paths, "
                                                "by default YT_PREFIX from environment")
    config_parser.add_argument("--config", action=ParseStructuredArgument, action_load_method=yson._loads_from_native_str,
                               help="specify configuration", default={})
    config_parser.add_argument("--trace", action="store_true")


    parser = ArgumentParser(parents=[config_parser],
                            formatter_class=RawDescriptionHelpFormatter,
                            description=DESCRIPTION)

    parser.add_argument("--version", action="version", version="Version: YT wrapper " + yt.get_version())

    subparsers = parser.add_subparsers(metavar="command")
    subparsers.required = True

    add_parser = add_subparser(subparsers)

    add_start_clique_parser(add_parser)

    if "_ARGCOMPLETE" in os.environ:
        completers.autocomplete(parser, enable_bash_fallback=False,
                                append_space_if_only_suggestion=False)

    config_args, unparsed = config_parser.parse_known_args()

    if config_args.proxy is not None:
        yt.config["backend"] = "http"
        yt.config["proxy"]["url"] = config_args.proxy
    if config_args.prefix is not None:
        yt.config["prefix"] = config_args.prefix
    yt.config.COMMAND_PARAMS["trace"] = config_args.trace

    if "read_progress_bar" not in config_args.config:
        config_args.config["read_progress_bar"] = {}
    if "enable" not in config_args.config["read_progress_bar"]:
        config_args.config["read_progress_bar"]["enable"] = True

    yt.config.update_config(config_args.config)

    yt.config["default_value_of_raw_option"] = True

    args = parser.parse_args(unparsed)

    func_args = dict(vars(args))

    if func_args.get("params") is not None:
        params = func_args["params"]
        for key in params:
            yt.config.COMMAND_PARAMS[key] = params[key]

    for key in ("func", "tx", "trace", "ping_ancestor_txs", "prefix", "proxy", "config", "master_cell_id", "params"):
        if key in func_args:
            func_args.pop(key)

    args.func(**func_args)

if __name__ == "__main__":
    run_main(main)
