#!/usr/bin/env python

from __future__ import print_function

from yt.common import copy_docstring_from

import yt.wrapper.yson as yson
from yt.wrapper.cli_helpers import run_main, ParseStructuredArgument, populate_argument_help, write_silently
from yt.wrapper.common import DoNotReplaceAction
import yt.wrapper.completers as completers


import yt.wrapper as yt

import yt.clickhouse as chyt

import os
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from yt.wrapper.common import chunk_iter_rows

DESCRIPTION = '''A lightweight part of YT CLI which contains only CHYT subcommands.
"chyt ..." is equivalent to "yt clickhouse ..."'''

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

def add_hybrid_argument(parser, name, help=None, description=None, group_required=True, **kwargs):
    group = parser.add_mutually_exclusive_group(required=group_required)
    dest = None
    positional_name = name
    if "dest" in kwargs:
        dest = kwargs.pop("dest")
        positional_name = dest
    # Positional argument
    positional = add_argument(parser=group,
                              name=positional_name,
                              help=help,
                              description=description,
                              nargs="?",
                              action=DoNotReplaceAction, **kwargs)
    # Optional argument
    if dest is not None:
        kwargs["dest"] = dest
    optional = add_argument(parser=group,
                            name="--" + name.replace("_", "-"),
                            help=help,
                            description=description, **kwargs)
    return positional, optional

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
        parser = populate_argument_help(subparsers.add_parser(command_name, *args, description=help, help=pythonic_help, **kwargs))
        parser.set_defaults(func=function)

        if params_argument:
            add_structured_argument(parser, "--params", "specify additional params")
        return parser

    return add_parser

@copy_docstring_from(chyt.start_clique)
def clickhouse_start_clique_handler(*args, **kwargs):
    op = chyt.start_clique(*args, **kwargs)
    print(op.id)

def add_clickhouse_start_clique_parser(add_parser):
    parser = add_parser("start-clique", clickhouse_start_clique_handler)
    parser.add_argument("--instance-count", required=True, type=int)
    parser.add_argument("--alias", "--operation-alias", help="Alias for clique; may be also specified "
                                                             "via CHYT_PROXY env variable")
    parser.add_argument("--cypress-ytserver-clickhouse-path")
    parser.add_argument("--cypress-clickhouse-trampoline-path")
    parser.add_argument("--cypress-ytserver-log-tailer-path")
    parser.add_argument("--cypress-base-config-path", default="//sys/clickhouse/config")
    parser.add_argument("--cpu-limit", type=int)
    parser.add_argument("--cypress-geodata-path")
    parser.add_argument("--abort-existing", action="store_true", help="Abort existing operation under same alias")
    parser.add_argument("--artifact-path", help="path for artifact directory; by default equals to "
                                                "//sys/clickhouse/kolkhoz/<operation_alias>")
    parser.add_argument("--skip-version-compatibility-validation", action="store_true", help="(For developer use only)")
    add_structured_argument(parser, "--spec")
    add_structured_argument(parser, "--clickhouse-config", "ClickHouse configuration patch")
    add_structured_argument(parser, "--memory-config", "Memory configuration")

@copy_docstring_from(chyt.execute)
def clickhouse_execute_handler(**kwargs):
    class FakeStream:
        def __init__(self, stream):
            self.stream = stream
        def _read_rows(self):
            return (row + b"\n" for row in self.stream)

    settings = kwargs.pop("setting")
    if settings is not None:
        parsed_settings = {}
        for setting in settings:
            if '=' not in setting:
                raise ValueError("Invalid setting '" + setting + "'. " +
                                 "Setting is expected to be in format <key>=<value>")

            setting_key, setting_value = setting.split('=', 1)

            if setting_key in parsed_settings:
                raise ValueError("Setting with key '" + setting_key + "' occurs multiple times")

            parsed_settings[setting_key] = setting_value
        kwargs["settings"] = parsed_settings

    iterator = chunk_iter_rows(FakeStream(chyt.execute(**kwargs)), yt.config["read_buffer_size"])
    write_silently(iterator)

def add_clickhouse_execute_parser(add_parser):
    parser = add_parser("execute", clickhouse_execute_handler)
    parser.add_argument("--alias", "--operation-alias", help="Alias for clique; may be also specified "
                                                             "via CHYT_PROXY env variable")
    add_hybrid_argument(parser, "query", help="Query to execute; do not specify FORMAT in query, use --format instead")
    parser.add_argument("--format",
                        help="ClickHouse data format; refer to https://clickhouse.tech/docs/en/interfaces/formats/; "
                             "default is TabSeparated",
                        default="TabSeparated")
    parser.add_argument("--setting", action="append", help="Add ClickHouse setting to query in format <key>=<value>.")

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

    parser.add_argument("--version", action="version", version="Version: CHYT " + yt.get_version())

    subparsers = parser.add_subparsers(metavar="command")
    subparsers.required = True

    add_parser = add_subparser(subparsers)

    add_clickhouse_start_clique_parser(add_parser)
    add_clickhouse_execute_parser(add_parser)

    if "_ARGCOMPLETE" in os.environ:
        completers.autocomplete(parser, append_space=False)

    config_args, unparsed = config_parser.parse_known_args()

    if config_args.proxy is not None:
        yt.config["backend"] = "http"
        yt.config["proxy"]["url"] = config_args.proxy
    if config_args.prefix is not None:
        yt.config["prefix"] = config_args.prefix
    yt.config.COMMAND_PARAMS["trace"] = config_args.trace

    yt.config.update_config(config_args.config)

    yt.config["default_value_of_raw_option"] = True

    args = parser.parse_args(unparsed)

    func_args = dict(vars(args))

    if func_args.get("params") is not None:
        params = func_args["params"]
        for key in params:
            yt.config.COMMAND_PARAMS[key] = params[key]

    for key in ("func", "trace", "prefix", "proxy", "config", "params", "last_parser"):
        if key in func_args:
            func_args.pop(key)

    args.func(**func_args)

if __name__ == "__main__":
    run_main(main)
