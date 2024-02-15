from argparse import ArgumentParser, SUPPRESS

from yt.wrapper.cli_helpers import (SUBPARSER_KWARGS, add_subparser, add_argument)
import yt.wrapper.strawberry as strawberry
import yt.wrapper as yt
import yt.yson as yson

import os


def _normalize_proxy(proxy):
    for prefix in ["http://", "https://"]:
        if proxy.startswith(prefix):
            proxy = proxy[len(prefix):]

    if ":" in proxy:
        proxy = proxy.rsplit(":", 1)[0]

    return proxy.lower()


def _strawberry_ctl_handler(family):
    def handler(**kwargs):
        address = kwargs.pop("address")
        command_name = kwargs.pop("command_name")
        parser = kwargs.pop("parser")
        proxy_choices = kwargs.pop("proxy_choices")
        cluster_proxy = yt.config["proxy"]["url"]
        params = kwargs

        if _normalize_proxy(cluster_proxy) not in map(_normalize_proxy, proxy_choices):
            address = strawberry.get_full_ctl_address(address, family)
            msg = "bad cluster proxy choice: {}\n".format(cluster_proxy)
            msg += "controller {} serves only following clusters: {}\n".format(address, proxy_choices)
            msg += "set up proper cluster proxy via --proxy option or via YT_PROXY env variable"
            parser.error(msg)

        try:
            for response in strawberry.make_request_generator(
                    command_name=command_name,
                    params=params,
                    address=address,
                    family=family,
                    cluster_proxy=cluster_proxy,
                    unparsed=True):

                if "to_print" in response:
                    print(response["to_print"])
                elif "result" in response:
                    print(yson.dumps(response["result"], yson_format="pretty").decode("utf-8"))

        except Exception as e:
            msg = "failed to execute the command in controller service\n"
            msg += str(e)
            parser.error(msg)

        if "error" in response:
            exit(2)
    return handler


def add_strawberry_ctl_parser(add_parser, family):
    family_upper = family.upper()

    address_parser = ArgumentParser(add_help=False)
    address_parser.add_argument("--address", help="controller service address")
    # "help" option should not be handled before we fetch and register all available commands.
    # We will add it manually later.
    parser = add_parser("ctl", add_help=False, pythonic_help="{} controller".format(family_upper),
                        parents=[address_parser])
    # Create "subparsers" immediately to show the semantic that "command" argument is expected.
    # Otherwise, argcomplete goes crazy. Nevertheless, "subparsers" are empty before calling "register_commands".
    subparsers = parser.add_subparsers(metavar="command", **SUBPARSER_KWARGS)
    add_cmd_subparser = add_subparser(subparsers, params_argument=False)

    def register_commands(address):
        try:
            api_structure = strawberry.describe_api(address, family)
        except Exception as e:
            msg = "failed to fetch available commands from controller service\n"
            msg += str(e)
            parser.error(msg)

        parser.add_argument("-h", "--help", action="help", default=SUPPRESS,
                            help="show this help message and exit")

        parser.set_defaults(proxy_choices=api_structure["clusters"])

        for command in api_structure["commands"]:
            subparser = add_cmd_subparser(
                command["name"].replace("_", "-"),
                _strawberry_ctl_handler(family),
                pythonic_help=command.get("description"))
            subparser.set_defaults(command_name=command["name"], parser=subparser)

            for param in command.get("parameters", []):
                required = param.get("required", False)
                env_variable = param.get("env_variable")
                # Parameter is passed as a positional argument if it is required and can not be set implicitly
                # via an env variable.
                as_positional_argument = required and not env_variable
                name = param["name"]
                aliases = [("-" if len(a) == 1 else "--") + a.replace("_", "-") for a in param.get("aliases", [])]
                description = param.get("description")

                default_value = None
                if env_variable:
                    # We add family prefix for all ctl-related env variables to prevent stealing
                    # sensitive env variables if controller is compromised.
                    default_value = os.getenv(family_upper + "_" + env_variable)
                    description_ext = "default value can be set via {}_{} env variable".format(family_upper,
                                                                                               env_variable)

                    if default_value is not None:
                        description_ext += " (current value: \"{}\")".format(default_value)
                        # If there is a default value, the parameter is not required anymore.
                        required = False

                    if description:
                        description += "; " + description_ext
                    else:
                        description = description_ext

                element_name = param.get("element_name")
                element_description = param.get("element_description")
                element_aliases = [("-" if len(a) == 1 else "--") + a.replace("_", "-")
                                   for a in param.get("element_aliases", [])]

                if as_positional_argument:
                    add_argument(
                        subparser,
                        name,
                        description=description)
                elif element_name:
                    group = subparser.add_mutually_exclusive_group()
                    add_argument(
                        group,
                        "--" + name.replace("_", "-"),
                        action=param.get("action"),
                        description=description,
                        aliases=aliases)
                    add_argument(
                        group,
                        "--" + element_name.replace("_", "-"),
                        action="append",
                        aliases=element_aliases,
                        dest=name,
                        description=element_description)
                else:
                    add_argument(
                        subparser,
                        "--" + name.replace("_", "-"),
                        action=param.get("action"),
                        description=description,
                        required=required,
                        aliases=aliases,
                        default=default_value)

    # We replace original "parse_known_args" with our implementation, which fetches and registers available
    # commands before parsing.
    do_parse_known_args = parser.parse_known_args

    def parse_known_args(args=None, namespace=None):
        parsed_address, unparsed = address_parser.parse_known_args(args=args, namespace=namespace)
        register_commands(parsed_address.address)
        return do_parse_known_args(args=unparsed, namespace=namespace)

    parser.parse_known_args = parse_known_args
