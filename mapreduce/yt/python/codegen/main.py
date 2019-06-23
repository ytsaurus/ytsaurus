# -*- coding: utf-8 -*-

DESCRIPTION = """Tool for code generation for C++ YT API.\n
Available commands:
    add_command"""

EPILOG="""Example use (run from <arcadia root>/mapreduce/yt or set --arcadia-root option):
    $ codegen add_command \\
            --method CheckPermission \\
            --command check_permission \\
            --after-method PutFileToCache \\
            --transactional no \\
            --mutating no \\
            --return-type TCheckPermissionResponse \\
            --positional-arguments "const TYPath& path, const TString& user, EPermission permission" \\
            --add-batch-method yes \\
            --http-method GET \\
            --dry-run

IMPORTANT NOTES:
    * Read and fix WARNINGS at the end of shell output.

The output looks like

    Inserting into ./raw_client/rpc_parameters_serialization.h:
    vim ./raw_client/rpc_parameters_serialization.h +155
    --------------------
         const TYPath& filePath,
         const TString& md5Signature,
         const TYPath& cachePath,
         const TPutFileToCacheOptions&);
    +
    +TNode SerializeParamsForCheckPermission(
    +    const TYPath& path,
    +    const TString& user,
    +    EPermission permission,
    +    const TCheckPermissionOptions& options);

     ////////////////////////////////////////////////////////////////////

     } // namespace NYT::NDetail::NRawClient
    --------------------

and so on. You can copy and paste the lines with "vim ..." to edit the generated code.
In the end of the output you get a list of warnings which you should read carefully
and fix if necessary."""

import argparse
import os
from os.path import join

from add_command import add_command


def possibly_is_mapreduce_yt(path):
    files = os.listdir(path)
    return all(f in files for f in ["client", "interface", "common", "raw_client"])


def parse_yes_no(s):
    if s == "yes":
        return True
    elif s == "no":
        return False
    raise Exception("Expected \"yes\" or \"no\", got \"{}\"".format(s))


def parse_positional_arguments(s):
    return [arg.strip() for arg in s.split(',')]


def main():
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--arcadia-root", help="path to Arcadia root")

    subparsers = parser.add_subparsers(metavar="command")
    subparsers.required = True

    subparser = subparsers.add_parser("add_command", description="Add a new command")

    subparser.set_defaults(func=add_command)
    subparser.add_argument("--command", help="name of the command (e.g. \"lock\")", required=True)
    subparser.add_argument("--after-method", help="name of the method to insert after (e.g. \"Get\")", required=True)
    subparser.add_argument("--transactional", help="is this command transactional", type=parse_yes_no, required=True)
    subparser.add_argument("--mutating", help="is this command mutating", type=parse_yes_no, required=True)
    subparser.add_argument("--method", help="name of the method to add (e.g. \"Lock\")", required=True)
    subparser.add_argument("--return-type", help="return type of method", required=True)
    subparser.add_argument(
        "--positional-arguments",
        help="comma-separated list of positional argument with types\n"
             "(e.g. \"const TYPath& path, ELockMode mode\")",
        default=[],
        type=parse_positional_arguments,
    )
    subparser.add_argument(
        "--add-batch-method", help="add similar method to batch client (\"yes\" by default)", type=parse_yes_no, default=True)
    subparser.add_argument(
        "--add-external-mocks", help="add method to external mock clients (\"yes\" by default)", type=parse_yes_no, default=True)
    subparser.add_argument("--http-method", help="HTTP method (GET or POST)", required=True)
    subparser.add_argument(
        "--dry-run",
        help="don't change any files, just threaten to do it",
        action="store_true",
        default=False,
    )

    args = parser.parse_args()
    func_args = dict(vars(args))

    for key in ["func", "arcadia_root"]:
        if key in func_args:
            del func_args[key]

    mapreduce_yt_path = "."
    if args.arcadia_root is not None:
        mapreduce_yt_path = join(args.arcadia_root, "mapreduce/yt")
    if not possibly_is_mapreduce_yt(mapreduce_yt_path):
        raise Exception("Cannot find <arcadia>/mapreduce/yt, consider specifying --arcadia-root or cd-ing to mapreduce/yt")

    args.func(mapreduce_yt_path=mapreduce_yt_path, **func_args)


if __name__ == "__main__":
    main()
