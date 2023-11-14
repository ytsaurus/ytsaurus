#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os

from yt_setup.os_helpers import apply_multiple, cp, cp_r, logger, touch


def prepare_python_modules(source_root, output_path):
    cp_r(os.path.join(source_root, "yt/odin/lib/yt_odin"), output_path)

    odin_bin_output = os.path.join(output_path, "yt_odin/bin")
    os.makedirs(odin_bin_output)
    for bin in os.listdir(os.path.join(source_root, "yt/odin/bin")):
        bin_dir = os.path.join(source_root, "yt/odin/bin", bin)
        if not os.path.isdir(bin_dir):
            continue
        cp(os.path.join(bin_dir, "__main__.py"), os.path.join(odin_bin_output, bin))

    odin_checks_output = os.path.join(output_path, "yt_odin_checks")
    os.makedirs(odin_checks_output)

    checks_source_path = os.path.join(source_root, "yt/odin/checks/")
    cp_r(os.path.join(checks_source_path, "lib"), odin_checks_output)
    touch(os.path.join(odin_checks_output, "__init__.py"))
    touch(os.path.join(odin_checks_output, "lib/__init__.py"))

    os.makedirs(os.path.join(odin_checks_output, "bin"))
    for check in os.listdir(os.path.join(checks_source_path, "bin")):
        check_dir = os.path.join(checks_source_path, "bin", check)
        if not os.path.isdir(check_dir):
            continue

        with open(os.path.join(check_dir, "__main__.py")) as fin:
            with open(os.path.join(odin_checks_output, "bin", check), "w") as fout:
                input = fin.read()
                if not input.startswith("#"):
                    fout.write("#!/usr/bin/env python\n\n")
                fout.write(input)


def get_default_source_root():
    return apply_multiple(times=4, func=os.path.dirname, argument=os.path.abspath(__file__))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", default=get_default_source_root())
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    prepare_python_modules(source_root=args.source_root, output_path=args.output_path)


if __name__ == "__main__":
    main()
