#!/usr/bin/env python
import argparse
import importlib.machinery
import importlib.util
import os
import sys


def import_path(path):
    # Workaround for importing dashed non .py names of files
    module_name = os.path.basename(path).replace('-', '_')
    source_file_loader = importlib.machinery.SourceFileLoader(module_name, path)
    spec = importlib.util.spec_from_loader(module_name, source_file_loader)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[module_name] = module
    return module


def run(command, args):
    module = import_path("spark-{}-yt".format(command))
    module.main(args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['discovery', 'launch', 'manage', 'shell', 'submit'])
    args, unknown_args = parser.parse_known_args()
    run(args.command, unknown_args)


if __name__ == "__main__":
    main()
