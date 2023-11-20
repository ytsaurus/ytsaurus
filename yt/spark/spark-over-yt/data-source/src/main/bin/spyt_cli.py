#!/usr/bin/env python
import argparse
import importlib.machinery
import importlib.util
import inspect
import logging
import os
import subprocess
import sys


logger = logging.getLogger(__name__)


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
    required_module = "spark-{}-yt".format(command)
    module = None
    try:
        module = import_path(required_module)
    except OSError as e:
        logger.info(f"Cannot import required module: {e}")
    if module is not None:
        module.main(args)
    else:
        logger.info("Fallback to subprocess run")
        script_directory = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        script = os.path.join(script_directory, required_module)
        subprocess.run([script] + args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['discovery', 'launch', 'manage', 'shell', 'submit'])
    args, unknown_args = parser.parse_known_args()
    run(args.command, unknown_args)


if __name__ == "__main__":
    main()
