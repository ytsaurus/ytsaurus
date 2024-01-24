#!/usr/bin/env python
import argparse
from importlib import import_module
import inspect
import logging
import os
import subprocess


logger = logging.getLogger(__name__)


def run(command, args):
    required_file = "spark-{}-yt".format(command)
    module = None
    try:
        required_module = "yt.spark.spark-over-yt.spyt-package.src.main.bin." + required_file
        module = import_module(required_module)
    except ImportError as e:
        logger.info(f"Cannot import required module: {e}")
    if module is not None:
        # Arcadia python
        module.main(args)
    else:
        # Pip installation
        logger.info("Fallback to subprocess run")
        script_directory = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        script = os.path.join(script_directory, required_file)
        subprocess.run([script] + args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['discovery', 'launch', 'manage', 'shell', 'submit'])
    args, unknown_args = parser.parse_known_args()
    run(args.command, unknown_args)


if __name__ == "__main__":
    main()
