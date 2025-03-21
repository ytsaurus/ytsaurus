#!/usr/bin/env python

import os
import sys

from yt.wrapper.cli_helpers import run_main
import yt.wrapper as yt

def mapper(row):
    assert len(sys.argv) == 3
    yield row

def main():
    yt.enable_python_job_processing_for_standalone_binary()
    input, output = sys.argv[1:3]
    yt.run_map(mapper, input, output, spec={"mapper": {"environment": {"PYTHONPATH": os.environ["PYTHONPATH"]}}})

if __name__ == "__main__":
    run_main(main)

