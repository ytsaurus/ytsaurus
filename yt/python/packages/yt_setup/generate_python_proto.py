#!/usr/bin/env python3

import argparse
import shutil
import subprocess
import os

from pathlib import Path


def touch(path):
    with open(path, "w"):
        pass


def search_proto_files(path):
    for root, subdirs, files in os.walk(path):
        for file in files:
            file_path = Path(root) / file
            if file_path.name.endswith(".proto"):
                yield file_path


def generate_proto_recursively(dir, source_root, output):
    assert dir.parts[0] == "yt"
    assert output.is_dir()

    proto_path = source_root / "yt"
    for file_path in search_proto_files(source_root / dir):
        subprocess.check_call(["protoc", "--proto_path", proto_path, "--python_out", output, file_path])
        dir_path = file_path.parent.relative_to(proto_path)
        while dir_path != dir_path.parent:
            touch(output / dir_path / "__init__.py")
            dir_path = dir_path.parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    proto_dirs = ("yt/yt_proto/yt/core", "yt/yt_proto/yt/client")
    for dir in proto_dirs:
        generate_proto_recursively(Path(dir), args.source_root, args.output)

    shutil.copy(args.source_root / "yt/python/packages/ytsaurus-proto/setup.py", args.output)


if __name__ == "__main__":
    main()
