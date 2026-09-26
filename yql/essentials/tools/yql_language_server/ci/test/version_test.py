#!/usr/bin/env python3

import subprocess
import sys


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(f"usage: {sys.argv[0]} VERSION_SCRIPT CHANGELOG SERVER")

    version_script, changelog_path, server = sys.argv[1:]
    with open(changelog_path, "rb") as changelog:
        expected = subprocess.run(
            [sys.executable, version_script],
            input=changelog.read(),
            stdout=subprocess.PIPE,
            check=True,
        ).stdout

    actual = subprocess.run(
        [server, "--version"],
        stdout=subprocess.PIPE,
        check=True,
    ).stdout

    if actual != expected:
        raise SystemExit(f"expected version {expected!r}, got {actual!r}")


if __name__ == "__main__":
    main()
