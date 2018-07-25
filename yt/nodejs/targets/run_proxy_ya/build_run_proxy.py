#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import stat
import sys

def main():
    # When we use CONFIGURE_FILE macro in `ya make` we cannot make result file executable, so we have to use
    # this script as workaround for this issue.
    arcadia_root = sys.argv[1]
    input = sys.argv[2]
    output = sys.argv[3]

    with open(input) as inf:
        text = inf.read()

    with open(output, "w") as outf:
        outf.write(text.replace("@ARCADIA_ROOT@", arcadia_root))
    os.chmod(output, stat.S_IXUSR | stat.S_IRUSR | stat.S_IXGRP | stat.S_IRGRP | stat.S_IXOTH | stat.S_IROTH)

if __name__ == "__main__":
    main()
