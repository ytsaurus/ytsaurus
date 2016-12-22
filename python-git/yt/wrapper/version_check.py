from __future__ import print_function

import sys

if sys.version_info[0] == 3 and sys.version_info[0:2] < (3, 4):
    print("YT API supports python3 only from version 3.4, but you have", sys.version, file=sys.stderr)
    raise ImportError()

