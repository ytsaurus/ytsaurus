from __future__ import print_function

import sys

if sys.version_info[0] == 3 and sys.version_info[0:2] < (3, 4):
    raise ImportError("YT API supports Python 3 only from version 3.4, but is imported "
                      "in Python {0}.{1}".format(*sys.version_info[:2]))
