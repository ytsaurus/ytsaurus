from __future__ import print_function

from yt.common import to_native_str

import sys

try:
    import subprocess32 as subprocess
except ImportError:
    if sys.version_info[:2] <= (2, 6):
        print("Some functions may not work properly on python of version <= 2.6 "
              "because subprocess32 library is not installed.", file=sys.stderr)
    import subprocess

Popen = subprocess.Popen
PIPE = subprocess.PIPE
STDOUT = subprocess.STDOUT
CalledProcessError = subprocess.CalledProcessError

def check_call(*args, **kwargs):
    return subprocess.check_call(*args, **kwargs)

def check_output(*args, **kwargs):
    return to_native_str(subprocess.check_output(*args, **kwargs))

