import pytest
import sys

import yt.yson

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestTablets(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 0

