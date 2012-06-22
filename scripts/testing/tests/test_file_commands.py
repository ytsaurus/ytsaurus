import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestFileCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5

    # TODO(panin): add checks of chunk_count and size
    def test(self):
        content = "some_data"
        upload('//tmp/file', content)
        assert download('//tmp/file') == content
