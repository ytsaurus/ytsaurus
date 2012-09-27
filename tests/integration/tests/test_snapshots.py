from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestSnapshot(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0
    START_SCHEDULER = False

    def modify_master_config(self, config):
    	config['meta_state']['max_changes_between_snapshots'] = 1

    def test(self):
    	set('//tmp/a', 42)
