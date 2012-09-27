from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestSnapshot(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0
    START_SCHEDULER = False

    def test(self):
    	set('//tmp/a', 42)

    	build_snapshot()

        # TODO(panin): make convenient way for this
        # Stop master
        self.kill_process(*self.Env.process_to_kill[0])
        self.Env.process_to_kill.pop()

        # Restore master
        self.Env._run_masters(prepare_files=False)

    	assert get('//tmp/a') == 42
