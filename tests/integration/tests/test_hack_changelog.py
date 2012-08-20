import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import os
import time
import subprocess

class TestHackChangelog(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def test_all(self):
        set_str("//home/@test_attribute", "123")

        self.assertTrue(get_str("//home/@").find("test_attribute") != -1)

        # Stop master
        self.kill_process(*self.Env.process_to_kill[0])
        self.Env.process_to_kill.pop()
        
        changelogs_path = self.Env._master_configs[0]["meta_state"]["changelogs"]["path"]
        changelog = os.path.join(
            changelogs_path,
            filter(lambda f: not f.endswith("index"), os.listdir(changelogs_path))[0])

        # Fetch number of records in changelog
        output = subprocess.check_output(
            ["process_changelog",
             "--action", "read",
             "--filename", changelog])

        record_count = None
        for line in output.split("\n"):
            if "Record count" in line:
                record_count = int(line[line.find("=") + 1:])

        # Replace last record with no operation
        subprocess.check_call(
            ["process_changelog",
             "--action", "remove",
             "--filename", changelog,
             "-f", str(record_count - 1),
             "-t", str(record_count - 1)],
            stdin=subprocess.PIPE)


        # Restore master
        self.Env._run_masters(prepare_files=False)
        
        self.assertTrue(get_str("//home/@").find("test_attribute") == -1)


