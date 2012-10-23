import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import os
import subprocess

class TestHackChangelog(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def prepare_changelog_path(self):
        # Search changelog
        changelogs_path = self.Env._master_configs[0]["meta_state"]["changelogs"]["path"]
        self.changelog = os.path.join(
            changelogs_path,
            filter(lambda f: not f.endswith("index"), os.listdir(changelogs_path))[0])

    def stop_master(self):
        # TODO(panin): make convenient way for this
        # Stop master
        self.kill_process(*self.Env.process_to_kill[0])
        self.Env.process_to_kill.pop()

    def get_record_count(self):
        output = subprocess.check_output(
            ["process_changelog",
             "--action", "read",
             "--filename", self.changelog])

        for line in output.split("\n"):
            if "Record count" in line:
                return int(line[line.find("=") + 1:])

    def truncate_changelog(self, truncate_record_number):
        record_count = self.get_record_count()
        subprocess.check_call(
            ["process_changelog",
             "--action", "remove",
             "--filename", self.changelog,
             "-f", str(truncate_record_number),
             "-t", str(record_count - 1)],
            stdin=subprocess.PIPE)

    def test_all(self):
        self.prepare_changelog_path()
        record_count = self.get_record_count()

        set_str("//home/@test_attribute", "123")
        assert get_str("//home/@").find("test_attribute") != -1

        self.stop_master()
        self.truncate_changelog(record_count)

        self.Env._run_masters(prepare_files=False)
        assert get_str("//home/@").find("test_attribute") == -1


