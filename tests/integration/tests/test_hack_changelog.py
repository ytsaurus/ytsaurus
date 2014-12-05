from yt_env_setup import YTEnvSetup
from yt_commands import *

import os
import subprocess


if not hasattr(subprocess, "check_output"):
    def check_output(*args, **kwargs):
        child = subprocess.Popen(stdout=subprocess.PIPE, *args, **kwargs)
        out, _ = child.communicate()
        result = child.poll()
        if result:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = args[0]
            error = subprocess.CalledProcessError(result, cmd)
            error.output = output
            raise error
        return out
    subprocess.check_output = check_output


class TestHackChangelog(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def prepare_changelog_path(self):
        # Search changelog
        changelogs_path = self.Env.configs["master"][0]["meta_state"]["changelogs"]["path"]
        self.changelog = os.path.join(
            changelogs_path,
            filter(lambda f: not f.endswith("index"), os.listdir(changelogs_path))[0])

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

    def DISABLED_test_all(self):
        self.prepare_changelog_path()
        record_count = self.get_record_count()

        set("//home/@test_attribute", 123)
        assert "test_attribute" in get("//home/@")

        self.Env._kill_service("master")

        self.truncate_changelog(record_count)

        self.Env.start_masters("master")

        assert "test_attribute" not in get("//home/@")


