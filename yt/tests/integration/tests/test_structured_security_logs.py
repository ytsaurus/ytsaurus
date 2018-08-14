from yt_env_setup import YTEnvSetup
from yt_commands import *

import json
import os
import time


class TestLogging(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1

    LOG_WRITE_WAIT_TIME = 0.2

    def test_users_and_groups(self):
        create_group("some_group")
        create_user("some_user")
        add_member("some_user", "some_group")
        remove_member("some_user", "some_group")
        remove_user("some_user")
        remove_group("some_group")
        time.sleep(self.LOG_WRITE_WAIT_TIME)

        log = self.load_structured_master_log()

        self.assert_log_event(log, {"event": "group_created", "name": "some_group"})
        self.assert_log_event(log, {"event": "user_created", "name": "some_user"})
        self.assert_log_event(log, {"event": "member_added", "group_name": "some_group", "member_type": "user", "member_name": "some_user"})
        self.assert_log_event(log, {"event": "member_removed", "group_name": "some_group", "member_type": "user", "member_name": "some_user"})
        self.assert_log_event(log, {"event": "user_destroyed", "name": "some_user"})
        self.assert_log_event(log, {"event": "group_destroyed", "name": "some_group"})

    def test_acd_update(self):
        create("table", "//tmp/test_table")
        set("//tmp/test_table/@acl", [{"permissions": ["read"], "action": "allow", "subjects": ["root"]}])
        time.sleep(self.LOG_WRITE_WAIT_TIME)

        self.assert_log_event(self.load_structured_master_log(), {
            "event": "object_acd_updated",
            "attribute": "acl",
            "value": [{"permissions": ["read"], "action": "allow", "subjects": ["root"]}]
        })

    def test_no_redundant_acd_updated_events(self):
        previous_log_size = len(self.load_structured_master_log())
        create("table", "//tmp/test_table")
        time.sleep(self.LOG_WRITE_WAIT_TIME)

        actual_log = self.load_structured_master_log()[previous_log_size:]
        for event in actual_log:
            if event.get("event") == "object_acd_updated":
                assert False, "Master log contains redundant event: " + str(event)

    def load_structured_master_log(self):
        log_path = self.path_to_run + "/logs/master-0-0.json.log"
        assert os.path.exists(log_path)
        log = []
        with open(log_path, 'r') as f:
            for line in f:
                log.append(json.loads(line))
        return log

    def assert_log_event(self, log, expected_event):
        found = False
        for event in log:
            match = True
            for key in expected_event:
                if key not in event or event[key] != expected_event[key]:
                    match = False
                    break
            if match:
                found = True

        assert found, "Event {} is not presented in log {}".format(expected_event, log)
