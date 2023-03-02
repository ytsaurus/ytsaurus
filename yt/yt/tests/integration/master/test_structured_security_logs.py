from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, set, create, ls,
    create_user, remove_user, create_group, remove_group, add_member, remove_member)

from yt_helpers import write_log_barrier, read_structured_log

import time


class TestStructuredSecurityLogs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1

    LOG_WRITE_WAIT_TIME = 0.2

    @authors("renadeen")
    def test_users_and_groups(self):
        create_group("some_group")
        create_user("some_user")
        add_member("some_user", "some_group")
        remove_member("some_user", "some_group")
        remove_user("some_user")
        remove_group("some_group")

        self.wait_log_event({"event": "group_created", "name": "some_group"})
        self.wait_log_event({"event": "user_created", "name": "some_user"})
        self.wait_log_event(
            {
                "event": "member_added",
                "group_name": "some_group",
                "member_type": "user",
                "member_name": "some_user",
            }
        )
        self.wait_log_event(
            {
                "event": "member_removed",
                "group_name": "some_group",
                "member_type": "user",
                "member_name": "some_user",
            }
        )
        self.wait_log_event({"event": "user_destroyed", "name": "some_user"})
        self.wait_log_event({"event": "group_destroyed", "name": "some_group"})

    @authors("renadeen")
    def test_acd_update(self):
        create("table", "//tmp/test_table")
        set(
            "//tmp/test_table/@acl",
            [{"permissions": ["read"], "action": "allow", "subjects": ["root"]}],
        )

        self.wait_log_event(
            {
                "event": "object_acd_updated",
                "attribute": "acl",
                "value": [{"permissions": ["read"], "action": "allow", "subjects": ["root"]}],
            }
        )

    @authors("renadeen")
    def test_no_redundant_acd_updated_events(self):
        master_address = ls("//sys/primary_masters")[0]
        from_barrier = write_log_barrier(master_address)
        create("table", "//tmp/test_table")
        time.sleep(self.LOG_WRITE_WAIT_TIME)
        to_barrier = write_log_barrier(master_address)

        actual_log = read_structured_log(self.get_structured_log_path(), from_barrier=from_barrier, to_barrier=to_barrier)
        for event in actual_log:
            if event.get("event") == "object_acd_updated":
                assert False, "Master log contains redundant event: " + str(event)

    def get_structured_log_path(self):
        return self.path_to_run + "/logs/master-0-0.json.log"

    def log_contains_event(self, expected_event):
        log = read_structured_log(self.get_structured_log_path())
        for event in log:
            match = True
            for key in expected_event:
                if key not in event or event[key] != expected_event[key]:
                    match = False
                    break
            if match:
                return True

        return False

    def wait_log_event(self, expected_event):
        wait(
            lambda: self.log_contains_event(expected_event),
            lambda: "Event {} is not presented in log {}".format(
                expected_event, read_structured_log(self.get_structured_log_path())),
        )
