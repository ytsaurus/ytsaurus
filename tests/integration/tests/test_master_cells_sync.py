from yt_env_setup import YTEnvSetup
from yt_commands import *

def _create_drivers(environment):
    driver_names = ["driver_secondary_{0}".format(i)
                    for i in xrange(environment.NUM_SECONDARY_MASTER_CELLS)]
    driver_configs = [environment.configs[driver_name] for driver_name in driver_names]
    return [Driver(config=config) for config in driver_configs]

class TestMasterCellsSync(YTEnvSetup):
    START_SECONDARY_MASTER_CELLS = True
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_NODES = 0

    @classmethod
    def setup_class(cls, delayed_secondary_cells_start=False):
        super(TestMasterCellsSync, cls).setup_class()
        cls.delayed_secondary_cells_start = delayed_secondary_cells_start

    def _check_true_for_secondary(self, check):
            if self.delayed_secondary_cells_start:
                for i in xrange(self.Env.NUM_SECONDARY_MASTER_CELLS):
                    self.Env.start_masters("master_secondary_{0}".format(i))
            try:
                drivers = _create_drivers(self.Env)
                multicell_sleep()
                for driver in drivers:
                    value = check(driver)
                    assert value
            finally:
                if self.delayed_secondary_cells_start:
                    for i in xrange(self.Env.NUM_SECONDARY_MASTER_CELLS):
                        self.Env.kill_service("master_secondary_{0}".format(i))

    def teardown(self):
        if self.delayed_secondary_cells_start:
            for i in xrange(self.Env.NUM_SECONDARY_MASTER_CELLS):
                self.Env.kill_service("master_secondary_{0}".format(i))

    def test_users_sync(self):
        create_user("tester")

        for i in xrange(10):
            set("//sys/users/tester/@custom{0}".format(i), "value")
        self._check_true_for_secondary(
                lambda driver: all([
                    get("//sys/users/tester/@custom{0}".format(i), driver=driver) == "value"
                    for i in xrange(10)]))

        multicell_sleep()
        self._check_true_for_secondary(
                lambda driver: "tester" in ls("//sys/users", driver=driver))

        remove_user("tester")
        self._check_true_for_secondary(
                lambda driver: "tester" not in ls("//sys/users", driver=driver))

    def test_groups_sync(self):
        create_user("tester")
        create_group("sudoers")
        add_member("tester", "sudoers")

        self._check_true_for_secondary(
                lambda driver: "sudoers" in ls("//sys/groups", driver=driver))

        multicell_sleep()
        self._check_true_for_secondary(
                lambda driver: "tester" in get("//sys/groups/sudoers/@members", driver=driver))

        multicell_sleep()
        self._check_true_for_secondary(
                lambda driver: "sudoers" in get("//sys/users/tester/@member_of", driver=driver))

        for i in xrange(10):
            set("//sys/groups/sudoers/@attr{0}".format(i), "value")
        remove_member("tester", "sudoers")

        check_attributes = lambda driver: all([
            get("//sys/groups/sudoers/@attr{0}".format(i), driver=driver) == "value" for i in xrange(10)])
        check_membership = lambda driver: "tester" not in get("//sys/groups/sudoers/@members", driver=driver)

        self._check_true_for_secondary(lambda driver: check_attributes(driver) and check_membership(driver))
        remove_group("sudoers")
        self._check_true_for_secondary(lambda driver: "sudoers" not in ls("//sys/groups", driver=driver))

    def test_accounts_sync(self):
        create_account("tst")

        for i in xrange(10):
            set("//sys/accounts/tst/@attr{0}".format(i), "value")
        self._check_true_for_secondary(
                lambda driver: all([
                    get("//sys/accounts/tst/@attr{0}".format(i), driver=driver) == "value"
                    for i in xrange(10)]))

        remove_account("tst")
        self._check_true_for_secondary(
                lambda driver: "tst" not in ls("//sys/accounts", driver=driver))

    def test_schemas_sync(self):
        create_group("testers")

        rule = {"action": "allow", "subjects": ["testers"], "permissions": ["create"]}

        for subj in ["user", "account", "table"]:
            set("//sys/schemas/{0}/@acl/end".format(subj), rule)

        def check(driver):
            ok = True
            for subj in ["user", "account"]:
                found = False
                for acl in get("//sys/schemas/{0}/@acl".format(subj), driver=driver):
                    if "testers" in acl["subjects"]:
                        found = True
                ok = ok and found
            return ok

        self._check_true_for_secondary(lambda driver: check(driver))

class TestMasterCellsSyncDelayed(TestMasterCellsSync):
    START_SECONDARY_MASTER_CELLS = False

    @classmethod
    def setup_class(cls):
        super(TestMasterCellsSyncDelayed, cls).setup_class(
                delayed_secondary_cells_start=True)
