from __future__ import print_function

from yp.local import (
    ACTUAL_DB_VERSION,
    YpInstance,
    reset_yp,
    unfreeze_yp,
)

from yp.local_heavy_scheduler import YpHeavySchedulerInstance

from yp.logger import logger

from yp.common import (
    WaitFailed,
    YtResponseError,
    wait,
)

from yt.wrapper.common import generate_uuid
from yt.wrapper.errors import YtTabletTransactionLockConflict
from yt.wrapper.retries import run_with_retries
from yt.wrapper.ypath import ypath_join

from yt.common import update, get_value

import yt.subprocess_wrapper as subprocess

from yt.packages.six.moves import xrange, map

from yt.environment import arcadia_interop

import pytest

import copy
import logging
import os
import sys
import time
import uuid

yatest_common = arcadia_interop.yatest_common


if yatest_common is None:
    sys.path.insert(0, os.path.abspath('../../python'))
    pytest_plugins = "yt.test_runner.plugin"

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", TESTS_LOCATION + ".sandbox")

ZERO_RESOURCE_REQUESTS = {
    "vcpu_guarantee": 0,
    "vcpu_limit": 0,
    "memory_guarantee": 0,
    "memory_limit": 0,
    "slot": 0,
}

DEFAULT_YP_MASTER_CONFIG = {
    "object_manager": {
        "pod_type_handler": {
            "spec_validation": {
                "min_vcpu_guarantee": 0
            }
        }
    }
}

DEFAULT_ACCOUNT_ID = "tmp"

DEFAULT_POD_SET_SPEC = dict(
    account_id=DEFAULT_ACCOUNT_ID,
    node_segment_id="default",
)

logger.setLevel(logging.DEBUG)


def check_over_time(predicate, iter=20, sleep_backoff=1):
    for _ in xrange(iter):
        if not predicate():
            return False
        time.sleep(sleep_backoff)
    return True


def assert_over_time(predicate, iter=20, sleep_backoff=1):
    for _ in xrange(iter):
        assert predicate()
        time.sleep(sleep_backoff)


def get_pod_scheduling_status(yp_client, pod_id):
    return yp_client.get_object("pod", pod_id, selectors=["/status/scheduling"])[0]


def get_pod_scheduling_statuses(yp_client, pod_ids):
    responses = yp_client.get_objects(
        "pod",
        pod_ids,
        selectors=["/status/scheduling"],
    )
    return list(map(lambda response: response[0], responses))


def is_assigned_pod_scheduling_status(scheduling_status):
    return "error" not in scheduling_status and \
        scheduling_status.get("state", None) == "assigned" and \
        scheduling_status.get("node_id", "") != ""


def are_assigned_pod_scheduling_statuses(scheduling_statuses):
    return all(map(is_assigned_pod_scheduling_status, scheduling_statuses))


def is_error_pod_scheduling_status(scheduling_status):
    return "error" in scheduling_status and \
        scheduling_status.get("state", None) != "assigned" and \
        scheduling_status.get("node_id", "") == ""


def are_error_pod_scheduling_statuses(scheduling_statuses):
    return all(map(is_error_pod_scheduling_status, scheduling_statuses))


def is_pod_assigned(yp_client, pod_id):
    return is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id))


def are_pods_assigned(yp_client, pod_ids):
    return are_assigned_pod_scheduling_statuses(get_pod_scheduling_statuses(yp_client, pod_ids))

def wait_pod_is_assigned(yp_client, pod_id):
    try:
        wait(lambda: is_pod_assigned(yp_client, pod_id))
    except WaitFailed:
        scheduling_error = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/error"])[0]
        raise WaitFailed("Error scheduling pod: {}".format(scheduling_error))

def wait_pods_are_assigned(yp_client, pod_ids):
    for pod_id in pod_ids:
        wait_pod_is_assigned(yp_client, pod_id)

def are_pods_touched_by_scheduler(yp_client, pod_ids):
    return all(map(
        lambda scheduling_status: is_error_pod_scheduling_status(scheduling_status) or \
            is_assigned_pod_scheduling_status(scheduling_status),
        get_pod_scheduling_statuses(yp_client, pod_ids)
    ))


def wait_pod_is_assigned_to(yp_client, pod_id, node_id):
    try:
        wait(lambda: is_pod_assigned(yp_client, pod_id))
    except WaitFailed:
        scheduling_error = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/error"])[0]
        raise WaitFailed("Error scheduling pod: expecte node: {}, got error: {}"
                         .format(node_id, scheduling_error))

    actual_node_id = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
    assert actual_node_id == node_id


def create_pod_set(yp_client, transaction_id=None, spec=None):
    spec = update(
        DEFAULT_POD_SET_SPEC,
        get_value(spec, dict()),
    )
    return yp_client.create_object(
        "pod_set",
        attributes=dict(spec=spec),
        transaction_id=transaction_id,
    )


def create_pod_with_boilerplate(
        yp_client,
        pod_set_id,
        spec=None,
        pod_id=None,
        transaction_id=None,
        labels=None):
    attributes = dict()

    attributes["spec"] = update(
        dict(resource_requests=ZERO_RESOURCE_REQUESTS),
        get_value(spec, dict()),
    )

    attributes["meta"] = dict(pod_set_id=pod_set_id)
    if pod_id is not None:
        attributes["meta"]["id"] = pod_id

    if labels is not None:
        attributes["labels"] = labels

    return yp_client.create_object("pod", attributes=attributes, transaction_id=transaction_id)

def update_node_id(yp_client, pod_id, node_id, other_updates=None, with_retries=True):
    if other_updates is None:
        other_updates = []

    def impl():
        yp_client.update_object("pod", pod_id, set_updates=
            [
                {
                    "path": "/spec/node_id",
                    "value": node_id
                }
            ] + other_updates)

    if with_retries:
        run_with_retries(impl, exceptions=(YtTabletTransactionLockConflict,))
    else:
        impl()


def create_nodes(
        yp_client,
        node_count=None,
        rack_count=1,
        hfsm_state="up",
        cpu_total_capacity=100,
        memory_total_capacity=1000000000,
        network_bandwidth=None,
        slot_capacity=300,
        disk_specs=None,
        gpu_specs=None,
        vlan_id="backbone",
        subnet="1:2:3:4::/64",
        network_module_id=None,
        node_ids=None,
        labels=None):
    disk_spec_defaults = dict(
        total_capacity=10 ** 11,
        total_volume_slots=10,
        storage_class="hdd",
        supported_policies=["quota", "exclusive"],
    )
    if disk_specs is None:
        disk_specs = [disk_spec_defaults]

    for i in xrange(len(disk_specs)):
        disk_specs[i] = update(disk_spec_defaults, disk_specs[i])

    assert (node_count is None) != (node_ids is None)
    if node_ids is None:
        node_ids = []
    else:
        node_count = len(node_ids)
    assert (node_count is not None) and (node_ids is not None)

    for i in xrange(node_count):
        node_meta = dict()
        if i < len(node_ids):
            node_meta["id"] = node_ids[i]
        base_labels = dict(
            topology=dict(
                node="node-{}".format(i),
                rack="rack-{}".format(i // (node_count // rack_count)),
                dc="butovo",
            ),
        )
        node_spec = {
            "ip6_subnets": [
                {
                    "vlan_id": vlan_id,
                    "subnet": subnet,
                },
            ],
        }
        if network_module_id is not None:
            node_spec["network_module_id"] = network_module_id

        current_labels = update(base_labels, get_value(labels, {}))
        node_id = yp_client.create_object("node", attributes={
                "meta": node_meta,
                "spec": node_spec,
                "labels" : current_labels,
            })
        yp_client.update_hfsm_state(node_id, hfsm_state, "Test")
        if i >= len(node_ids):
            node_ids.append(node_id)
        yp_client.create_object("resource", attributes={
                "meta": {
                    "node_id": node_id
                },
                "spec": {
                    "cpu": {
                        "total_capacity": cpu_total_capacity,
                    }
                }
            })
        yp_client.create_object("resource", attributes={
                "meta": {
                    "node_id": node_id
                },
                "spec": {
                    "memory": {
                        "total_capacity": memory_total_capacity,
                    }
                }
            })
        for spec in disk_specs:
            yp_client.create_object("resource", attributes={
                "meta": {
                    "node_id": node_id
                },
                "spec": {
                    "disk": spec
                }
            })

        if slot_capacity is not None:
            yp_client.create_object("resource", attributes={
                "meta": {
                    "node_id": node_id
                },
                "spec": {
                    "slot": {
                        "total_capacity": slot_capacity
                    }
                }
            })

        for gpu_spec in get_value(gpu_specs, []):
            if "uuid" not in gpu_spec:
                gpu_spec["uuid"] = str(uuid.uuid4())
            yp_client.create_object("resource", attributes={
                "meta": {
                    "node_id": node_id
                },
                "spec": {
                    "gpu": gpu_spec
                }
            })

        if network_bandwidth is not None:
            yp_client.create_object("resource", attributes={
                "meta": {
                    "node_id": node_id
                },
                "spec": {
                    "network": {
                        "total_bandwidth": network_bandwidth,
                    }
                }
            })

    return node_ids

def create_pod_set_with_quota(yp_client, cpu_quota=1000, memory_quota=2**10, bandwidth_quota=None,
                              gpu_quota=None, disk_quota=None):
    bandwidth_quota = bandwidth_quota or 0
    gpu_quota = gpu_quota or {}
    disk_quota = disk_quota or {}
    node_segment_id = yp_client.create_object("node_segment", attributes={
        "spec": {
            "node_filter": "%true"
        }
    })
    account_id = yp_client.create_object(
        "account",
        attributes=dict(
            spec=dict(resource_limits=dict(per_segment={node_segment_id: dict(
                gpu_per_model={model: dict(capacity=cap) for model, cap in gpu_quota.items()},
                disk_per_storage_class=disk_quota,
                cpu=dict(capacity=cpu_quota),
                memory=dict(capacity=memory_quota),
                network=dict(bandwidth=bandwidth_quota),
            )})),
        ),
    )
    pod_set_id = yp_client.create_object("pod_set", attributes=dict(spec=dict(
        account_id=account_id,
        node_segment_id=node_segment_id,
    )))
    return pod_set_id, account_id, node_segment_id


def run_eviction_acknowledger(yp_client, iteration_count=60, sleep_time=1):
    for _ in xrange(iteration_count):
        responses = yp_client.select_objects(
            "pod",
            filter="[/status/eviction/state] = \"requested\"",
            selectors=["/meta/id"],
        )
        for response in responses:
            pod_id = response[0]
            yp_client.acknowledge_pod_eviction(pod_id, "Test")
        time.sleep(sleep_time)


def attach_pod_set_to_disruption_budget(yp_client, pod_set_id, pod_disruption_budget_id):
    def impl():
        yp_client.update_object(
            "pod_set",
            pod_set_id,
            set_updates=[dict(
                path="/spec/pod_disruption_budget_id",
                value=pod_disruption_budget_id,
            )],
        )
    # Bypass conflicts with pod disruption budget controller.
    run_with_retries(impl, exceptions=(YtResponseError,))


class Cli(object):
    def __init__(self, directory_path, yamake_subdirectory_name, binary_name):
        if yatest_common is not None:
            binary_path = os.path.join("yp", directory_path, yamake_subdirectory_name, binary_name)
            self._cli_execute = [yatest_common.binary_path(binary_path)]
        else:
            binary_path = os.path.join(TESTS_LOCATION, "..", directory_path, binary_name)
            self._cli_execute = [
                os.environ.get("PYTHON_BINARY", sys.executable),
                os.path.normpath(binary_path),
            ]
        self._env_patch = None

    def _get_env(self):
        env = copy.deepcopy(os.environ)
        if self._env_patch is not None:
            env = update(env, self._env_patch)
        return env

    def set_env_patch(self, env_patch):
        self._env_patch = copy.deepcopy(env_patch)

    def get_args(self, args):
        return self._cli_execute + args

    def check_call(self, args, stdout, stderr):
        return subprocess.check_call(
            self.get_args(args),
            stdout=stdout,
            env=self._get_env(),
            stderr=stderr,
        )

    def check_output(self, args):
        subprocess_args = self.get_args(args)
        logging.info("Running {}".format(subprocess_args))

        return subprocess.check_output(
            subprocess_args,
            env=self._get_env(),
            stderr=sys.stderr
        ).strip()


def _insert_environ_path(path):
    assert len(path) > 0
    tokens = set(os.environ.get("PATH", "").split(os.pathsep))
    if path not in tokens:
        os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])


def _try_makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


class SandboxBase(object):
    def __init__(self):
        self._path = self._infer_path()
        _try_makedirs(self.get_port_locks_path())

    def _infer_path(self):
        if yatest_common is None:
            return TESTS_SANDBOX
        else:
            if yatest_common.get_param("ram_drive_path") is None:
                return yatest_common.output_path()
            else:
                return arcadia_interop.yatest_common.output_ram_drive_path()

    def get_port_locks_path(self):
        return os.path.join(self._path, "ports")

    def make_sandbox(self, name):
        path = os.path.join(self._path, name + "_" + generate_uuid())
        os.makedirs(path)
        return path


def prepare_yp_sandbox(sandbox_base):
    if yatest_common is not None:
        destination = os.path.join(yatest_common.work_path(), "yt_build_" + generate_uuid())
        os.makedirs(destination)
        path = arcadia_interop.prepare_yt_environment(destination)
        ypserver_master_binary = yatest_common.binary_path("yp/server/master/bin/ypserver-master")
        os.symlink(ypserver_master_binary, os.path.join(path, "ypserver-master"))
        _insert_environ_path(path)
    return sandbox_base.make_sandbox("yp")


def yatest_save_sandbox(sandbox_path):
    if yatest_common is not None:
        arcadia_interop.save_sandbox(sandbox_path, os.path.basename(sandbox_path))


class YpOrchidClient(object):
    def __init__(self, yt_client, yp_path):
        self._yt_client = yt_client
        self._instances_path = ypath_join(yp_path, "/master/instances")
        self._instance_addresses = self._yt_client.list(self._instances_path)
        assert len(self._instance_addresses) > 0

    def get(self, instance_address, path, *args, **kwargs):
        absolute_path = ypath_join(
            self._instances_path,
            instance_address,
            "/orchid",
            path,
        )
        return self._yt_client.get(absolute_path, *args, **kwargs)

    def get_instances(self):
        return list(self._instance_addresses)


class YpTestEnvironment(object):
    def __init__(self,
                 yp_master_config=None,
                 enable_ssl=False,
                 start=True,
                 db_version=ACTUAL_DB_VERSION,
                 local_yt_options=None,
                 start_yp_heavy_scheduler=False,
                 yp_heavy_scheduler_config=None,
                 sandbox_base=None):
        yp_master_config = update(DEFAULT_YP_MASTER_CONFIG, get_value(yp_master_config, {}))

        if sandbox_base is None:
            sandbox_base = SandboxBase()
        self._sandbox_base = sandbox_base
        self._test_sandbox_path = prepare_yp_sandbox(self._sandbox_base)

        self.yp_heavy_scheduler_instance = None
        if start_yp_heavy_scheduler:
            self.yp_heavy_scheduler_instance = YpHeavySchedulerInstance(
                yt_root_path="//yp/heavy_scheduler",
                working_directory_path=os.path.join(self._test_sandbox_path, "yp_heavy_scheduler"),
                config_patch=yp_heavy_scheduler_config,
            )

            if local_yt_options is None:
                local_yt_options = dict()
            self._ensure_option_value_greater_or_equal(local_yt_options, "http_proxy_count", 1)
            self._ensure_option_value_greater_or_equal(local_yt_options, "rpc_proxy_count", 1)

        self.yp_instance = YpInstance(
            self._test_sandbox_path,
            yp_master_config=yp_master_config,
            enable_ssl=enable_ssl,
            db_version=db_version,
            port_locks_path=self._sandbox_base.get_port_locks_path(),
            local_yt_options=local_yt_options,
        )

        self._prepare()
        if start:
            self._start()

    def _ensure_option_value_greater_or_equal(self, config, option_name, lower_bound):
        if option_name in config:
            if config[option_name] < lower_bound:
                raise RuntimeError(
                    "Incorrect value of option \"{}\": "
                    "expected greater or equal to {}, but got {}".format(
                        option_name,
                        lower_bound,
                        config[option_name],
                    ),
                )
        else:
            config[option_name] = lower_bound

    def _prepare(self):
        self.yp_instance.prepare()
        self.yp_client = None
        self.yt_client = self.yp_instance.create_yt_client()

    def _start(self):
        try:
            self.yp_instance.start()
            self.yp_client = self.yp_instance.create_client()

            def touch_pod_set():
                try:
                    pod_set_id = self.yp_client.create_object("pod_set")
                    self.yp_client.remove_object("pod_set", pod_set_id)
                except YtResponseError:
                    return False
                return True

            wait(touch_pod_set)

            self.sync_access_control()

            if self.yp_heavy_scheduler_instance is not None:
                self.yp_heavy_scheduler_instance.start(
                    yt_http_proxy_address=self.yp_instance.yt_instance.get_http_proxy_address(),
                    yp_master_grpc_address=self.yp_instance.yp_client_grpc_address,
                    port_locks_path=self._sandbox_base.get_port_locks_path(),
                )
        except:
            yatest_save_sandbox(self._test_sandbox_path)
            raise

    def create_orchid_client(self):
        return YpOrchidClient(self.yt_client, "//yp")

    def get_cypress_config_patch_path(self):
        return "//yp/master/config"

    def set_cypress_config_patch(self, value, type="document"):
        self.yt_client.create(
            type,
            self.get_cypress_config_patch_path(),
            attributes=dict(value=value),
            force=True,
        )

    def reset_cypress_config_patch(self):
        self.yt_client.remove(
            self.get_cypress_config_patch_path(),
            force=True,
            recursive=True,
        )
        orchid = self.create_orchid_client()
        instance_address = orchid.get_instances()[0]
        def is_config_reinitialized():
            try:
                config = dict(orchid.get(instance_address, "/config"))
                initial_config = dict(orchid.get(instance_address, "/initial_config"))
                return initial_config == config
            except Exception:  # Ignore non existent Orchid nodes.
                return False
        wait(is_config_reinitialized)

    def sync_scheduler(self):
        # TODO(bidzilya): YP-1235
        time.sleep(10)

    def sync_access_control(self):
        orchid = self.create_orchid_client()
        master_addresses = orchid.get_instances()

        expected_timestamp = self.yp_client.generate_timestamp()

        synced_master_addresses = set()

        def is_state_updated():
            for master_address in master_addresses:
                if master_address in synced_master_addresses:
                    continue
                if orchid.get(master_address, "/access_control/cluster_state_timestamp") > expected_timestamp:
                    synced_master_addresses.add(master_address)
                else:
                    return False
            return True

        wait(is_state_updated, iter=5, sleep_backoff=1)

    def cleanup(self):
        try:
            if self.yp_heavy_scheduler_instance is not None:
                self.yp_heavy_scheduler_instance.stop()
            if self.yp_client is not None:
                self.yp_client.close()
            self.yp_instance.stop()
            yatest_save_sandbox(self._test_sandbox_path)
        except:
            # Additional logging added due to https://github.com/pytest-dev/pytest/issues/2237
            logger.exception("YpTestEnvironment cleanup failed")
            raise

def test_method_setup(yp_env):
    print("\n", file=sys.stderr)

def test_method_teardown(yp_env):
    print("\n", file=sys.stderr)
    try:
        # Reset database state.
        reset_yp(yp_env.yp_client)

        yp_env.reset_cypress_config_patch()
    except:
        # Additional logging added due to https://github.com/pytest-dev/pytest/issues/2237
        logger.exception("test_method_teardown failed")
        raise

def test_method_unfreeze(yp_env):
    try:
        # Unfreeze database.
        unfreeze_yp(yp_env.yt_client, "//yp")
    except:
        logger.exception("test_method_unfreeze failed")
        raise

@pytest.fixture(scope="session")
def test_environment(request):
    environment = YpTestEnvironment()
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="function")
def yp_env(request, test_environment):
    test_method_setup(test_environment)
    request.addfinalizer(lambda: test_method_teardown(test_environment))
    return test_environment

@pytest.fixture(scope="class")
def test_environment_configurable(request):
    environment = YpTestEnvironment(
        yp_master_config=getattr(request.cls, "YP_MASTER_CONFIG", None),
        enable_ssl=getattr(request.cls, "ENABLE_SSL", False),
        local_yt_options=getattr(request.cls, "LOCAL_YT_OPTIONS", None),
        start=getattr(request.cls, "START", True),
        start_yp_heavy_scheduler=getattr(request.cls, "START_YP_HEAVY_SCHEDULER", False),
        yp_heavy_scheduler_config=getattr(request.cls, "YP_HEAVY_SCHEDULER_CONFIG", None),
    )
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="function")
def yp_env_configurable(request, test_environment_configurable):
    test_method_setup(test_environment_configurable)
    request.addfinalizer(lambda: test_method_teardown(test_environment_configurable))
    return test_environment_configurable

@pytest.fixture(scope="function")
def yp_env_unfreezenable(request, yp_env_configurable):
    request.addfinalizer(lambda: test_method_unfreeze(yp_env_configurable))
    return yp_env_configurable
