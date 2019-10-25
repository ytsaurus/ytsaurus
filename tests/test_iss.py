from .conftest import (
    DEFAULT_POD_SET_SPEC,
    YpTestEnvironment,
    create_nodes,
    create_pod_with_boilerplate,
    prepare_test_sandbox,
    # Make sure these methods do not start with "test" prefix.
    test_method_setup as _test_method_setup,
    test_method_teardown as _test_method_teardown,
    wait,
    yatest_save_sandbox,
)

import os
import pytest
import time


def is_in_arcadia():
    from .conftest import yatest_common
    return yatest_common is not None


if is_in_arcadia():
    from iss.local import IssLocal, ResourceManager

    from iss.common.qemu import get_default_route_addresses
    from iss.common.utils import get_free_port_in_range


# Test resources.
ISS_LOCAL_QEMU_IMAGE_FILE_PATH = "yp_tests_iss_local_qemu.img"
POD_AGENT_ROOTFS_FILE_PATH = "yp_tests_pod_agent_rootfs.tar.gz"
POD_AGENT_BINARY_FILE_PATH = "yp_tests_pod_agent"
WORKLOAD_ROOTFS_FILE_PATH = "yp_tests_workload_rootfs.tar.gz"


def get_iss_local_qemu_image_file_path():
    return os.path.abspath(ISS_LOCAL_QEMU_IMAGE_FILE_PATH)


@pytest.fixture(scope="class")
def iss_agent_address():
    return get_default_route_addresses()


@pytest.fixture(scope="class")
def yp_agent_grpc_port():
    return next(get_free_port_in_range(1024, 10240))


@pytest.fixture(scope="function")
def iss_agent(request, iss_agent_address, yp_agent_grpc_port):
    qemu_sandbox_path = prepare_test_sandbox("iss_local_qemu")
    iss_local = IssLocal(
        dom0_address=iss_agent_address,
        qemu_image=get_iss_local_qemu_image_file_path(),
        qemu_work_dir=qemu_sandbox_path,
        yp_agent_port=yp_agent_grpc_port,
        port_generator=get_free_port_in_range(1024, 10240),
    )
    iss_agent = iss_local.start()
    def finalizer():
        iss_agent.stop()
        iss_local.stop()
        yatest_save_sandbox(qemu_sandbox_path)
    request.addfinalizer(finalizer)
    return iss_agent


@pytest.fixture(scope="function")
def iss_resource_manager(request, iss_agent_address):
    sandbox_path = prepare_test_sandbox("iss_resource_manager")
    resource_manager = ResourceManager(
        sandbox_path,
        ssl=False,
        host=iss_agent_address,
    )
    resource_manager.start()
    def finalizer():
        resource_manager.stop()
        yatest_save_sandbox(sandbox_path)
    request.addfinalizer(finalizer)
    return resource_manager


@pytest.fixture(scope="class")
def test_environment_iss(request, iss_agent_address, yp_agent_grpc_port):
    yp_master_config = dict( # TODO: Fix addresses overriding in the yp/local.py::YpInstance.
        agent_grpc_server=dict(
            addresses=[
                dict(
                    address="[{}]:{}".format(iss_agent_address, yp_agent_grpc_port)
                )
            ]
        )
    )
    environment = YpTestEnvironment(yp_master_config=yp_master_config)
    request.addfinalizer(lambda: environment.cleanup())
    return environment


@pytest.fixture(scope="function")
def yp_env_iss(request, test_environment_iss):
    _test_method_setup(test_environment_iss)
    request.addfinalizer(lambda: _test_method_teardown(test_environment_iss))
    return test_environment_iss


@pytest.mark.skipif(
    not is_in_arcadia(),
    reason="YP Iss tests are disabled outside Arcadia environment",
)
@pytest.mark.usefixtures("yp_env_iss", "iss_agent", "iss_resource_manager")
class TestSchedulePod(object):
    def test_pod_agent(self, yp_env_iss, iss_agent, iss_resource_manager):
        with open(POD_AGENT_ROOTFS_FILE_PATH, "rb") as pod_agent_rootfs_file:
            pod_agent_rootfs_resource = iss_resource_manager.put_resource(
                "pod_agent_rootfs.tar.gz",
                pod_agent_rootfs_file.read()
            )

        with open(POD_AGENT_BINARY_FILE_PATH, "rb") as pod_agent_binary_file:
            pod_agent_binary_resource = iss_resource_manager.put_resource(
                "pod_agent",
                pod_agent_binary_file.read()
            )

        with open(WORKLOAD_ROOTFS_FILE_PATH, "rb") as workload_rootfs_file:
            workload_rootfs_resource = iss_resource_manager.put_resource(
                "workload_rootfs.tar.gz",
                workload_rootfs_file.read()
            )

        self._test_pod_agent_impl(
            yp_env_iss.yp_client,
            iss_agent,
            pod_agent_rootfs_resource,
            pod_agent_binary_resource,
            workload_rootfs_resource,
        )

    def prepare_objects(self, yp_client, iss_agent, pod_spec, network_project_id):
        node_id = iss_agent.hostname
        subnet = iss_agent.allocatable_subnet.exploded
        yp_client.create_object(
            "network_project",
            attributes=dict(
                meta=dict(id=network_project_id),
                spec=dict(project_id=0xdeaf),
            ),
        )
        create_nodes(
            yp_client,
            vlan_id="backbone",
            subnet=subnet,
            node_ids=[node_id],
        )
        pod_set_id = yp_client.create_object(
            "pod_set",
            attributes=dict(spec=DEFAULT_POD_SET_SPEC),
        )
        return create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=pod_spec,
        )

    def _test_pod_agent_impl(self,
                             yp_client,
                             iss_agent,
                             pod_agent_rootfs_resource,
                             pod_agent_binary_resource,
                             workload_rootfs_resource):
        workload_id = "workload-id"
        box_id = "box-id"
        layer_id = "layer-id"
        network_project_id = "_TEST_NET_"
        pod_spec = {
            "enable_scheduling": True,
            "pod_agent_payload": {
                "meta": {
                    "url": pod_agent_binary_resource.url,
                    "checksum": pod_agent_binary_resource.verification,
                    "layers": [{
                        "url": pod_agent_rootfs_resource.url,
                        "checksum": pod_agent_rootfs_resource.verification,
                    }]
                },
                "spec": {
                    "workloads": [{
                        "id": workload_id,
                        "box_ref": box_id,
                        "start": {
                            "command_line": "sleep 1000",
                            "time_limit": {
                                "min_restart_period_ms": 100,
                                "max_restart_period_ms": 1000,
                                "max_execution_time_ms": 10000
                            }
                        },
                        "readiness_check": {
                            "container": {
                                "command_line": "echo ready",
                                "time_limit": {
                                    "min_restart_period_ms": 100,
                                    "max_restart_period_ms": 1000,
                                    "max_execution_time_ms": 10000
                                }
                            }
                        }
                    }],
                    "boxes": [{
                        "id": box_id,
                        "rootfs": {
                            "layer_refs": [layer_id]
                        }
                    }],
                    "resources": {
                        "layers": [{
                            "id": layer_id,
                            "url": workload_rootfs_resource.url,
                            "checksum": workload_rootfs_resource.verification
                        }]
                    },
                    "mutable_workloads": [{
                        "workload_ref": workload_id,
                        "target_state": "active"
                    }],
                    "revision": 1
                }
            },
            "ip6_address_requests": [{
                "network_id": network_project_id,
                "vlan_id": "backbone"
            }],
            "disk_volume_requests": [{
                "id": "allocation",
                "storage_class": "hdd",
                "quota_policy": {
                    "capacity": 1024 * 1024 * 1024
                },
                "labels": {
                    "used_by_infra": True
                }
            }]
        }
        pod_id = self.prepare_objects(yp_client, iss_agent, pod_spec, network_project_id)

        iss_agent.client.start()

        def is_workload_active():
            status_response = yp_client.get_object(
                "pod",
                pod_id,
                selectors=["/status/agent/pod_agent_payload/status"]
            )[0]
            if status_response == None: # Check for YsonEntity or None.
                return False
            workloads = status_response.get("workloads", [])
            if len(workloads) < 1:
                return False
            assert len(workloads) == 1
            workload_status = workloads[0]
            return workload_status.get("id") == workload_id and \
                workload_status.get("state") == "active"

        wait(is_workload_active, iter=300, sleep_backoff=1)

        expected_spec_timestamp = yp_client.get_object(
            "pod",
            pod_id,
            selectors=["/status/master_spec_timestamp"],
        )[0]
        assert expected_spec_timestamp > 0

        def is_spec_applied():
            pod_agent_status = yp_client.get_object(
                "pod",
                pod_id,
                selectors=["/status/agent"],
            )[0]
            return pod_agent_status.get("current_spec_timestamp") == expected_spec_timestamp \
                and "current_spec_applied" in pod_agent_status

        wait(is_spec_applied, iter=300, sleep_backoff=1)
