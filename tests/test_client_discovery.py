from yp.client import YpClientError, YpMasterDiscovery
from yp.common import GrpcDeadlineExceededError
from yp.retries import get_default_retries_config

from yt.wrapper.errors import YtRetriableError as ChaosMonkeyError

from yt.common import update

import pytest

import copy
import sys
import time


@pytest.mark.usefixtures("yp_env")
class TestClientYpMasterDiscovery(object):
    def get_mock_yp_client(self, instance_discovery_infos):
        class MockYpClient(object):
            def get_masters(self, _allow_retries=None):
                return dict(master_infos=instance_discovery_infos)

        return MockYpClient()

    def test_nonalive_filtering(self):
        client = self.get_mock_yp_client(
            [
                dict(alive=False, fqdn="fqdn1", instance_tag="tag1", http_address="httpaddress1",),
                dict(alive=False, fqdn="fqdn2", instance_tag="tag2", http_address="httpaddress2",),
            ]
        )
        discovery = YpMasterDiscovery(client, expiration_time=100)
        with pytest.raises(YpClientError):
            discovery.get_random_instance_address("http")
        with pytest.raises(YpClientError):
            discovery.get_instance_address_by_tag("tag1", "http")

    def test_transport(self):
        client = self.get_mock_yp_client(
            [
                dict(alive=True, fqdn="fqdn1", instance_tag="tag1", http_address="httpaddress1",),
                dict(alive=False, fqdn="fqdn2", instance_tag="tag2", http_address="httpaddress2",),
                dict(alive=True, fqdn="fqdn3", instance_tag="tag3", grpc_address="grpcaddress1",),
                dict(alive=False, fqdn="fqdn4", instance_tag="tag4", grpc_address="grpcaddress2",),
            ]
        )
        discovery = YpMasterDiscovery(client, expiration_time=100)
        for _ in range(10):
            assert discovery.get_random_instance_address("grpc") == "grpcaddress1"
            assert discovery.get_random_instance_address("http") == "httpaddress1"

    def test_required_fields(self):
        instance_discovery_info = dict(
            alive=True, fqdn="fqdn1", instance_tag="tag1", grpc_address="grpcaddress1",
        )
        for field in ["alive", "fqdn", "instance_tag", "grpc_address"]:
            info = copy.deepcopy(instance_discovery_info)
            del info[field]
            client = self.get_mock_yp_client([info])
            discovery = YpMasterDiscovery(client, expiration_time=100)
            with pytest.raises(YpClientError):
                discovery.get_random_instance_address("grpc")
        client = self.get_mock_yp_client([instance_discovery_info])
        discovery = YpMasterDiscovery(client, expiration_time=100)
        assert discovery.get_random_instance_address("grpc") == "grpcaddress1"

    def test_duplicate_tags(self):
        client = self.get_mock_yp_client(
            [
                dict(alive=True, fqdn="fqdn1", instance_tag="tag1", http_address="httpaddress1",),
                dict(alive=False, fqdn="fqdn2", instance_tag="tag1", http_address="httpaddress2",),
            ]
        )
        discovery = YpMasterDiscovery(client, expiration_time=100)
        with pytest.raises(YpClientError):
            discovery.get_random_instance_address("http")

    def test_expiration(self):
        class MockYpClient(object):
            def __init__(self):
                self._request_count = 0

            def get_masters(self, _allow_retries=None):
                self._request_count += 1
                return dict(
                    master_infos=[
                        dict(
                            alive=True,
                            fqdn="fqdn",
                            instance_tag="tag",
                            grpc_address=str(self._request_count),
                        )
                    ]
                )

        EXPIRATION_TIME = 1000
        client = MockYpClient()
        discovery = YpMasterDiscovery(client, expiration_time=EXPIRATION_TIME)
        assert int(discovery.get_random_instance_address("grpc")) == 1
        start_time = time.time()
        while time.time() - start_time < (EXPIRATION_TIME / 1000.0) / 10.0:
            assert int(discovery.get_random_instance_address("grpc")) == 1
        time.sleep(2 * EXPIRATION_TIME / 1000.0)
        assert int(discovery.get_random_instance_address("grpc")) > 1

    def test_balancing(self):
        client = self.get_mock_yp_client(
            [
                dict(alive=True, fqdn="fqdn1", instance_tag="tag1", http_address="httpaddress1",),
                dict(alive=True, fqdn="fqdn2", instance_tag="tag2", http_address="httpaddress2",),
            ]
        )
        discovery = YpMasterDiscovery(client, expiration_time=100)
        addresses = set()
        for _ in range(1000):
            addresses.add(discovery.get_random_instance_address("http"))
        assert addresses == set(["httpaddress1", "httpaddress2"])

    # YpClient is supposed to hold strong reference to YpMasterDiscovery,
    # but YpMasterDiscovery is obviously needed to call client get_masters method,
    # so we need to test that there are no cyclic references between discovery and client.
    def test_client_cyclic_reference(self):
        client = self.get_mock_yp_client([])
        discovery = YpMasterDiscovery(client, expiration_time=100)
        # One additional reference from getrefcount call argument.
        assert sys.getrefcount(client) == 2

    def test_consistency_after_failure(self):
        class MockYpClient(object):
            def __init__(self):
                self._request_count = 0

            def get_masters(self, _allow_retries=None):
                self._request_count += 1
                if self._request_count == 1:
                    raise GrpcDeadlineExceededError()
                return dict(
                    master_infos=[
                        dict(alive=True, fqdn="fqdn", instance_tag="tag", grpc_address="address")
                    ]
                )

        client = MockYpClient()
        discovery = YpMasterDiscovery(client, expiration_time=2000)
        with pytest.raises(GrpcDeadlineExceededError):
            discovery.get_random_instance_address("grpc")
        assert discovery.get_random_instance_address("grpc") == "address"
        assert discovery.get_instance_address_by_tag("tag", "grpc") == "address"

    def test_disabled_retries(self):
        class MockYpClient(object):
            def get_masters(self, _allow_retries=True):
                assert _allow_retries == False
                return dict(
                    master_infos=[
                        dict(alive=True, fqdn="fqdn", instance_tag="tag", grpc_address="address")
                    ]
                )

        client = MockYpClient()
        discovery = YpMasterDiscovery(client, expiration_time=2000)
        assert discovery.get_random_instance_address("grpc") == "address"

    def test_get_masters_method_allow_retries_option(self, yp_env):
        class ChaosMonkey(object):
            def __init__(self):
                self._request_count = 0

            def __call__(self):
                self._request_count += 1
                return self._request_count == 1

        def create_client():
            retries_config = update(
                get_default_retries_config(), dict(_CHAOS_MONKEY_FACTORY=lambda: ChaosMonkey()),
            )
            return yp_env.yp_instance.create_client(config=dict(retries=retries_config))

        with create_client() as yp_client:
            with pytest.raises(ChaosMonkeyError):
                yp_client.get_masters(_allow_retries=False)

        with create_client() as yp_client:
            yp_client.get_masters(_allow_retries=True)
