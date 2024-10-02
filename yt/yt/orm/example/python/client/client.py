import yt.yt.orm.example.python.client.data_model as data_model

from yt.yt.orm.example.python.common.logger import logger

import yt.yt.orm.example.client.proto.api.discovery_service_pb2 as discovery_service_pb2
import yt.yt.orm.example.client.proto.api.discovery_service_pb2_grpc as discovery_service_pb2_grpc

import yt.yt.orm.example.client.proto.api.object_service_pb2 as object_service_pb2
import yt.yt.orm.example.client.proto.api.object_service_pb2_grpc as object_service_pb2_grpc

from yt.orm.client.client import (
    GrpcServices,
    OrmClientTraits,
    OrmClient,
    find_token as orm_find_token,
)
from yt.orm.client.discovery import MasterDiscovery

from copy import deepcopy


def find_token(allow_receive_token_by_ssh_session=True):
    return orm_find_token(
        logger,
        env_name="EXAMPLE_TOKEN",
        dir_name=".example",
        oauth_client_id="",
        oauth_client_secret="",
        allow_receive_token_by_ssh_session=allow_receive_token_by_ssh_session,
    )


EXAMPLE_CLIENT_TRAITS = OrmClientTraits(
    object_type_enum_type=data_model.EObjectType,
    access_control_permission_enum_type=data_model.EAccessControlPermission,
    access_control_action_enum_type=data_model.EAccessControlAction,
)


# Use ExampleClient as a context manager or call ExampleClient.close at the end (see _GrpcLayer.close for details).
class ExampleClient(OrmClient):
    def __init__(
        self,
        address,
        ip6_address=None,
        transport=None,
        config=None,
        grpc_services=None,
        http_session=None,
    ):
        if grpc_services:
            grpc_services = deepcopy(grpc_services)
        else:
            grpc_services = GrpcServices()
        grpc_services.add_service(
            "object_service",
            object_service_pb2_grpc.ObjectServiceStub,
        )
        grpc_services.add_service(
            "discovery_service",
            discovery_service_pb2_grpc.DiscoveryServiceStub,
        )
        self._use_ip6_addresses = ip6_address is not None
        OrmClient.__init__(
            self,
            address=address,
            ip6_address=ip6_address,
            transport=transport,
            config=config,
            grpc_services=grpc_services,
            http_session=http_session,
        )

    def get_master_discovery(self):
        return MasterDiscovery(
            client=self,
            expiration_time=self._config["master_discovery_expiration_time"],
            logger=logger,
            human_readable_orm_name="Example",
            use_ip6_addresses=self._use_ip6_addresses,
        )

    def get_logger(self):
        return logger

    @property
    def client_traits(self):
        return EXAMPLE_CLIENT_TRAITS

    def get_discovery_service_pb2(self):
        return discovery_service_pb2

    def get_object_service_pb2(self):
        return object_service_pb2

    def get_grpc_default_address_suffix(self):
        return ".example.yandex.net:8090"

    def get_http_default_address_suffix(self):
        return ".example.yandex.net:8443"

    def get_select_object_history_index_mode(self):
        return object_service_pb2.ESelectObjectHistoryIndexMode
