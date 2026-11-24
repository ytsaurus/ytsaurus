import threading

from concurrent import futures

import grpc

from contrib.ydb.public.api.grpc import ydb_auth_v1_pb2_grpc, ydb_federation_discovery_v1_pb2_grpc
from contrib.ydb.public.api.protos import ydb_auth_pb2, ydb_federation_discovery_pb2, ydb_status_codes_pb2

_STATUS_BY_NAME = {
    "AVAILABLE": ydb_federation_discovery_pb2.DatabaseInfo.AVAILABLE,
    "READ_ONLY": ydb_federation_discovery_pb2.DatabaseInfo.READ_ONLY,
    "UNAVAILABLE": ydb_federation_discovery_pb2.DatabaseInfo.UNAVAILABLE,
}


class _LoginService(ydb_auth_v1_pb2_grpc.AuthServiceServicer):
    def Login(self, request, context):
        response = ydb_auth_pb2.LoginResponse()
        response.operation.ready = True
        response.operation.status = ydb_status_codes_pb2.StatusIds.SUCCESS
        response.operation.result.Pack(ydb_auth_pb2.LoginResult(token="federation-discovery-mock-token"))
        return response


class FederationDiscoveryMock(ydb_federation_discovery_v1_pb2_grpc.FederationDiscoveryServiceServicer):
    """Serves ListFederationDatabases from a mutable in-memory cluster list, so a test may flip a
    cluster's status while the pipeline is running."""

    def __init__(self, databases, port):
        # databases: list of {"name", "endpoint", "path", "status"} where status is one of _STATUS_BY_NAME.
        self._databases = [dict(database) for database in databases]
        self._lock = threading.Lock()
        self._port = port
        self.endpoint = f"127.0.0.1:{port}"
        self._server = None

    def __enter__(self):
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        ydb_federation_discovery_v1_pb2_grpc.add_FederationDiscoveryServiceServicer_to_server(self, self._server)
        ydb_auth_v1_pb2_grpc.add_AuthServiceServicer_to_server(_LoginService(), self._server)
        self._server.add_insecure_port(f"[::]:{self._port}")
        self._server.start()
        return self

    def __exit__(self, *_):
        self._server.stop(grace=None)

    def set_status(self, name, status):
        with self._lock:
            for database in self._databases:
                if database["name"] == name:
                    database["status"] = status
                    return
        raise KeyError(name)

    def ListFederationDatabases(self, request, context):
        result = ydb_federation_discovery_pb2.ListFederationDatabasesResult()
        result.self_location = self._databases[0]["name"]
        with self._lock:
            for database in self._databases:
                info = result.federation_databases.add()
                info.name = database["name"]
                info.id = database["name"]
                info.path = database["path"]
                info.endpoint = database["endpoint"]
                info.location = database["name"]
                # The SDK treats a zero-weight database as unusable, so it must be set even though this
                # connector never asks the SDK to choose a cluster.
                info.weight = 100
                info.status = _STATUS_BY_NAME[database["status"]]

        response = ydb_federation_discovery_pb2.ListFederationDatabasesResponse()
        response.operation.ready = True
        response.operation.status = ydb_status_codes_pb2.StatusIds.SUCCESS
        response.operation.result.Pack(result)
        return response
