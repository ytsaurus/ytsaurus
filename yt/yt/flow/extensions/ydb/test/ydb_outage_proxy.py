import threading

from concurrent import futures

import grpc

from contrib.ydb.public.api.protos import ydb_discovery_pb2, ydb_topic_pb2


class YdbOutageProxy:
    def __init__(self, upstream, port, on_first_publish=None, on_first_write_ack=None):
        self.endpoint = f"127.0.0.1:{port}"
        self._upstream = upstream.removeprefix("grpc://")
        self._on_first_publish = on_first_publish
        self._on_first_write_ack = on_first_write_ack
        self._publish_seen = threading.Event()
        self._write_ack_seen = threading.Event()
        self._publish_blocked = True
        self._login_response = None
        self._lock = threading.Lock()
        self._server = None
        self._channel = None

    def __enter__(self):
        self._channel = grpc.insecure_channel(self._upstream)
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
        self._server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "Ydb.Auth.V1.AuthService",
                    {"Login": grpc.unary_unary_rpc_method_handler(self._login)},
                ),
                grpc.method_handlers_generic_handler(
                    "Ydb.Discovery.V1.DiscoveryService",
                    {
                        "ListEndpoints": grpc.unary_unary_rpc_method_handler(self._list_endpoints),
                        "WhoAmI": grpc.unary_unary_rpc_method_handler(
                            self._forward_unary("/Ydb.Discovery.V1.DiscoveryService/WhoAmI")
                        ),
                    },
                ),
                grpc.method_handlers_generic_handler(
                    "Ydb.Topic.DeferredPublish.V1.TopicDeferredPublishService",
                    {
                        name: grpc.unary_unary_rpc_method_handler(
                            self._publish
                            if name == "Publish"
                            else self._forward_unary(
                                f"/Ydb.Topic.DeferredPublish.V1.TopicDeferredPublishService/{name}"
                            )
                        )
                        for name in (
                            "BeginPublication",
                            "Publish",
                            "CancelPublication",
                            "ListPublications",
                            "DescribePublication",
                        )
                    },
                ),
                grpc.method_handlers_generic_handler(
                    "Ydb.Topic.V1.TopicService",
                    {
                        "StreamWrite": grpc.stream_stream_rpc_method_handler(self._stream_write),
                        "StreamRead": grpc.stream_stream_rpc_method_handler(
                            self._forward_stream("/Ydb.Topic.V1.TopicService/StreamRead")
                        ),
                        "DescribeTopic": grpc.unary_unary_rpc_method_handler(
                            self._forward_unary("/Ydb.Topic.V1.TopicService/DescribeTopic")
                        ),
                    },
                ),
            )
        )
        self._server.add_insecure_port(f"[::]:{self.endpoint.rsplit(':', 1)[1]}")
        self._server.start()
        return self

    def __exit__(self, *_):
        self._server.stop(grace=None)
        self._channel.close()

    def allow_publish(self):
        with self._lock:
            self._publish_blocked = False

    def wait_publish(self, timeout):
        assert self._publish_seen.wait(timeout), "deferred Publish did not reach the outage proxy"

    def wait_write_ack(self, timeout):
        assert self._write_ack_seen.wait(timeout), "deferred write ACK did not reach the outage proxy"

    @staticmethod
    def _metadata(context):
        return tuple(context.invocation_metadata())

    def _forward_unary(self, method):
        call = self._channel.unary_unary(
            method,
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )

        def forward(request, context):
            try:
                return call(
                    request,
                    metadata=self._metadata(context),
                    timeout=context.time_remaining(),
                )
            except grpc.RpcError as error:
                context.set_trailing_metadata(error.trailing_metadata())
                context.abort(error.code(), error.details())

        return forward

    def _forward_stream(self, method):
        call = self._channel.stream_stream(
            method,
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )

        def forward(requests, context):
            upstream = call(requests, metadata=self._metadata(context))
            context.add_callback(upstream.cancel)
            try:
                yield from upstream
            except grpc.RpcError as error:
                context.set_trailing_metadata(error.trailing_metadata())
                context.abort(error.code(), error.details())

        return forward

    def _list_endpoints(self, request, context):
        response = ydb_discovery_pb2.ListEndpointsResponse.FromString(
            self._forward_unary("/Ydb.Discovery.V1.DiscoveryService/ListEndpoints")(request, context)
        )
        result = ydb_discovery_pb2.ListEndpointsResult()
        assert response.operation.result.Unpack(result)
        for endpoint in result.endpoints:
            endpoint.address = "127.0.0.1"
            endpoint.port = int(self.endpoint.rsplit(":", 1)[1])
            del endpoint.ip_v4[:]
            del endpoint.ip_v6[:]
        response.operation.result.Pack(result)
        return response.SerializeToString()

    def _login(self, request, context):
        with self._lock:
            if self._login_response is not None:
                return self._login_response
        response = self._forward_unary("/Ydb.Auth.V1.AuthService/Login")(request, context)
        with self._lock:
            self._login_response = response
        return response

    def _publish(self, request, context):
        with self._lock:
            blocked = self._publish_blocked
            if blocked and not self._publish_seen.is_set():
                if self._on_first_publish is not None:
                    self._on_first_publish()
                self._publish_seen.set()
        if blocked:
            context.abort(grpc.StatusCode.UNAVAILABLE, "test cluster is unavailable")
        return self._forward_unary("/Ydb.Topic.DeferredPublish.V1.TopicDeferredPublishService/Publish")(
            request, context
        )

    def _stream_write(self, requests, context):
        upstream = self._channel.stream_stream(
            "/Ydb.Topic.V1.TopicService/StreamWrite",
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )(requests, metadata=self._metadata(context))
        context.add_callback(upstream.cancel)
        try:
            for response in upstream:
                parsed = ydb_topic_pb2.StreamWriteMessage.FromServer.FromString(response)
                if self._on_first_write_ack is not None and parsed.HasField("write_response"):
                    callback = self._on_first_write_ack
                    self._on_first_write_ack = None
                    callback()
                    self._write_ack_seen.set()
                    context.abort(grpc.StatusCode.UNAVAILABLE, "test write acknowledgement is unavailable")
                yield response
        except grpc.RpcError as error:
            context.set_trailing_metadata(error.trailing_metadata())
            context.abort(error.code(), error.details())
