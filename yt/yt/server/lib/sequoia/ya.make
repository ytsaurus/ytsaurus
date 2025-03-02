LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    cypress_transaction.cpp
    helpers.cpp
    protobuf_helpers.cpp

    proto/cypress_proxy_tracker.proto
    proto/transaction_manager.proto
)

PEERDIR(
    yt/yt/ytlib
)

END()
