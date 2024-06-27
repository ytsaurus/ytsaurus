LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    cypress_transaction.cpp
    protobuf_helpers.cpp

    proto/transaction_manager.proto
)

PEERDIR(
    yt/yt/ytlib
)

END()
