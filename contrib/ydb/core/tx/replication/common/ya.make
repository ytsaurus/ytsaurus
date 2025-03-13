LIBRARY()

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/protobuf_printer
)

SRCS(
    sensitive_event_pb.h
    worker_id.cpp
)

YQL_LAST_ABI_VERSION()

END()
