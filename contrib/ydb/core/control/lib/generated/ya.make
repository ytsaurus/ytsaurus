LIBRARY()

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/control/lib/base
    contrib/ydb/library/yverify_stream
    library/cpp/threading/hot_swap
)

RUN_PROGRAM(
    contrib/ydb/core/control/lib/generated/codegen
        control_board_proto.h.in
        control_board_proto.h
    OUTPUT_INCLUDES
        contrib/ydb/core/protos/tablet.pb.h
        contrib/ydb/core/protos/config.pb.h
    IN
        control_board_proto.h.in
    OUT
        control_board_proto.h
)

RUN_PROGRAM(
    contrib/ydb/core/control/lib/generated/codegen
        control_board_proto.cpp.in
        control_board_proto.cpp
    OUTPUT_INCLUDES
        contrib/ydb/core/protos/tablet.pb.h
        contrib/ydb/core/protos/config.pb.h
        contrib/ydb/core/control/lib/generated/control_board_proto.h
    IN
        control_board_proto.cpp.in
    OUT
        control_board_proto.cpp
)

END()

RECURSE(
    codegen
)
