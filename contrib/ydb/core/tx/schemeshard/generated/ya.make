LIBRARY()

PEERDIR(
    contrib/ydb/core/protos
)

RUN_PROGRAM(
    contrib/ydb/core/tx/schemeshard/generated/codegen
        dispatch_op.h.in
        dispatch_op.h
    IN dispatch_op.h.in
    OUT dispatch_op.h
    OUTPUT_INCLUDES
        contrib/ydb/core/protos/flat_scheme_op.pb.h
)

END()

RECURSE(
    codegen
)
