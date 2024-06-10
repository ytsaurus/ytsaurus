PROGRAM()

PEERDIR(
    contrib/libs/protoc
    contrib/ydb/public/api/protos/annotations
    contrib/ydb/public/lib/protobuf
)

SRCS(
    main.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
