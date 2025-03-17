PROGRAM(config_proto_plugin)

PEERDIR(
    contrib/libs/protoc
    library/cpp/protobuf/json
    contrib/ydb/public/lib/protobuf
    contrib/ydb/core/config/protos
    contrib/ydb/core/config/utils
)

SRCS(
    main.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
