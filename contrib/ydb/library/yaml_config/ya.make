LIBRARY()

SRCS(
    console_dumper.cpp
    console_dumper.h
    yaml_config.cpp
    yaml_config.h
    yaml_config_parser.cpp
    yaml_config_parser.h
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/protobuf
    contrib/libs/yaml-cpp
    library/cpp/actors/core
    library/cpp/protobuf/json
    library/cpp/yaml/fyamlcpp
    contrib/ydb/core/base
    contrib/ydb/core/cms/console/util
    contrib/ydb/core/erasure
    contrib/ydb/core/protos
    contrib/ydb/library/yaml_config/public
)

END()

RECURSE(
    public
    validator
    static_validator
)

RECURSE_FOR_TESTS(
    ut
)
