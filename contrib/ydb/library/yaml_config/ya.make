LIBRARY()

SRCS(
    console_dumper.cpp
    console_dumper.h
    serialize_deserialize.cpp
    yaml_config.cpp
    yaml_config.h
    yaml_config_helpers.cpp
    yaml_config_helpers.h
    yaml_config_parser.cpp
    yaml_config_parser.h
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/protobuf
    library/cpp/protobuf/json
    contrib/ydb/core/base
    contrib/ydb/core/cms/console/util
    contrib/ydb/core/config/validation
    contrib/ydb/core/erasure
    contrib/ydb/core/protos
    contrib/ydb/core/protos/out
    contrib/ydb/library/actors/core
    contrib/ydb/library/fyamlcpp
    contrib/ydb/library/yaml_config/protos
    contrib/ydb/library/yaml_config/public
    contrib/ydb/library/yaml_json
)

END()

RECURSE(
    deprecated
    protos
    public
    static_validator
    tools
    validator
)

RECURSE_FOR_TESTS(
    ut
    ut_transform
)
