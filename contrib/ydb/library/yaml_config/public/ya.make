LIBRARY()

SRCS(
    yaml_config.cpp
    yaml_config.h
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/protobuf
    contrib/libs/yaml-cpp
    contrib/ydb/library/actors/core
    library/cpp/protobuf/json
    contrib/ydb/library/fyamlcpp
)

END()
