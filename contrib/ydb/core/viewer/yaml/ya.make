LIBRARY()

SRCS(
    yaml.cpp
    yaml.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yaml/as
    contrib/ydb/core/viewer/protos
)

END()
