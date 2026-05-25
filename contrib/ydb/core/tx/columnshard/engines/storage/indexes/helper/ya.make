LIBRARY()

SRCS(
    case_helper.cpp
    index_defaults.cpp
    index_parameters.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
)

END()
