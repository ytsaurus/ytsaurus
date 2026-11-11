LIBRARY()

SRCS(
    manager.h
    manager.cpp
)

STYLE_CPP()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    yql/essentials/providers/common/metrics
    yql/essentials/utils
)

END()
