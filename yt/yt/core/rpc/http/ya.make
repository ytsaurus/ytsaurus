LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

OWNER(g:yt babenko)

SRCS(
    server.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/http
)

END()
