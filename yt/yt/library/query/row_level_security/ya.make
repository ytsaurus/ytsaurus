LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL row_level_security.cpp
)

ADDINCL(
    contrib/libs/sparsehash/src
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/library/codegen
    yt/yt/library/query/row_level_security_api
    contrib/libs/sparsehash
)

END()
