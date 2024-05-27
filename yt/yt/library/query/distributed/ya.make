LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    join_tree.cpp
    shuffle.cpp
)

PEERDIR(
    yt/yt/library/query/base
)

END()
