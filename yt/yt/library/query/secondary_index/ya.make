LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    schema.cpp
    transform.cpp
)

PEERDIR(
    yt/yt/library/query/base
)

END()
