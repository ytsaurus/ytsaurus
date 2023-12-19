LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    avenue_directory.cpp
    config.cpp
    helpers.cpp
    hive_manager.cpp
    mailbox.cpp
    public.cpp

    proto/hive_manager.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/server/lib/election
    yt/yt/server/lib/hydra
)

END()
