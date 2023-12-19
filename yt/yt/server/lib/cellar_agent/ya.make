LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    cellar.cpp
    cellar_manager.cpp
    config.cpp
    helpers.cpp
    master_connector_helpers.cpp
    occupant.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/server/lib/hive
    yt/yt/server/lib/transaction_supervisor
)

END()
