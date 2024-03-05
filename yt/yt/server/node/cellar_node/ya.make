LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap.cpp
    config.cpp
    bundle_dynamic_config_manager.cpp
    master_connector.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
