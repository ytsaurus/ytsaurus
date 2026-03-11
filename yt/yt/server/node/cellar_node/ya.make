LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap.cpp
    bundle_controller_connector.cpp
    bundle_dynamic_config_manager.cpp
    config.cpp
    master_connector.cpp
    security_manager.cpp
    tablet_cell_snapshot_validator.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/library/profiling/solomon
)

END()
