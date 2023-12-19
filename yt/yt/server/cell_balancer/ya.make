LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    bundle_controller.cpp
    bundle_scheduler.cpp
    cell_tracker.cpp
    cell_tracker_impl.cpp
    cluster_state_provider.cpp
    cypress_bindings.cpp
    config.cpp
    master_connector.cpp
    node_tag_filters_manager.cpp
    orchid_bindings.cpp
    program.cpp
    proxy_roles_manager.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/yt/phdr_cache
    yt/yt/server/lib
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/tablet_server
    yt/yt/server/master
    yt/yt/server/node
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()

RECURSE_FOR_TESTS(
    unittests
)
