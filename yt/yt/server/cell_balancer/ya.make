LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    bundle_controller.cpp
    bundle_controller_service.cpp
    bundle_scheduler.cpp
    cell_downtime_tracker.cpp
    cell_tracker.cpp
    cell_tracker_impl.cpp
    chaos_scheduler.cpp
    cluster_state_provider.cpp
    config.cpp
    cypress_bindings.cpp
    helpers.cpp
    input_state.cpp
    instance_manager.cpp
    mutations.cpp
    node_tag_filters_manager.cpp
    orchid_bindings.cpp
    pod_id_helpers.cpp
    program.cpp
    proxy_roles_manager.cpp
    rpc_proxy_allocator_adapter.cpp
    system_accounts.cpp
    tablet_node_allocator_adapter.cpp
)

PEERDIR(
    yt/yt/server/master

    yt/yt/server/lib
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/tablet_server

    yt/yt/library/monitoring
    yt/yt/library/orchid
    yt/yt/library/server_program

    library/cpp/getopt
    library/cpp/yt/phdr_cache
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
