LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    actions.cpp
    action_helpers.cpp
    bootstrap.cpp
    config.cpp
    dynamic_config_manager.cpp
    helpers.cpp
    node_proxy.cpp
    node_proxy_base.cpp
    object_service.cpp
    path_resolver.cpp
    per_user_and_workload_request_queue_provider.cpp
    program.cpp
    rootstock_proxy.cpp
    response_keeper.cpp
    sequoia_service.cpp
    sequoia_session.cpp
    sequoia_tree_visitor.cpp
    user_directory_synchronizer.cpp
    user_directory.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache
    library/cpp/iterator

    library/cpp/getopt

    yt/yt/library/dynamic_config
    yt/yt/library/server_program

    yt/yt/ytlib

    yt/yt/server/lib/admin
    yt/yt/server/lib/misc
    yt/yt/server/lib/cypress_registrar
)

END()
