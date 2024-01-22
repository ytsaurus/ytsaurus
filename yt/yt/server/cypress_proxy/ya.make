LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    action_helpers.cpp
    bootstrap.cpp
    config.cpp
    dynamic_config_manager.cpp
    helpers.cpp
    node_proxy.cpp
    object_service.cpp
    path_resolver.cpp
    program.cpp
    rootstock_proxy.cpp
    sequoia_service.cpp
    sequoia_tree_visitor.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    library/cpp/getopt

    yt/yt/library/dynamic_config

    yt/yt/ytlib

    yt/yt/server/lib/admin
    yt/yt/server/lib/misc
    yt/yt/server/lib/cypress_registrar
)

END()

