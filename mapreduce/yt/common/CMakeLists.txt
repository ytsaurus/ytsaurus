LIBRARY()

OWNER(monster)

SRCS(
    config.cpp
    log.cpp
    serialize.cpp
    helpers.cpp
    node_builder.cpp
    node_visitor.cpp
)

PEERDIR(
    library/json
    library/svnversion
    library/yson
    util
)

END()
