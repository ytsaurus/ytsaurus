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
    util
    mapreduce/yt/yson
    library/json
    library/svnversion
)

END()
