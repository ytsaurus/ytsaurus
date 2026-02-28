LIBRARY()

SRCS(
    mlp_changer.cpp
    mlp_purger.cpp
    mlp_reader.cpp
    mlp_writer.cpp
    mlp.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public
    contrib/ydb/core/persqueue/public/describer
)

END()

RECURSE_FOR_TESTS(
    ut
)
