GTEST(unittester-queue-agent)

SRCS(
    ytree_helpers_ut.cpp
    queue_export_ut.cpp
)

PEERDIR(
    library/cpp/iterator
    library/cpp/testing/common
    library/cpp/testing/hook

    yt/yt/server/queue_agent
)

END()
