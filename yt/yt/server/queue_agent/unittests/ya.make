GTEST(unittester-federated-client)

SUBSCRIBER(g:yt)

SRCS(
    queue_export_ut.cpp
)

PEERDIR(
    library/cpp/iterator
    library/cpp/testing/common
    library/cpp/testing/hook

    yt/yt/server/queue_agent
)

END()
