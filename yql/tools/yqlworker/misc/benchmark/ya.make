IF (OS_LINUX OR OS_DARWIN)
    G_BENCHMARK()

    SRCS(
        duplex_ipc_channel_bench.cpp
    )

    PEERDIR(
        yql/tools/yqlworker/misc
        yql/tools/yqlworker/misc/test_support
    )

    END()
ENDIF()
