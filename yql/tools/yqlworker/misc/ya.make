LIBRARY()

SRCS(
    background_threads.cpp
    background_threads.h
    conductor.cpp
    conductor.h
    deploy_sd.cpp
    deploy_sd.h
    expiration_tracker.cpp
    expiration_tracker.h
    socket_transfer.cpp
    socket_transfer.h
)

IF (NOT WINDOWS)
    SRCS(
        multiprocessing.cpp
        multiprocessing.h
        duplex_ipc_channel.cpp
        duplex_ipc_channel.h
    )
ENDIF()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/http/io
    library/cpp/json/yson
    library/cpp/logger/global
    library/cpp/openssl/io
    library/cpp/protobuf/json
    yql/essentials/utils
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/utils/signals
    yql/essentials/utils/fetch
    yt/yql/providers/yt/lib/log
    yql/tools/yqlworker/config
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
