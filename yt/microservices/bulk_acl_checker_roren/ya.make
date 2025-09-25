PROGRAM()

SRCS(
    data.proto
    main.cpp
    import_snapshot.cpp
    remove_excessive.cpp
    full.cpp
)

PEERDIR(
    yt/yt_proto/yt/formats
    yt/cpp/roren/interface
    yt/cpp/roren/yt
    yt/cpp/mapreduce/util
    yt/cpp/mapreduce/interface/logging
    yt/yt/client/hedging
    yt/yt/core
    yt/yt/library/named_value
    yt/yt/library/auth
    library/cpp/getopt
    contrib/libs/fmt
    yt/microservices/lib/cpp/get_snapshots
)

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()
