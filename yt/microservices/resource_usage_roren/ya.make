PROGRAM()

SRCS(
    data.proto
    import_snapshot.cpp
    remove_excessive.cpp
    main.cpp
)

PEERDIR(
    yt/yt_proto/yt/formats
    yt/cpp/roren/interface
    yt/cpp/roren/yt
    yt/cpp/mapreduce/tests/yt_initialize_hook
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/util
    yt/microservices/resource_usage_roren/lib
    yt/yt/core
    yt/yt/library/named_value
    yt/yt/library/auth
    library/cpp/getopt
    library/cpp/iterator
    contrib/libs/fmt
    yt/microservices/lib/cpp/get_snapshots
)

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        tests
        ut
    )
ENDIF()
