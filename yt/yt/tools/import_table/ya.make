PROGRAM()

PEERDIR(
    yt/yt/tools/import_table/lib
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/util

    yt/cpp/mapreduce/library/table_schema

    library/cpp/yson/node
    library/cpp/getopt
)

SRCS(
    main.cpp
)

END()

IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        unittests
    )
ENDIF()
