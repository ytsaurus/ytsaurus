IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")
    PROGRAM()

    PEERDIR(
        library/cpp/resource
        library/cpp/getopt
        yql/essentials/sql/v1/highlight
    )

    SRCS(
        yql_highlight.cpp
    )

    RESOURCE(
        DONT_PARSE yql/essentials/tools/yql_highlight/resource/README.md README.md
    )

    END()
ENDIF()
