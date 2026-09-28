IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

IF(BUILD_TYPE == RELEASE)
    STRIP()
ENDIF()

IF (YQL_LANGUAGE_SERVER_VERSION)
    CFLAGS(-DYQL_LANGUAGE_SERVER_VERSION=$YQL_LANGUAGE_SERVER_VERSION)
ENDIF()

PEERDIR(
    yql/essentials/tools/yql_language_server/api
    yql/essentials/tools/yql_language_server/core
    yql/essentials/tools/yql_language_server/service
    yql/essentials/tools/yql_language_server/lsp/server
    library/cpp/getopt
    library/cpp/time_provider
)

SRCS(
    args.cpp
    main.cpp
    message_capture.cpp
)

END()

RECURSE(
    api
    core
    lsp
    service
    testing
)

ENDIF()
