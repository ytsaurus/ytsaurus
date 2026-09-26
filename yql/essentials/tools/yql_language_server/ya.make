IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

IF (YQL_LANGUAGE_SERVER_VERSION)
    CFLAGS(-DYQL_LANGUAGE_SERVER_VERSION=$YQL_LANGUAGE_SERVER_VERSION)
ENDIF()

PEERDIR(
    yql/essentials/tools/yql_language_server/api
    yql/essentials/tools/yql_language_server/service
    yql/essentials/tools/yql_language_server/lsp/server
    library/cpp/getopt
    library/cpp/time_provider
)

SRCS(
    args.cpp
    main.cpp
    message_capture.cpp
    version.cpp
)

END()

RECURSE(
    api
    ci
    lsp
    service
    testing
)

ENDIF()
