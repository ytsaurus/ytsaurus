IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    contrib/restricted/patched/replxx
    library/cpp/getopt
    yql/essentials/sql/v1/complete
)

SRCS(
    yql_complete.cpp
)

END()

ENDIF()
