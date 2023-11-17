PROGRAM(astdiff)

SRCS(
    astdiff.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils/backtrace
)

END()
