LIBRARY()

PEERDIR(
    library/cpp/regex/hyperscan
    contrib/ydb/library/rewrapper
)

SRCS(
    GLOBAL hyperscan.cpp
)

END()

