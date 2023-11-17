LIBRARY()

PEERDIR(
    contrib/ydb/library/schlab/schine
    contrib/ydb/library/schlab/schoot
)

SRCS(
    defs.h
    schemu.h
    schemu.cpp
)

END()

RECURSE()
