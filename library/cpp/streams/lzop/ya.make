LIBRARY()

PEERDIR(
    contrib/libs/minilzo
)

SRCS(
    lzop.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
