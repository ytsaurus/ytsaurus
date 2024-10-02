LIBRARY()

SRCS(
    matcher.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/restricted/googletest/googlemock
    contrib/restricted/googletest/googletest
)

END()

RECURSE_FOR_TESTS(ut)
