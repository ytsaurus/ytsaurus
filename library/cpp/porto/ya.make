LIBRARY()

PEERDIR(
    library/cpp/porto/proto
    contrib/libs/protobuf
)

SRCS(
    libporto.cpp
    metrics.cpp
)

END()

RECURSE_FOR_TESTS(ut)
