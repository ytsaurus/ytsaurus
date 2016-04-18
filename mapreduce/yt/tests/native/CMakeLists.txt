PROGRAM(native)

OWNER(monster)

SRCS(
    sample.proto
    cypress.cpp
    io.cpp
    operations.cpp
    main.cpp
)

PEERDIR(
    mapreduce/yt/tests/lib
    mapreduce/yt/client
)

END()

RECURSE(test)

