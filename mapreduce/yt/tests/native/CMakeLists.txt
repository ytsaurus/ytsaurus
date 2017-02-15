PROGRAM(native)

OWNER(
    ermolovd
    monster
)

SRCS(
    sample.proto
    cypress.cpp
    io.cpp
    operations.cpp
    schema.cpp
    tablets.cpp
    main.cpp
)

PEERDIR(
    library/dbg_output
    mapreduce/yt/tests/lib
    mapreduce/yt/client
    mapreduce/yt/common
    mapreduce/yt/interface
)

END()

RECURSE(test)

