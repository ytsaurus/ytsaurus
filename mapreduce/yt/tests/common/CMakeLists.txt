PROGRAM(common)

OWNER(monster)

SRCS(
    operations.cpp
    table.cpp
    main.cpp
)

PEERDIR(
    mapreduce/yt/tests/lib
    mapreduce/lib
)

END()

RECURSE(test)

