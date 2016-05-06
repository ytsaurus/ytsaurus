PROGRAM(common)

OWNER(monster)

SRCS(
    helpers.h
    helpers.cpp
    main.cpp
    test_operations.cpp
    test_table.cpp
)

PEERDIR(
    mapreduce/yt/tests/lib
    mapreduce/lib
)

END()

RECURSE(test)
