PROGRAM(common)

OWNER(monster)

SRCS(
    main.cpp
    test_operations.cpp
    test_table.cpp
    test_update.cpp
)

PEERDIR(
    mapreduce/yt/tests/lib
    mapreduce/yt/tests/util
    mapreduce/lib
)

END()

RECURSE(test)
