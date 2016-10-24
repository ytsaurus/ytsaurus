PROGRAM(common)

OWNER(
    ermolovd
    monster
)

SRCS(
    main.cpp
    test_cypress_manipulations.cpp
    test_operations.cpp
    test_table.cpp
    test_update.cpp
    test_meta_attrs.cpp
)

PEERDIR(
    mapreduce/yt/client
    mapreduce/yt/tests/lib
    mapreduce/yt/tests/operations
    mapreduce/yt/tests/util
    mapreduce/lib
)

END()

RECURSE(
    test
)
