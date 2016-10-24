PROGRAM(library)

OWNER(
    ermolovd
    monster
)

SRCS(
    main.cpp
    test_temptable.cpp
)

PEERDIR(
    mapreduce/yt/tests/lib
    mapreduce/yt/tests/util
    mapreduce/lib
    mapreduce/library/temptable
)

END()

RECURSE(test)
