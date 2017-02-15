PROGRAM(test_temp_table)

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
    mapreduce/lib
    mapreduce/library/temptable
)

END()
