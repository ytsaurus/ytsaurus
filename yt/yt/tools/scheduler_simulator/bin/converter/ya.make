PROGRAM(convert_operations_to_binary_format)

ALLOCATOR(YT)

SRCS(
    convert.cpp
)

ADDINCL(
    yt/yt/tools/scheduler_simulator
)

PEERDIR(
    yt/yt/tools/scheduler_simulator
)

END()
