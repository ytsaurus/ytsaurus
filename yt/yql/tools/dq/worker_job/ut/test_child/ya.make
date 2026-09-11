PROGRAM(dq_worker_runtime_test_child)

PEERDIR(
    contrib/libs/libiconv/dynamic
)

LDFLAGS(-Wl,-rpath,${"$"}ORIGIN)

SRCS(
    main.cpp
)

END()
