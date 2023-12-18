PROGRAM(iotest)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    configuration.cpp
    driver.cpp
    driver_factory.cpp
    linuxaio.cpp
    main.cpp
    meters.cpp
    operation.cpp
    output.cpp
    pread.cpp
    pstat.cpp
    rusage.cpp
    statistics.cpp
    throttler.cpp
    uname.cpp
    uring.cpp
    worker.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/yt/phdr_cache
    contrib/libs/liburing
    library/cpp/getopt/small
)

END()
