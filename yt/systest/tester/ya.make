PROGRAM(systest)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/systest
)

IF (OS_LINUX OR OS_FREEBSD)
    EXTRALIBS(-lutil)
ENDIF()

END()
