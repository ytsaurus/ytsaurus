PROGRAM(ytserver-yql-agent)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

ALLOCATOR(LF)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yql/agent
    yt/yql/plugin/process
)

END()
