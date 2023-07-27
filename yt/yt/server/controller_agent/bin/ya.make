PROGRAM(ytserver-controller-agent)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

OWNER(g:yt)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/server/controller_agent
)

END()
