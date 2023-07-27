OWNER(g:yt)

PROGRAM(ytserver-job-proxy)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/server/job_proxy
)

END()
