LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    events_on_fs.cpp
    job_probe.cpp
    orchid.cpp

    proto/job_prober_service.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/library/dns_over_rpc/client
    yt/yt/library/containers/cri
    yt/yt/library/stockpile
    yt/yt/server/lib/misc
    yt/yt/server/lib/rpc_proxy

    yt/yt_proto/yt/client
)

END()
