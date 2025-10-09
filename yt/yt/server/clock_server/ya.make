LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    cluster_clock/automaton.cpp
    cluster_clock/bootstrap.cpp
    cluster_clock/config.cpp
    cluster_clock/hydra_facade.cpp
    cluster_clock/serialize.cpp
    cluster_clock/program.cpp
)

PEERDIR(
    yt/yt/library/orchid
    yt/yt/library/server_program
    yt/yt/library/monitoring
    yt/yt/library/profiling/solomon

    yt/yt/server/lib
    yt/yt/server/lib/hydra
    yt/yt/server/lib/hive
    yt/yt/server/lib/timestamp_server
)

END()
