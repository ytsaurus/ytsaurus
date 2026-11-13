LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    GLOBAL source.cpp
    static_table_export.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/extensions/static_table_export/group_by_system_timestamp
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/process_function/host
)

END()
