PROGRAM(unittester)

SRCS(
    ${ARCADIA_ROOT}/contrib/testing/framework.cpp
    utmain.cpp
    change_log_ut.cpp
    async_result_ut.cpp
    snapshot_ut.cpp
    preprocessor_ut.cpp
    ref_counted_tracker_ut.cpp
    pattern_formatter_ut.cpp
    enum_ut.cpp
    yverify_ut.cpp
    metric_ut.cpp
    #meta_state_map_ut.cpp
    foreach_ut.cpp
    thread_affinity_ut.cpp
    ytree_fluent_ut.cpp
    yson_reader_ut.cpp
    yson_writer_ut.cpp
    zigzag_ut.cpp
    varint_ut.cpp
)

PEERDIR(
    yt/ytlib
)

END()
