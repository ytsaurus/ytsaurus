PROGRAM(unittester)

SRCS(
    utmain.cpp
    framework/framework.cpp
    #change_log_ut.cpp
    async_result_ut.cpp
    snapshot_ut.cpp
    preprocessor_ut.cpp
    ref_counted_tracker_ut.cpp
    pattern_formatter_ut.cpp
    enum_ut.cpp
)

PEERDIR(
    yt/ytlib
)

END()
