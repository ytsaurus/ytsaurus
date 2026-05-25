LIBRARY()

SRCS(
    fmr_download.cpp
)

PEERDIR(
    yql/essentials/core/file_storage
    yql/essentials/core/file_storage/defs
    yql/essentials/core/file_storage/http_download
    yql/essentials/utils/log
    yql/essentials/utils

    yt/yql/providers/yt/fmr/vanilla/peer_tracker

    yt/cpp/mapreduce/interface
)

YQL_LAST_ABI_VERSION()

END()

