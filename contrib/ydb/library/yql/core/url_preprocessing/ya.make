LIBRARY()

SRCS(
    url_mapper.cpp
    pattern_group.cpp
    url_preprocessing.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/url_preprocessing/interface
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/utils/log
    library/cpp/regex/pcre
)

END()

RECURSE(
    interface
)
