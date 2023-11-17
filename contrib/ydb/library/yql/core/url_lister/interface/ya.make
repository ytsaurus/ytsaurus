LIBRARY()

SRCS(
    url_lister.cpp
    url_lister_manager.cpp
)

PEERDIR(
    library/cpp/uri
    library/cpp/yson/node
    contrib/ydb/library/yql/core/credentials
    contrib/ydb/library/yql/core/url_preprocessing/interface
)

END()
