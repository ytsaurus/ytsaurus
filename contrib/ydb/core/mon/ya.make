LIBRARY()

SRCS(
    async_http_mon.cpp
    async_http_mon.h
    mon.cpp
    mon.h
    sync_http_mon.cpp
    sync_http_mon.h
    crossref.cpp
    crossref.h
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/lwtrace/mon
    library/cpp/string_utils/url
    contrib/ydb/core/base
    contrib/ydb/core/driver_lib/version
    contrib/ydb/core/protos
    contrib/ydb/library/aclib
)

END()
