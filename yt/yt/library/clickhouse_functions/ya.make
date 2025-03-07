LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    contrib/clickhouse/src
)

ADDINCL(
    contrib/clickhouse/src
    contrib/libs/double-conversion
    contrib/clickhouse/base/poco/Foundation/include
    contrib/clickhouse/base/poco/Net/include
    contrib/clickhouse/base/poco/Util/include
    contrib/clickhouse/base/poco/XML/include
    contrib/libs/re2
    contrib/libs/rapidjson/include
)

SRCS(
    GLOBAL convert_yson.cpp
    unescaped_yson.cpp
    GLOBAL ypath.cpp
    GLOBAL yson_extract.cpp
    yson_parser_adapter.cpp
)

END()
