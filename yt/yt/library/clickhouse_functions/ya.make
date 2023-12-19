LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    contrib/clickhouse/src
)

ADDINCL(
    contrib/clickhouse/src
    contrib/libs/double-conversion
    contrib/libs/poco/Foundation/include
    contrib/libs/poco/Net/include
    contrib/libs/poco/Util/include
    contrib/libs/poco/XML/include
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
