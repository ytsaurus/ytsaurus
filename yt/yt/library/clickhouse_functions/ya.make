LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    contrib/clickhouse/src
    library/cpp/json/yson
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
    GLOBAL yson_to_json.cpp
    unescaped_yson.cpp
    GLOBAL ypath.cpp
    GLOBAL yson_functions.cpp
    yson_parser_adapter.cpp
    yson_extract_tree.cpp
)

END()
