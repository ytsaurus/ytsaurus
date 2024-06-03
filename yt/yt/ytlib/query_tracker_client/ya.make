LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

GENERATE_YT_RECORD(
    records/query.yaml
    OUTPUT_INCLUDES
    yt/yt/ytlib/query_tracker_client/public.h
    yt/yt/client/table_client/record_codegen_deps.h
    yt/yt/core/yson/string.h
    yt/yt/core/misc/error.h
)

SRCS(
    proto/query_tracker_service.proto

    config.cpp
    helpers.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt_proto/yt/client
)

END()
