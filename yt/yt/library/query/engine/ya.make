LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    GLOBAL append_function_implementation.cpp
    cg_fragment_compiler.cpp
    cg_helpers.cpp
    cg_ir_builder.cpp
    cg_routines.cpp
    GLOBAL column_evaluator.cpp
    GLOBAL coordinator.cpp
    GLOBAL evaluator.cpp
    folding_profiler.cpp
    functions_cg.cpp
    functions_builder.cpp
    GLOBAL builtin_function_profiler.cpp
    GLOBAL range_inferrer.cpp
    position_independent_value_caller.cpp
    GLOBAL new_range_inferrer.cpp
    web_assembly_caller.cpp
    web_assembly_data_transfer.cpp
    web_assembly_type_builder.cpp
)

IF (OPENSOURCE)
    SRCS(
        disable_system_libraries.cpp
    )
ELSE()
    SRCS(
        enable_system_libraries.cpp
    )
ENDIF()

ADDINCL(
    contrib/libs/sparsehash/src
    contrib/libs/re2
    contrib/libs/xdelta3

    contrib/restricted/wavm/Include
)

CFLAGS(
    -DWASM_C_API=WAVM_API
    -DWAVM_API=
)

PEERDIR(
    yt/yt/core
    yt/yt/library/codegen
    yt/yt/library/web_assembly/api
    yt/yt/library/web_assembly/engine
    yt/yt/library/query/base
    yt/yt/library/query/engine_api
    yt/yt/library/query/misc
    yt/yt/library/query/proto
    yt/yt/client
    library/cpp/yt/memory
    library/cpp/xdelta3/state
    contrib/libs/sparsehash
)

USE_LLVM_BC14()

LLVM_BC(
    udf/hyperloglog.cpp
    NAME hyperloglog
    SYMBOLS
        cardinality_init
        cardinality_update
        cardinality_merge
        cardinality_finalize
)

LLVM_BC(
    udf/farm_hash.cpp
    NAME farm_hash
    SYMBOLS
        farm_hash
)

LLVM_BC(
    udf/bigb_hash.cpp
    NAME bigb_hash
    SYMBOLS
        bigb_hash
)

LLVM_BC(
    udf/make_map.cpp
    NAME make_map
    SYMBOLS
        make_map
)

LLVM_BC(
    udf/make_list.cpp
    NAME make_list
    SYMBOLS
        make_list
)

LLVM_BC(
    udf/make_entity.cpp
    NAME make_entity
    SYMBOLS
        make_entity
)

LLVM_BC(
    udf/str_conv.cpp
    NAME str_conv
    SYMBOLS
        numeric_to_string
        parse_int64
        parse_uint64
        parse_double
)

LLVM_BC(
    udf/regex.cpp
    NAME regex
    SYMBOLS
        regex_full_match
        regex_partial_match
        regex_replace_first
        regex_replace_all
        regex_extract
        regex_escape
)

LLVM_BC(
    udf/avg.c
    NAME avg
    SYMBOLS
        avg_init
        avg_update
        avg_merge
        avg_finalize
)

LLVM_BC(
    udf/concat.c
    NAME concat
    SYMBOLS
        concat
)

LLVM_BC(
    udf/replica_set.cpp
    NAME stored_replica_set
    SYMBOLS
        _yt_stored_replica_set_init
        _yt_stored_replica_set_update
        _yt_stored_replica_set_merge
        _yt_stored_replica_set_finalize
)

LLVM_BC(
    udf/last_seen_replica_set.cpp
    NAME last_seen_replica_set
    SYMBOLS
        _yt_last_seen_replica_set_init
        _yt_last_seen_replica_set_update
        _yt_last_seen_replica_set_merge
        _yt_last_seen_replica_set_finalize
)

LLVM_BC(
    udf/first.c
    NAME first
    SYMBOLS
        first_init
        first_update
        first_merge
        first_finalize
)

LLVM_BC(
    udf/is_prefix.c
    NAME is_prefix
    SYMBOLS
        is_prefix
)

LLVM_BC(
    udf/is_substr.c
    NAME is_substr
    SYMBOLS
      is_substr
)

LLVM_BC(
    udf/to_any.cpp
    NAME to_any
    SYMBOLS
      to_any
)

LLVM_BC(
    udf/max.c
    NAME max
    SYMBOLS
        max_init
        max_update
        max_merge
        max_finalize
)

LLVM_BC(
    udf/min.c
    NAME min
    SYMBOLS
        min_init
        min_update
        min_merge
        min_finalize
)

LLVM_BC(
    udf/sleep.c
    NAME sleep
    SYMBOLS
        sleep
        sleep
)

LLVM_BC(
    udf/sum.c
    NAME sum
    SYMBOLS
        sum_init
        sum_update
        sum_merge
        sum_finalize
)

LLVM_BC(
    udf/ypath_get.c
    NAME ypath_get
    SYMBOLS
        try_get_int64
        get_int64
        try_get_uint64
        get_uint64
        try_get_double
        get_double
        try_get_boolean
        get_boolean
        try_get_string
        get_string
        try_get_any
        get_any
)

LLVM_BC(
    udf/lower.cpp
    NAME lower
    SYMBOLS
        lower
)

LLVM_BC(
    udf/length.c
    NAME length
    SYMBOLS
        length
)

LLVM_BC(
    udf/yson_length.cpp
    NAME yson_length
    SYMBOLS
        yson_length
)

LLVM_BC(
    udf/dates.c
    NAME dates
    SYMBOLS
        format_timestamp
        timestamp_floor_hour
        timestamp_floor_day
        timestamp_floor_week
        timestamp_floor_month
        timestamp_floor_year
)

LLVM_BC(
    udf/format_guid.c
    NAME format_guid
    SYMBOLS
        format_guid
)

LLVM_BC(
    udf/list_contains.cpp
    NAME list_contains
    SYMBOLS
        list_contains
)

LLVM_BC(
    udf/any_to_yson_string.cpp
    NAME any_to_yson_string
    SYMBOLS
        any_to_yson_string
)

LLVM_BC(
    udf/has_permissions.cpp
    NAME has_permissions
    SYMBOLS
        has_permissions
)

LLVM_BC(
    udf/xdelta3.c
    NAME xdelta
    SYMBOLS
        xdelta_init
        xdelta_update
        xdelta_merge
        xdelta_finalize
)

LLVM_BC(
    udf/greatest.cpp
    NAME greatest
    SYMBOLS
        greatest
)

END()
