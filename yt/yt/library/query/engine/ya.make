LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    GLOBAL append_function_implementation.cpp
    cg_cache.cpp
    cg_fragment_compiler.cpp
    cg_helpers.cpp
    cg_ir_builder.cpp
    cg_routines.cpp
    GLOBAL column_evaluator.cpp
    GLOBAL expression_evaluator.cpp
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
    GLOBAL query_engine_config.cpp  # TODO(dtorilov): Fix static initializer and remove this GLOBAL.
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
    yt/yt/library/query/engine/time
    yt/yt/client
    library/cpp/yt/memory
    library/cpp/xdelta3/state
    contrib/libs/sparsehash
    contrib/libs/xxhash
)

USE_LLVM_BC18()

LLVM_BC(
    udf/hyperloglog.cpp
    NAME hyperloglog
    SYMBOLS
        hll_7_init
        hll_7_update
        hll_7_merge
        hll_7_finalize
        hll_7_state_init
        hll_7_state_update
        hll_7_state_merge
        hll_7_state_finalize
        hll_7_merge_init
        hll_7_merge_update
        hll_7_merge_merge
        hll_7_merge_finalize
        hll_7_merge_state_init
        hll_7_merge_state_update
        hll_7_merge_state_merge
        hll_7_merge_state_finalize

        hll_8_init
        hll_8_update
        hll_8_merge
        hll_8_finalize
        hll_8_state_init
        hll_8_state_update
        hll_8_state_merge
        hll_8_state_finalize
        hll_8_merge_init
        hll_8_merge_update
        hll_8_merge_merge
        hll_8_merge_finalize
        hll_8_merge_state_init
        hll_8_merge_state_update
        hll_8_merge_state_merge
        hll_8_merge_state_finalize

        hll_9_init
        hll_9_update
        hll_9_merge
        hll_9_finalize
        hll_9_state_init
        hll_9_state_update
        hll_9_state_merge
        hll_9_state_finalize
        hll_9_merge_init
        hll_9_merge_update
        hll_9_merge_merge
        hll_9_merge_finalize
        hll_9_merge_state_init
        hll_9_merge_state_update
        hll_9_merge_state_merge
        hll_9_merge_state_finalize

        hll_10_init
        hll_10_update
        hll_10_merge
        hll_10_finalize
        hll_10_state_init
        hll_10_state_update
        hll_10_state_merge
        hll_10_state_finalize
        hll_10_merge_init
        hll_10_merge_update
        hll_10_merge_merge
        hll_10_merge_finalize
        hll_10_merge_state_init
        hll_10_merge_state_update
        hll_10_merge_state_merge
        hll_10_merge_state_finalize

        hll_11_init
        hll_11_update
        hll_11_merge
        hll_11_finalize
        hll_11_state_init
        hll_11_state_update
        hll_11_state_merge
        hll_11_state_finalize
        hll_11_merge_init
        hll_11_merge_update
        hll_11_merge_merge
        hll_11_merge_finalize
        hll_11_merge_state_init
        hll_11_merge_state_update
        hll_11_merge_state_merge
        hll_11_merge_state_finalize

        hll_12_init
        hll_12_update
        hll_12_merge
        hll_12_finalize
        hll_12_state_init
        hll_12_state_update
        hll_12_state_merge
        hll_12_state_finalize
        hll_12_merge_init
        hll_12_merge_update
        hll_12_merge_merge
        hll_12_merge_finalize
        hll_12_merge_state_init
        hll_12_merge_state_update
        hll_12_merge_state_merge
        hll_12_merge_state_finalize

        hll_13_init
        hll_13_update
        hll_13_merge
        hll_13_finalize
        hll_13_state_init
        hll_13_state_update
        hll_13_state_merge
        hll_13_state_finalize
        hll_13_merge_init
        hll_13_merge_update
        hll_13_merge_merge
        hll_13_merge_finalize
        hll_13_merge_state_init
        hll_13_merge_state_update
        hll_13_merge_state_merge
        hll_13_merge_state_finalize

        hll_14_init
        hll_14_update
        hll_14_merge
        hll_14_finalize
        hll_14_state_init
        hll_14_state_update
        hll_14_state_merge
        hll_14_state_finalize
        hll_14_merge_init
        hll_14_merge_update
        hll_14_merge_merge
        hll_14_merge_finalize
        hll_14_merge_state_init
        hll_14_merge_state_update
        hll_14_merge_state_merge
        hll_14_merge_state_finalize
)

LLVM_BC(
    udf/uniq.cpp
    NAME uniq
    SYMBOLS
        uniq_init
        uniq_update
        uniq_merge
        uniq_finalize
        uniq_state_init
        uniq_state_update
        uniq_state_merge
        uniq_state_finalize
        uniq_merge_init
        uniq_merge_update
        uniq_merge_merge
        uniq_merge_finalize
        uniq_merge_state_init
        uniq_merge_state_update
        uniq_merge_state_merge
        uniq_merge_state_finalize
)


LLVM_BC(
    udf/array_agg.cpp
    NAME array_agg
    SYMBOLS
        array_agg_init
        array_agg_update
        array_agg_merge
        array_agg_finalize
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
    udf/ngrams.cpp
    NAME ngrams
    SYMBOLS
        make_ngrams
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
    udf/is_finite.cpp
    NAME is_finite
    SYMBOLS
      is_finite
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
    udf/to_valid_utf8.cpp
    NAME to_valid_utf8
    SYMBOLS
        to_valid_utf8
        is_valid_utf8
)

LLVM_BC(
    udf/length.c
    NAME length
    SYMBOLS
        length
)

LLVM_BC(
    udf/split.cpp
    NAME split
    SYMBOLS
        split
)

LLVM_BC(
    udf/yson_length.cpp
    NAME yson_length
    SYMBOLS
        yson_length
)

LLVM_BC(
    udf/dates.cpp
    NAME dates
    SYMBOLS
        format_timestamp
        timestamp_floor_hour
        timestamp_floor_day
        timestamp_floor_week
        timestamp_floor_month
        timestamp_floor_quarter
        timestamp_floor_year
        format_timestamp_localtime
        timestamp_floor_hour_localtime
        timestamp_floor_day_localtime
        timestamp_floor_week_localtime
        timestamp_floor_month_localtime
        timestamp_floor_quarter_localtime
        timestamp_floor_year_localtime
        format_timestamp_tz
        timestamp_floor_hour_tz
        timestamp_floor_day_tz
        timestamp_floor_week_tz
        timestamp_floor_month_tz
        timestamp_floor_quarter_tz
        timestamp_floor_year_tz
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
    udf/list_has_intersection.cpp
    NAME list_has_intersection
    SYMBOLS
        list_has_intersection
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

LLVM_BC(
    udf/math_abs.cpp
    NAME math_abs
    SYMBOLS
        math_abs
)

LLVM_BC(
    udf/math.cpp
    NAME math
    SYMBOLS
        math_acos
        math_asin
        math_ceil
        math_cbrt
        math_cos
        math_cot
        math_degrees
        math_even
        math_exp
        math_floor
        math_gamma
        math_is_inf
        math_lgamma
        math_ln
        math_log
        math_log10
        math_log2
        math_radians
        math_sign
        math_signbit
        math_sin
        math_sqrt
        math_tan
        math_trunc
        math_bit_count
        math_atan2
        math_factorial
        math_gcd
        math_lcm
        math_pow
        math_round
        math_xor
)

LLVM_BC(
    udf/dict_sum.c
    NAME dict_sum
    SYMBOLS
        dict_sum_init
        dict_sum_update
        dict_sum_merge
        dict_sum_finalize
)

LLVM_BC(
    udf/yson_string_to_any.cpp
    NAME yson_string_to_any
    SYMBOLS
        yson_string_to_any
)

END()
