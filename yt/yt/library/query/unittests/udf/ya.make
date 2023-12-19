LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ADDINCL(
    yt
)

LLVM_BC(
    malloc_udf.c
    NAME malloc_udf
    SYMBOLS
        malloc_udf
)

LLVM_BC(
    test_udfs.c
    NAME test_udfs
    SYMBOLS
        seventyfive
        strtol_udf
        exp_udf
        tolower_udf
        is_null_udf
        string_equals_42_udf
        abs_udf
        sum_udf
        throw_if_negative_udf
        avg_udaf_init
        avg_udaf_update
        avg_udaf_merge
        avg_udaf_finalize
)

LLVM_BC(
    test_udfs_fc.cpp
    NAME test_udfs_fc
    SYMBOLS
        udf_with_function_context
)

LLVM_BC(
    xor_aggregate.c
    NAME xor_aggregate
    SYMBOLS
        xor_aggregate_init
        xor_aggregate_update
        xor_aggregate_merge
        xor_aggregate_finalize
)

LLVM_BC(
    concat_all.c
    NAME concat_all
    SYMBOLS
        concat_all_init
        concat_all_update
        concat_all_merge
        concat_all_finalize
)

END()
