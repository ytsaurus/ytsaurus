LIBRARY()

OWNER(sandello)

CFLAGS(-Wno-unused-parameter)

ADDINCL(
    yt/19_1/yt/ytlib/query_client/udf
)

LLVM_BC(
    malloc_udf.c
    NAME malloc_udf
    SYMBOLS
    malloc_udf_init
    malloc_udf_update
    malloc_udf_merge
    malloc_udf_finalize
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
    abs_udf
    sum_udf
    throw_if_negative_udf
    avg_udaf_init
    avg_udaf_update
    avg_udaf_merge
    avg_udaf_finalize
)

END()
