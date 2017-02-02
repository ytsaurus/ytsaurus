PROGRAM()

OWNER(sandello)

NO_WERROR()

ADDINCLSELF()

PEERDIR(
    yt/19_1/yt/core
    yt/19_1/yt/ytlib
    library/resource
)

SET(YT_ROOT ${ARCADIA_ROOT}/yt/19_1/yt)

LLVM_BC(
    hyperloglog.cpp
    ${YT_ROOT}/ytlib/table_client/unversioned_value.cpp
    ${YT_ROOT}/contrib/farmhash/farmhash.cc

    NAME hyperloglog
    SYMBOLS
    cardinality_init
    cardinality_update
    cardinality_merge
    cardinality_finalize
)

LLVM_BC(
    farm_hash.cpp
    ${YT_ROOT}/ytlib/table_client/unversioned_value.cpp
    ${YT_ROOT}/contrib/farmhash/farmhash.cc

    NAME farm_hash
    SYMBOLS
    farm_hash
    farm_hash_init
    farm_hash_update
    farm_hash_merge
    farm_hash_finalize
)

LLVM_BC(
    regex.cpp
    ${YT_ROOT}/ytlib/query_client/function_context.cpp

    NAME regex
    SYMBOLS
    regex
    regex_init
    regex_update
    regex_merge
    regex_finalize
    regex_full_match
    regex_partial_match
    regex_replace_first
    regex_replace_all
    regex_extract
    regex_escape
)

LLVM_BC(
    is_substr.c

    NAME is_substr
    SYMBOLS
    is_substr
    is_substr_init
    is_substr_update
    is_substr_merge
    is_substr_finalize
)

LLVM_BC(
    is_prefix.c

    NAME is_prefix
    SYMBOLS
    is_prefix
    is_prefix_init
    is_prefix_update
    is_prefix_merge
    is_prefix_finalize
)

LLVM_BC(
    concat.c

    NAME concat
    SYMBOLS
    concat
    concat_init
    concat_update
    concat_merge
    concat_finalize
)

LLVM_BC(
    sleep.c

    NAME sleep
    SYMBOLS
    sleep
    sleep_init
    sleep_update
    sleep_merge
    sleep_finalize
)

LLVM_BC(
    min.c

    NAME min
    SYMBOLS
    min
    min_init
    min_update
    min_merge
    min_finalize
)

LLVM_BC(
    max.c

    NAME max
    SYMBOLS
    max
    max_init
    max_update
    max_merge
    max_finalize
)

LLVM_BC(
    sum.c

    NAME sum
    SYMBOLS
    sum
    sum_init
    sum_update
    sum_merge
    sum_finalize
)

LLVM_BC(
    avg.c

    NAME avg
    SYMBOLS
    avg
    avg_init
    avg_update
    avg_merge
    avg_finalize
)

LLVM_BC(
    ypath_get.c

    NAME ypath_get
    SYMBOLS
    ypath_get
    ypath_get_init
    ypath_get_update
    ypath_get_merge
    ypath_get_finalize
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
)

LLVM_BC(
    lower.cpp
    ${ARCADIA_ROOT}/util/charset/utf8.cpp

    NAME lower
    SYMBOLS
    lower
    lower_init
    lower_update
    lower_merge
    lower_finalize
)

LLVM_BC(
    dates.c

    NAME dates
    SYMBOLS
    dates
    dates_init
    dates_update
    dates_merge
    dates_finalize

    format_timestamp
    timestamp_floor_hour
    timestamp_floor_day
    timestamp_floor_week
    timestamp_floor_month
    timestamp_floor_year
)

SRCS(
    main.cpp
)

END()
