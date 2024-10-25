PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

CFLAGS(
    -mavx2
    -mavx
)

ADDINCL(
    contrib/libs/sparsehash/src
)

# This flag is required for linking code of bc functions.
# Note that -rdynamic enabled by default in arcadia build, but we do not want to rely on it.
LDFLAGS(-rdynamic)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/library/query/distributed
    yt/yt/library/query/engine
    yt/yt/library/query/engine_api
    yt/yt/library/query/unittests/helpers
    yt/yt/library/query/unittests/udf
    contrib/libs/sparsehash
    yt/yt/core
    yt/yt/library/web_assembly/api
    yt/yt/library/query/misc
    yt/yt/library/query/proto
    yt/yt/library/query/base
    yt/yt/client
    library/cpp/yt/memory
    contrib/libs/sparsehash
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

FORK_SUBTESTS(MODULO)

IF (SANITIZER_TYPE)
    SPLIT_FACTOR(10)
ELSE()
    SPLIT_FACTOR(3)
ENDIF()

SIZE(MEDIUM)

END()
