IF (OS_LINUX AND ARCH_X86_64 AND NOT SANITIZER_TYPE)
    GTEST(unittester-library-query-portable-engine-allocation)

    INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

    ALLOCATOR(LF_DBG)

    SRCS(
        query_evaluator_ut.cpp
    )

    INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

    PEERDIR(
        library/cpp/lfalloc/dbg_info
        library/cpp/malloc/api
        yt/yt/client
        yt/yt/core
        yt/yt/core/test_framework
        yt/yt/library/query/portable_engine
    )

    SIZE(SMALL)

    END()
ENDIF()
