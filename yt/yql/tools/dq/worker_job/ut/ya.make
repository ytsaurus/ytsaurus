UNITTEST()

PEERDIR(
    library/cpp/testing/common
    yql/essentials/utils
)

DEPENDS(
    contrib/libs/libiconv/dynamic
    yt/yql/tools/dq/worker_job/ut/test_child
)

SRCS(
    dq_worker_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(test_child)
