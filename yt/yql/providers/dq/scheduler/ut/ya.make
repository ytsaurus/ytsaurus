UNITTEST_FOR(yt/yql/providers/dq/scheduler)

SIZE(SMALL)

SRCS(
    dq_scheduler_ut.cpp
)

PEERDIR(
    yql/essentials/providers/common/metrics
    yql/essentials/public/udf/service/stub
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
