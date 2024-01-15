#pragma once

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EThrottlerType,
    (Throughput)
    (Iops)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TThrottlerConfig)
DECLARE_REFCOUNTED_STRUCT(IThrottler)

////////////////////////////////////////////////////////////////////////////////

class TThrottlerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration Period;
    i64 Limit;

    REGISTER_YSON_STRUCT(TThrottlerConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("period", &TThis::Period)
            .Default(TDuration::Seconds(1));
        registrar.Parameter("limit", &TThis::Limit)
            .Default(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TThrottlerConfig)

using TCombinedThrottlerConfig = THashMap<TString, TThrottlerConfigPtr>;

////////////////////////////////////////////////////////////////////////////////

struct IThrottler
    : public TRefCounted
{
    virtual bool IsAvailable(i64 value) = 0;
    virtual void Acquire(i64 value) = 0;
};

DEFINE_REFCOUNTED_TYPE(IThrottler)

IThrottlerPtr CreateCombinedThrottler(const TCombinedThrottlerConfig& throttlerConfigs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
