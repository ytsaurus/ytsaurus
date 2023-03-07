#pragma once

#include "public.h"

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfileManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxKeepInterval;
    TDuration DequeuePeriod;
    TDuration SampleRateLimit;

    THashMap<TString, TString> GlobalTags;

    TProfileManagerConfig()
    {
        RegisterParameter("global_tags", GlobalTags)
            .Default();
        RegisterParameter("max_keep_interval", MaxKeepInterval)
            .Default(TDuration::Minutes(5));
        RegisterParameter("deque_period", DequeuePeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("sample_rate_limit", SampleRateLimit)
            .Default(TDuration::MilliSeconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TProfileManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

