#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration CleanupPeriod;
    i64 TraceBufferSize;

    TTraceManagerConfig()
    {
        RegisterParameter("cleanup_period", CleanupPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("trace_buffer_size", TraceBufferSize)
            .Default(16_MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TTraceManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSamplingConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Request is sampled with probability P.
    double GlobalSampleRate = 0.0;
    
    //! Additionaly, request is sampled with probability P(user).
    THashMap<TString, double> UserSampleRate;
    
    //! Additionaly, first K requests for each user are sampled after reset.
    int MinUserTraceCount = 0;

    TSamplingConfig()
    {
        RegisterParameter("global_sample_rate", GlobalSampleRate)
            .Default(0.0);
        RegisterParameter("user_sample_rate", UserSampleRate)
            .Default();
        RegisterParameter("min_user_trace_count", MinUserTraceCount)
            .Default(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSamplingConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

