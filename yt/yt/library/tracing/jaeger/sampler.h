#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TSamplerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Request is sampled with probability P.
    double GlobalSampleRate;
    
    //! Additionaly, request is sampled with probability P(user).
    THashMap<TString, double> UserSampleRate;

    //! Additionally, sample first N requests for each user in the window.
    ui64 MinPerUserSamples;
    TDuration MinPerUserSamplesPeriod;

    //! Clear sampled from from incoming user request.
    THashMap<TString, bool> ClearSampledFlag;

    TSamplerConfig()
    {
        RegisterParameter("global_sample_rate", GlobalSampleRate)
            .Default(0.0);
        RegisterParameter("user_sample_rate", UserSampleRate)
            .Default();
        RegisterParameter("clear_sampled_flag", ClearSampledFlag)
            .Default();

        RegisterParameter("min_per_user_samples", MinPerUserSamples)
            .Default(0);
        RegisterParameter("min_per_user_samples_period", MinPerUserSamplesPeriod)
            .Default(TDuration::Minutes(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TSamplerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSampler
    : public TRefCounted
{
public:
    TSampler();

    explicit TSampler(
        const TSamplerConfigPtr& config);

    void SampleTraceContext(const TString& user, const TTraceContextPtr& traceContext);

    void UpdateConfig(const TSamplerConfigPtr& config);

private:
    TAtomicObject<TSamplerConfigPtr> Config_;

    struct TUserState final
    {
        std::atomic<ui64> Sampled = {0};
        std::atomic<NProfiling::TCpuInstant> LastReset = {0};

        bool TrySampleByMinCount(ui64 minCount, NProfiling::TCpuDuration period);

        NProfiling::TCounter TracesSampledByUser;
        NProfiling::TCounter TracesSampledByProbability;
    };

    NConcurrency::TSyncMap<TString, TIntrusivePtr<TUserState>> Users_;
    NProfiling::TCounter TracesSampled_;
};

DEFINE_REFCOUNTED_TYPE(TSampler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
