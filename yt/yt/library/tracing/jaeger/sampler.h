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

    //! Clear sampled from from incoming user request.
    THashMap<TString, bool> ClearSampledFlag;

    //! Clear debug from from incoming user request.
    THashMap<TString, bool> ClearDebugFlag;

    TSamplerConfig()
    {
        RegisterParameter("global_sample_rate", GlobalSampleRate)
            .Default(0.0);
        RegisterParameter("user_sample_rate", UserSampleRate)
            .Default();
        RegisterParameter("clear_sampled_flag", ClearSampledFlag)
            .Default();
        RegisterParameter("clear_debug_flag", ClearDebugFlag)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSamplerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSampler
{
public:
    TSampler();

    explicit TSampler(
        const TSamplerConfigPtr& config);

    bool IsTraceSampled(const TString& user);

    void UpdateConfig(const TSamplerConfigPtr& config);

private:
    TAtomicObject<TSamplerConfigPtr> Config_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
