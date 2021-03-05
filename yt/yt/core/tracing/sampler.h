#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/spinlock.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TSampler
{
public:
    TSampler();
    explicit TSampler(const TSamplingConfigPtr& config);

    bool IsTraceSampled(const TString& user);

    void ResetPerUserLimits();
    void UpdateConfig(const TSamplingConfigPtr& config);

private:
    struct TUserState
        : public TRefCounted
    {
        std::atomic<uint64_t> SampleCount;
    };

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, Lock_);
    TSamplingConfigPtr Config_;
    THashMap<TString, TIntrusivePtr<TUserState>> UserState_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
