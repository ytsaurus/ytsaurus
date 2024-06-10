#pragma once

#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

extern const TString POOL_SWIFT;
extern const TString POOL_SYSTEM;

////////////////////////////////////////////////////////////////////////////////

class TMappedPoolWeightProvider
    : public NYT::NConcurrency::IPoolWeightProvider
{
public:
    TMappedPoolWeightProvider(THashMap<TString, double> weights);

    double GetWeight(const TString& poolName) override;

private:
    THashMap<TString, double> Weights_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
