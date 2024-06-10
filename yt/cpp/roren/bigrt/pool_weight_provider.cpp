#include "pool_weight_provider.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

const TString POOL_SWIFT = "swift";
const TString POOL_SYSTEM = "system";

////////////////////////////////////////////////////////////////////////////////

TMappedPoolWeightProvider::TMappedPoolWeightProvider(THashMap<TString, double> weights)
    : Weights_(std::move(weights))
{
    Weights_.try_emplace(POOL_SWIFT, 100.0);
    Weights_.try_emplace(POOL_SYSTEM, 1.0);
}

double TMappedPoolWeightProvider::GetWeight(const TString& poolName)
{
    return Weights_.Value(poolName, 1.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
