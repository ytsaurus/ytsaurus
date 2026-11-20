#include "causal_state_registry.h"

#include "config.h"

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/error.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TCausalStateRegistry::TCausalStateRegistry(
    TCausalStateRegistryConfigPtr config,
    IInvokerPtr automatonInvoker,
    const NProfiling::TProfiler& profiler)
    : Config_(std::move(config))
    , AutomatonInvoker_(std::move(automatonInvoker))
    , EvictionExecutor_(New<TPeriodicExecutor>(
        AutomatonInvoker_,
        BIND(&TCausalStateRegistry::OnEvict, MakeWeak(this)),
        Config_->EvictionPeriod))
{
    EvictionExecutor_->Start();

    profiler.AddFuncGauge("/causal_state_registry/registry_size", MakeStrong(this), [this] {
        return CausalStateMapSize_.load();
    });
}

void TCausalStateRegistry::Clear()
{
    CausalStateMap_.clear();
    CausalStateMapSize_ = 0;
}

TCausalState TCausalStateRegistry::GetStateOrThrow(std::optional<TCausalTime> causalTime) const
{
    if (CausalStateMap_.empty()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CausalTimeNotFound,
            "Requested causal state is outside retained completed history")
            .With("requested_causal_time", causalTime);
    }

    auto latestIt = std::prev(CausalStateMap_.end());
    if (!causalTime) {
        return latestIt->second.State;
    }
    if (*causalTime > latestIt->first) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CausalTimeNotReached,
            "Requested causal time has not been reached")
            .With("requested_causal_time", causalTime)
            .With("latest_causal_time", latestIt->first);
    }

    auto it = CausalStateMap_.upper_bound(*causalTime);
    if (it == CausalStateMap_.begin()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CausalTimeNotFound,
            "Requested causal state is outside retained completed history")
            .With("requested_causal_time", causalTime);
    }
    return std::prev(it)->second.State;
}

void TCausalStateRegistry::RegisterState(const TCausalState& state)
{
    EmplaceOrCrash(CausalStateMap_, state.CausalTime, TCausalStateInfo{
        .State = state,
        .RegisteredAt = TInstant::Now(),
    });
    ++CausalStateMapSize_;
}

void TCausalStateRegistry::OnEvict()
{
    auto threshold = TInstant::Now() - Config_->ExpirationTimeout;

    // Retain the latest state for idle cells.
    while (CausalStateMap_.size() > 1 && CausalStateMap_.begin()->second.RegisteredAt <= threshold) {
        CausalStateMap_.erase(CausalStateMap_.begin());
    }
    CausalStateMapSize_ = std::ssize(CausalStateMap_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
