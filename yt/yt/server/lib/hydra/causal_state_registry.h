#pragma once

#include "public.h"
#include "hydra_manager.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TCausalStateRegistry
    : public TRefCounted
{
public:
    TCausalStateRegistry(
        TCausalStateRegistryConfigPtr config,
        IInvokerPtr automatonInvoker,
        const NProfiling::TProfiler& profiler);

    void Clear();
    void RegisterState(const TCausalState& state);

    //! Returns the latest state, or the retained state at or before #causalTime.
    //! Throws CausalTimeNotReached for a future cutoff and CausalTimeNotFound
    //! when no history is retained at the requested cutoff.
    TCausalState GetStateOrThrow(std::optional<TCausalTime> causalTime) const;

private:
    const TCausalStateRegistryConfigPtr Config_;
    const IInvokerPtr AutomatonInvoker_;

    struct TCausalStateInfo
    {
        TCausalState State;
        TInstant RegisteredAt;
    };
    std::map<TCausalTime, TCausalStateInfo> CausalStateMap_;
    std::atomic<int> CausalStateMapSize_ = 0;

    const NConcurrency::TPeriodicExecutorPtr EvictionExecutor_;

    void OnEvict();
};

DEFINE_REFCOUNTED_TYPE(TCausalStateRegistry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
