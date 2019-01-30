#pragma once

#include "private.h"

#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/ytlib/election/public.h>

#include <yt/core/actions/callback.h>
#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/timing.h>

#include <atomic>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TLeaderLease
    : public TIntrinsicRefCounted
{
public:
    bool IsValid() const;
    void SetDeadline(NProfiling::TCpuInstant deadline);
    void Invalidate();

private:
    std::atomic<NProfiling::TCpuInstant> Deadline_ = {0};

};

DEFINE_REFCOUNTED_TYPE(TLeaderLease)

////////////////////////////////////////////////////////////////////////////////

class TLeaseTracker
    : public TRefCounted
{
public:
    TLeaseTracker(
        TDistributedHydraManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext,
        TLeaderLeasePtr lease,
        const std::vector<TCallback<TFuture<void>()>>& customLeaseCheckers);

    void Start();

    TFuture<void> GetLeaseAcquired();
    TFuture<void> GetLeaseLost();

private:
    class TFollowerPinger;

    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    TEpochContext* const EpochContext_;
    TLeaderLeasePtr Lease_;
    const std::vector<TCallback<TFuture<void>()>> CustomLeaseCheckers_;

    NConcurrency::TPeriodicExecutorPtr LeaseCheckExecutor_;

    TPromise<void> LeaseAcquired_ = NewPromise<void>();
    TPromise<void> LeaseLost_ = NewPromise<void>();

    NLogging::TLogger Logger;


    void OnLeaseCheck();
    TFuture<void> FireLeaseCheck();

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TLeaseTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
