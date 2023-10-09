#pragma once

#include "private.h"

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/timing.h>

#include <atomic>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

/*!
 * Lease state transitions are as follows:
 * NotAcquired -> Valid (with deadline) -> Abandoned -> NotAcquired -> ...
 * Some intermediate states could be skipped.
 *
 * \note Thread affinity: Control (unless noted otherwise)
 */
class TLeaderLease
    : public TRefCounted
{
public:
    //! Returns |true| if the lease range covers the current time instant.
    /*!
     *  Thread affinity: any
     */
    bool IsValid() const;

    //! Switches the lease to unacquired state.
    void Restart();

    //! If the lease was abandoned then does nothing.
    //! Otherwise prolongs the lease range up to #deadline.
    void Extend(NProfiling::TCpuInstant deadline);

    //! If the lease is valid then returns |true| (and abandones it).
    //! Otherwise returns |false|.
    bool TryAbandon();

private:
    static constexpr NProfiling::TCpuInstant NotAcquiredDeadline = 0;
    static constexpr NProfiling::TCpuInstant AbandonedDeadline = 1;
    std::atomic<NProfiling::TCpuInstant> Deadline_ = NotAcquiredDeadline;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TLeaderLease)

////////////////////////////////////////////////////////////////////////////////

/*!
 * Once created, starts executing a sequence of rounds.
 * Each rounds sends out pings to all followers and collects responses.
 * If a quorum of successful responses is received, the round considered successful as a whole.
 * The local peer is not explicitly pinged but is implicitly counted as a success.
 * Non-voting peers are pinged but their responses are ignored for the purpose of quorum counting.
 *
 * Additionally, when #EnableTracking is called then round outcomes start affecting the lease as follows:
 * on success, the lease is extended (see #TLeaderLease::Extend) appropriately;
 * on failure, #LeaseLost signal is raised (and the lease remains intact).
 *
 * \note Thread affinity: Control
 */
class TLeaseTracker
    : public TRefCounted
{
public:
    TLeaseTracker(
        NHydra::TDistributedHydraManagerConfigPtr config,
        TEpochContext* epochContext,
        TLeaderLeasePtr lease,
        std::vector<TCallback<TFuture<void>()>> customLeaseCheckers,
        NLogging::TLogger logger);

    //! Starts sending term.
    void EnableSendingTerm();

    //! Activates the tracking mode.
    void EnableTracking();

    //! Must be called whenever the tracker is no longer needed to prevent certain promises from being stuck in forever-unset state.
    void Finalize();

    //! When invoked at instant |T|, returns a future that becomes set
    //! when the first ping round started after |T| finishes
    //! (either with success or error).
    TFuture<void> GetNextQuorumFuture();

    //! Raised when a lease check (issued in tracking mode) fails.
    DECLARE_SIGNAL(void(const TError&), LeaseLost);

private:
    class TFollowerPinger;

    const NHydra::TDistributedHydraManagerConfigPtr Config_;
    TEpochContext* const EpochContext_;
    const TLeaderLeasePtr Lease_;
    const std::vector<TCallback<TFuture<void>()>> CustomLeaseCheckers_;
    const NLogging::TLogger Logger;

    const NConcurrency::TPeriodicExecutorPtr LeaseCheckExecutor_;

    bool TermSendingEnabled_ = false;
    bool TrackingEnabled_ = false;
    bool Finalized_ = false;
    TPromise<void> NextCheckPromise_ = NewPromise<void>();
    TSingleShotCallbackList<void(const TError&)> LeaseLost_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void OnLeaseCheck();
    TFuture<void> FireLeaseCheck();
};

DEFINE_REFCOUNTED_TYPE(TLeaseTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
