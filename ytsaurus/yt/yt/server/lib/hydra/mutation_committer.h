#pragma once

#include "private.h"
#include "decorated_automaton.h"
#include "hydra_service_proxy.h"

#include <yt/yt/server/lib/hydra_common/mutation_context.h>
#include <yt/yt/server/lib/hydra_common/distributed_hydra_manager.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/invoker_alarm.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/tracing/async_queue_trace.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TCommitterBase
    : public TRefCounted
{
public:
    //! Temporarily suspends writing mutations to the changelog and keeps them in memory.
    void SuspendLogging();

    //! Resumes an earlier suspended mutation logging and sends out all pending mutations.
    void ResumeLogging();

    //! Returns |true| is mutation logging is currently suspended.
    bool IsLoggingSuspended() const;


    //! Raised on mutation logging failure.
    DEFINE_SIGNAL(void(const TError& error), LoggingFailed);

protected:
    TCommitterBase(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler);

    virtual void DoSuspendLogging() = 0;
    virtual void DoResumeLogging() = 0;

    const TDistributedHydraManagerConfigPtr Config_;
    const TDistributedHydraManagerOptions Options_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    TEpochContext* const EpochContext_;

    const NLogging::TLogger Logger;

    const NElection::TCellManagerPtr CellManager_;

    NProfiling::TEventTimer LoggingSuspensionProfilingTimer_;

    bool LoggingSuspended_ = false;
    std::optional<NProfiling::TWallTimer> LoggingSuspensionTimer_;
    NConcurrency::TDelayedExecutorCookie LoggingSuspensionTimeoutCookie_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

private:
    void OnLoggingSuspensionTimeout();
};

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a leader.
/*!
 *  \note Thread affinity: AutomatonThread
 */
class TLeaderCommitter
    : public TCommitterBase
{
public:
    TLeaderCommitter(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler);

    ~TLeaderCommitter();

    //! Initiates a new distributed commit.
    /*!
     *  A distributed commit is completed when the mutation is received, applied,
     *  and flushed to the changelog by a quorum of replicas.
     */
    TFuture<TMutationResponse> Commit(TMutationRequest&& request);

    //! Sends out the current batch of mutations.
    void Flush();

    //! Returns a future that is set when all mutations submitted to #Commit are
    //! flushed by a quorum of changelogs.
    TFuture<void> GetQuorumFlushFuture();

    //! Cleans things up, aborts all pending mutations with a human-readable error.
    void Stop();


    //! Raised each time a checkpoint is needed.
    DEFINE_SIGNAL(void(bool snapshotIsMandatory), CheckpointNeeded);

    //! Raised on commit failure.
    DEFINE_SIGNAL(void(const TError& error), CommitFailed);

private:
    class TBatch;
    using TBatchPtr = TIntrusivePtr<TBatch>;

    TFuture<TMutationResponse> LogLeaderMutation(
        TInstant timestamp,
        std::unique_ptr<TMutationRequest> request);

    void OnBatchCommitted(const TErrorOr<TVersion>& errorOrVersion);
    TIntrusivePtr<TBatch> GetOrCreateBatch(TVersion version);
    void AddToBatch(
        const TDecoratedAutomaton::TPendingMutation& pendingMutation,
        TSharedRef recordData,
        TFuture<void> localFlushFuture);

    void OnAutoSnapshotCheck();

    void FireCommitFailed(const TError& error);

    void DoSuspendLogging() override;
    void DoResumeLogging() override;

    const NConcurrency::TPeriodicExecutorPtr AutoSnapshotCheckExecutor_;
    const NConcurrency::TInvokerAlarmPtr BatchAlarm_;

    struct TPendingMutation
    {
        TPendingMutation(
            TInstant timestamp,
            std::unique_ptr<TMutationRequest> request)
            : Timestamp(timestamp)
            , Request(std::move(request))
        { }

        TInstant Timestamp;
        std::unique_ptr<TMutationRequest> Request;
        TPromise<TMutationResponse> CommitPromise = NewPromise<TMutationResponse>();
    };

    std::vector<TPendingMutation> PendingMutations_;

    TBatchPtr CurrentBatch_;
    TFuture<void> PrevBatchQuorumFlushFuture_ = VoidFuture;

    NProfiling::TEventTimer CommitTimer_;
    NProfiling::TSummary BatchSize_;
};

DEFINE_REFCOUNTED_TYPE(TLeaderCommitter)

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a follower.
/*!
 *  \note Thread affinity: AutomatonThread
 */
class TFollowerCommitter
    : public TCommitterBase
{
public:
    TFollowerCommitter(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler);

    //! Logs a batch of mutations at the follower.
    TFuture<void> AcceptMutations(
        TVersion expectedVersion,
        const std::vector<TSharedRef>& recordsData);

    //! Forwards a given mutation to the leader via RPC.
    TFuture<TMutationResponse> Forward(TMutationRequest&& request);

    //! Cleans things up, aborts all pending mutations with a human-readable error.
    void Stop();

private:
    TFuture<void> DoAcceptMutations(
        TVersion expectedVersion,
        const std::vector<TSharedRef>& recordsData);

    void DoSuspendLogging() override;
    void DoResumeLogging() override;

    struct TPendingMutation
    {
        std::vector<TSharedRef> RecordsData;
        TVersion ExpectedVersion;
        TPromise<void> Promise;
    };

    std::vector<TPendingMutation> PendingMutations_;
};

DEFINE_REFCOUNTED_TYPE(TFollowerCommitter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
