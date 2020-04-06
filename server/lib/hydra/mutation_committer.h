#pragma once

#include "private.h"
#include "mutation_context.h"
#include "decorated_automaton.h"
#include "distributed_hydra_manager.h"

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/client/hydra/version.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

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
    const NProfiling::TProfiler Profiler;

    const NElection::TCellManagerPtr CellManager_;

    NProfiling::TSimpleGauge LoggingSuspensionTimeGauge_{"/mutation_logging_suspension_time"};

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
    TFuture<void> GetQuorumFlushResult();

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
        TMutationRequest&& request,
        NTracing::TTraceContextPtr traceContext);

    void OnBatchTimeout(const TBatchPtr& batch);
    void OnBatchCommitted(const TBatchPtr& batch, const TError& error);
    TIntrusivePtr<TBatch> GetOrCreateBatch(TVersion version);
    void AddToBatch(
        const TDecoratedAutomaton::TPendingMutation& pendingMutation,
        TSharedRef recordData,
        TFuture<void> localFlushResult);
    void FlushCurrentBatch();

    void OnAutoSnapshotCheck();

    void FireCommitFailed(const TError& error);

    virtual void DoSuspendLogging() override;
    virtual void DoResumeLogging() override;

    const NConcurrency::TPeriodicExecutorPtr AutoSnapshotCheckExecutor_;

    struct TPendingMutation
    {
        TPendingMutation(
            TInstant timestamp,
            TMutationRequest&& request,
            NTracing::TTraceContextPtr traceContext)
            : Timestamp(timestamp)
            , Request(request)
            , TraceContext(std::move(traceContext))
        { }

        TInstant Timestamp;
        TMutationRequest Request;
        NTracing::TTraceContextPtr TraceContext;
        TPromise<TMutationResponse> CommitPromise = NewPromise<TMutationResponse>();
    };

    std::vector<TPendingMutation> PendingMutations_;

    TSpinLock BatchSpinLock_;
    TBatchPtr CurrentBatch_;
    TFuture<void> PrevBatchQuorumFlushResult_ = VoidFuture;
    NConcurrency::TDelayedExecutorCookie BatchTimeoutCookie_;

    NProfiling::TSimpleGauge CommitTimeGauge_{"/mutation_commit_time"};
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

    virtual void DoSuspendLogging() override;
    virtual void DoResumeLogging() override;

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
