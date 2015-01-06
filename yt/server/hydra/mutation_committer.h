#pragma once

#include "private.h"
#include "mutation_context.h"

#include <core/concurrency/thread_affinity.h>

#include <core/actions/signal.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <ytlib/election/public.h>

#include <ytlib/hydra/hydra_service_proxy.h>
#include <ytlib/hydra/version.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TCommitterBase
    : public TRefCounted
{
protected:
    TCommitterBase(
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext,
        const NProfiling::TProfiler& profiler);

    ~TCommitterBase();


    NElection::TCellManagerPtr CellManager_;
    TDecoratedAutomatonPtr DecoratedAutomaton_;
    TEpochContext* EpochContext_;

    NProfiling::TRateCounter CommitCounter_;
    NProfiling::TRateCounter BatchFlushCounter_;

    NLog::TLogger Logger;
    NProfiling::TProfiler Profiler;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

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
    //! Creates an instance.
    TLeaderCommitter(
        TDistributedHydraManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        TEpochContext* epochContext,
        const NProfiling::TProfiler& profiler);

    ~TLeaderCommitter();

    //! Initiates a new distributed commit.
    /*!
     *  A distributed commit is completed when the mutation is received, applied,
     *  and flushed to the changelog by a quorum of replicas.
     */
    TFuture<TMutationResponse> Commit(const TMutationRequest& request);

    //! Sends out the current batch of mutations.
    void Flush();

    //! Returns a future that is set when all mutations submitted to #Commit are
    //! flushed by a quorum of changelogs.
    TFuture<void> GetQuorumFlushResult();

    //! Temporarily suspends writing mutations to the changelog and keeps them in memory.
    void SuspendLogging();

    //! Resumes an earlier suspended mutation logging and sends out all pending mutations.
    void ResumeLogging();


    //! Raised each time a checkpoint is needed.
    DEFINE_SIGNAL(void(), CheckpointNeeded);

    //! Raised on commit failure.
    DEFINE_SIGNAL(void(const TError& error), CommitFailed);


private:
    class TBatch;
    typedef TIntrusivePtr<TBatch> TBatchPtr;

    void OnBatchTimeout(TBatchPtr batch);
    void OnBatchCommitted(TBatchPtr batch, const TError& error);
    TIntrusivePtr<TBatch> GetOrCreateBatch(TVersion version);
    void AddToBatch(
        TVersion version,
        const TSharedRef& recordData,
        TFuture<void> localFlushResult);
    void FlushCurrentBatch();

    void OnAutoCheckpointCheck();

    void FireCommitFailed(const TError& error);


    TDistributedHydraManagerConfigPtr Config_;
    IChangelogStorePtr ChangelogStore_;

    struct TPendingMutation
    {
        TMutationRequest Request;
        TPromise<TMutationResponse> Promise;
    };
    
    bool LoggingSuspended_ = false;
    std::vector<TPendingMutation> PendingMutations_;

    TSpinLock BatchSpinLock_;
    TBatchPtr CurrentBatch_;
    TFuture<void> PrevBatchQuorumFlushResult_ = VoidFuture;
    NConcurrency::TDelayedExecutorCookie BatchTimeoutCookie_;

    NConcurrency::TPeriodicExecutorPtr AutoCheckpointCheckExecutor_;

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
    //! Creates an instance.
    TFollowerCommitter(
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext,
        const NProfiling::TProfiler& profiler);

    ~TFollowerCommitter();

    //! Logs a batch of mutations at the follower.
    TFuture<void> LogMutations(
        TVersion expectedVersion,
        const std::vector<TSharedRef>& recordsData);

    //! Returns |true| is mutation logging is currently suspended.
    bool IsLoggingSuspended() const;

    //! Temporarily suspends writing mutations to the changelog and keeps them in memory.
    void SuspendLogging();

    //! Resumes an earlier suspended mutation logging and logs out all pending mutations.
    void ResumeLogging();

private:
    TFuture<void> DoLogMutations(
        TVersion expectedVersion,
        const std::vector<TSharedRef>& recordsData);

    struct TPendingMutation
    {
        std::vector<TSharedRef> RecordsData;
        TVersion ExpectedVersion;
        TPromise<void> Promise;
    };

    bool LoggingSuspended_ = false;
    std::vector<TPendingMutation> PendingMutations_;

};

DEFINE_REFCOUNTED_TYPE(TFollowerCommitter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
