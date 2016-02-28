#pragma once

#include "private.h"
#include "mutation_context.h"
#include "distributed_hydra_manager.h"

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>
#include <yt/ytlib/hydra/version.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TCommitterBase
    : public TRefCounted
{
protected:
    TCommitterBase(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext);

    ~TCommitterBase();


    const TDistributedHydraManagerConfigPtr Config_;
    const TDistributedHydraManagerOptions Options_;
    const NElection::TCellManagerPtr CellManager_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    TEpochContext* EpochContext_;

    NProfiling::TSimpleCounter CommitCounter_;
    NProfiling::TSimpleCounter FlushCounter_;

    NLogging::TLogger Logger;


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
    TLeaderCommitter(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        TEpochContext* epochContext);

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
        const TMutationRequest& request,
        const TSharedRef& recordData,
        TFuture<void> localFlushResult);
    void FlushCurrentBatch();

    void OnAutoCheckpointCheck();

    void FireCommitFailed(const TError& error);


    const IChangelogStorePtr ChangelogStore_;

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
    TFollowerCommitter(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext);

    ~TFollowerCommitter();

    //! Logs a batch of mutations at the follower.
    TFuture<void> AcceptMutations(
        TVersion expectedVersion,
        const std::vector<TSharedRef>& recordsData);

    //! Returns |true| is mutation logging is currently suspended.
    bool IsLoggingSuspended() const;

    //! Temporarily suspends writing mutations to the changelog and keeps them in memory.
    void SuspendLogging();

    //! Resumes an earlier suspended mutation logging and logs out all pending mutations.
    void ResumeLogging();

    //! Fowards a given mutation to the leader via RPC.
    TFuture<TMutationResponse> Forward(const TMutationRequest& request);

private:
    TFuture<void> DoAcceptMutations(
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
