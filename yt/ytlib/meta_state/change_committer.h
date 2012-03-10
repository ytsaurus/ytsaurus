#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/election/election_manager.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/signal.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! A common base for TFollowerCommitter and TLeaderCommitter.
class TCommitter
    : public TRefCounted
{
public:
    TCommitter(
        TDecoratedMetaState* metaState,
        IInvoker* epochControlInvoker,
        IInvoker* epochStateInvoker);

    DECLARE_ENUM(EResult,
        (Committed)
        (MaybeCommitted)
        (LateChanges)
        (OutOfOrderChanges)
    );
    typedef TFuture<EResult> TResult;

protected:
    // Corresponds to ControlThread.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    // Corresponds to MetaState->GetInvoker().
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    TDecoratedMetaStatePtr MetaState;
    IInvoker::TPtr EpochControlInvoker;
    IInvoker::TPtr EpochStateInvoker;
    NProfiling::TRateCounter CommitCounter;
    NProfiling::TRateCounter BatchCommitCounter;

};

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a leader.
class TLeaderCommitter
    : public TCommitter
{
public:
    typedef TIntrusivePtr<TLeaderCommitter> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration RpcTimeout;
        TDuration MaxBatchDelay;
        int MaxBatchSize;

        TConfig()
        {
            Register("rpc_timeout", RpcTimeout)
                .GreaterThan(TDuration())
                .Default(TDuration::Seconds(3));
            Register("max_batch_delay", MaxBatchDelay)
				.Default(TDuration::MilliSeconds(10));
            Register("max_batch_size", MaxBatchSize)
				.Default(10000);
        }
    };

    //! Creates an instance.
    TLeaderCommitter(
        TConfig* config,
        TCellManager* cellManager,
        TDecoratedMetaState* metaState,
        TChangeLogCache* changeLogCache,
        TFollowerTracker* followerTracker,
        const TEpoch& epoch,
        IInvoker* epochControlInvoker,
        IInvoker* epochStateInvoker);

    //! Initiates a new distributed commit.
    /*!
     *  \param changeAction An action that will be called in the context of
     *  the state thread and will update the state.
     *  \param changeData A serialized representation of the change that
     *  will be sent down to follower.
     *  \return An asynchronous flag indicating the outcome of the distributed commit.
     *  
     *  The current implementation regards a distributed commit as completed when the update is
     *  received, applied, and flushed to the changelog by a quorum of replicas.
     *  
     *  \note Thread affinity: StateThread
     */
    TResult::TPtr Commit(
        IAction::TPtr changeAction,
        const TSharedRef& changeData);

    //! Force to send all pending changes.
    /*!
     * \note Thread affinity: any
     */
    void Flush();

    //! Raised in the state thread each time a change is applied locally.
    DEFINE_SIGNAL(void(), ChangeApplied);

private:
    class TBatch;
    typedef TMetaStateManagerProxy TProxy;

    void DelayedFlush(TIntrusivePtr<TBatch> batch);
    TIntrusivePtr<TBatch> GetOrCreateBatch(const TMetaVersion& version);
    TResult::TPtr BatchChange(
        const TMetaVersion& version,
        const TSharedRef& changeData,
        TFuture<TVoid>::TPtr changeLogResult);
    void FlushCurrentBatch();

    TConfig::TPtr Config;
    TCellManagerPtr CellManager;
    TChangeLogCachePtr ChangeLogCache;
    TFollowerTrackerPtr FollowerTracker;
    TEpoch Epoch;

    //! Protects #CurrentBatch and #TimeoutCookie.
    TSpinLock BatchSpinLock;
    TIntrusivePtr<TBatch> CurrentBatch;
    TDelayedInvoker::TCookie BatchTimeoutCookie;
};

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a follower.
class TFollowerCommitter
    : public TCommitter
{
public:
    typedef TIntrusivePtr<TFollowerCommitter> TPtr;

    //! Creates an instance.
    TFollowerCommitter(
        TDecoratedMetaState* metaState,
        IInvoker* epochControlInvoker,
        IInvoker* epochStateInvoker);

    //! Commits a bunch of changes at a follower.
    /*!
     *  \param expectedVersion A version that the state is currently expected to have.
     *  \param changes A bunch of serialized changes to apply.
     *  \return An asynchronous flag indicating the outcome of the local commit.
     *  
     *  The current implementation regards a local commit as completed when the update is
     *  flushed to the local changelog.
     *  
     *  \note Thread affinity: ControlThread
     */
    TResult::TPtr Commit(
        const TMetaVersion& expectedVersion,
        const std::vector<TSharedRef>& changes);

private:
    TResult::TPtr DoCommit(
        const TMetaVersion& expectedVersion,
        const std::vector<TSharedRef>& changes);

};

////////////////////////////////////////////////////////////////////////////////


} // namespace NMetaState
} // namespace NYT
