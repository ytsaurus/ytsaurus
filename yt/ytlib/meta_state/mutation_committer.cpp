#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "mutation_committer.h"
#include "meta_version.h"
#include "decorated_meta_state.h"
#include "change_log_cache.h"
#include "quorum_tracker.h"
#include "serialize.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/foreach.h>

#include <ytlib/actions/parallel_awaiter.h>

#include <ytlib/ytree/ypath_client.h>

#include <ytlib/logging/tagged_logger.h>

#include <util/random/random.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static NProfiling::TProfiler& Profiler = MetaStateProfiler;

////////////////////////////////////////////////////////////////////////////////

TCommitter::TCommitter(
    TDecoratedMetaStatePtr decoratedState,
    IInvokerPtr controlInvoker,
    IInvokerPtr epochStateInvoker)
    : DecoratedState(decoratedState)
    , ControlInvoker(controlInvoker)
    , EpochStateInvoker(epochStateInvoker)
    , CommitCounter("/commit_rate")
    , BatchCommitCounter("/commit_batch_rate")
    , CommitTimeCounter("/commit_time")
{
    YCHECK(decoratedState);
    YCHECK(controlInvoker);
    YCHECK(epochStateInvoker);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(epochStateInvoker, StateThread);
}

TCommitter::~TCommitter()
{ }

////////////////////////////////////////////////////////////////////////////////

class TLeaderCommitter::TBatch
    : public TRefCounted
{
public:
    TBatch(
        IInvokerPtr controlInvoker,
        TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        TLeaderCommitterConfigPtr config,
        const TMetaVersion& startVersion,
        const TEpochId& epochId)
        : ControlInvoker(controlInvoker)
        , CellManager(cellManager)
        , DecoratedState(decoratedState)
        , Config(config)
        , StartVersion(startVersion)
        , EpochId(epochId)
        , Promise(NewPromise<TError>())
        // The local commit is also counted.
        , CommitSuccessCount(0)
        , Logger(MetaStateLogger)
    {
        YCHECK(controlInvoker);
        YCHECK(cellManager);
        YCHECK(decoratedState);
        YCHECK(config);

        Logger.AddTag(Sprintf("StartVersion: %s", ~StartVersion.ToString()));
    }

    TAsyncError AddMutation(const TSharedRef& recordData)
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        TMetaVersion currentVersion(
            StartVersion.SegmentId,
            StartVersion.RecordCount + BatchedRecordsData.size());
        BatchedRecordsData.push_back(recordData);

        LOG_DEBUG("Mutation is added to batch at version %s", ~currentVersion.ToString());

        return Promise;
    }

    void SetLastChangeLogResult(TFuture<void> result)
    {
        LogResult = result;
    }

    void FlushMutations(bool rotateChangeLog)
    {
        Logger.AddTag(Sprintf("MutationCount: %d", static_cast<int>(BatchedRecordsData.size())));
        ControlInvoker->Invoke(BIND(
            &TBatch::DoFlushMutations,
            MakeStrong(this),
            rotateChangeLog));
    }

    int GetMutationCount() const
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        return static_cast<int>(BatchedRecordsData.size());
    }

private:
    void DoFlushMutations(bool rotateChangeLog)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
            
        if (!BatchedRecordsData.empty()) {
            Profiler.Enqueue("/commit_batch_size", BatchedRecordsData.size());

            YASSERT(!LogResult.IsNull());

            Awaiter = New<TParallelAwaiter>(
                ControlInvoker,
                &Profiler,
                "/commit_batch_time");

            Awaiter->Await(
                LogResult,
                EscapeYPathToken(CellManager->GetSelfAddress()),
                BIND(&TBatch::OnLocalFlush, MakeStrong(this)));

            LOG_DEBUG("Sending batched mutations to followers");
            for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
                if (id == CellManager->GetSelfId()) continue;

                LOG_DEBUG("Sending mutations to follower %d", id);

                TProxy proxy(CellManager->GetMasterChannel(id));
                proxy.SetDefaultTimeout(Config->RpcTimeout);

                auto request = proxy.ApplyMutations();
                request->set_segment_id(StartVersion.SegmentId);
                request->set_record_count(StartVersion.RecordCount);
                *request->mutable_epoch_id() = EpochId.ToProto();
                FOREACH (const auto& mutation, BatchedRecordsData) {
                    request->Attachments().push_back(mutation);
                }
                Awaiter->Await(
                    request->Invoke(),
                    EscapeYPathToken(CellManager->GetPeerAddress(id)),
                    BIND(&TBatch::OnRemoteCommit, MakeStrong(this), id));
            }
            LOG_DEBUG("Batched mutations sent");

            Awaiter->Complete(BIND(&TBatch::OnCompleted, MakeStrong(this)));

        }
        
        // This is the version the next batch will have.
        DecoratedState->SetPingVersion(
            rotateChangeLog
            ? TMetaVersion(StartVersion.SegmentId + 1, 0)
            : TMetaVersion(StartVersion.SegmentId, StartVersion.RecordCount + BatchedRecordsData.size()));
    }

    bool CheckCommitQuorum()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (CommitSuccessCount < CellManager->GetQuorum())
            return false;

        Promise.Set(TError());
        Awaiter->Cancel();
        
        LOG_DEBUG("Mutations are committed by quorum");

        return true;
    }

    void OnRemoteCommit(TPeerId peerId, TProxy::TRspApplyMutationsPtr response)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!response->IsOK()) {
            LOG_WARNING(*response, "Error committing mutations by follower %d",
                peerId);
            return;
        }

        if (response->committed()) {
            LOG_DEBUG("Mutations are committed by follower %d", peerId);
            ++CommitSuccessCount;
            CheckCommitQuorum();
        } else {
            LOG_DEBUG("Mutations are acknowledged by follower %d", peerId);
        }
    }
    
    void OnLocalFlush()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Mutations are flushed locally");
        ++CommitSuccessCount;
        CheckCommitQuorum();
    }

    void OnCompleted()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (CheckCommitQuorum())
            return;

        Promise.Set(TError(
            ECommitCode::MaybeCommitted,
            "Mutations are uncertain: %d out of %d commits were successful",
            CommitSuccessCount,
            CellManager->GetQuorum()));
    }


    IInvokerPtr ControlInvoker;
    TCellManagerPtr CellManager;
    TDecoratedMetaStatePtr DecoratedState;
    TLeaderCommitterConfigPtr Config;
    TMetaVersion StartVersion;
    TEpochId EpochId;

    TPromise<TError> Promise;
    int CommitSuccessCount;
    NLog::TTaggedLogger Logger;

    TParallelAwaiterPtr Awaiter;
    TFuture<void> LogResult;
    std::vector<TSharedRef> BatchedRecordsData;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TLeaderCommitterConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TChangeLogCachePtr changeLogCache,
    TQuorumTrackerPtr followerTracker,
    const TEpochId& epochId,
    IInvokerPtr controlInvoker,
    IInvokerPtr epochStateInvoker)
    : TCommitter(decoratedState, controlInvoker, epochStateInvoker)
    , Config(config)
    , CellManager(cellManager)
    , ChangeLogCache(changeLogCache)
    , FollowerTracker(followerTracker)
    , EpochId(epochId)
{
    YCHECK(config);
    YCHECK(cellManager);
    YCHECK(changeLogCache);
    YCHECK(followerTracker);
}

TLeaderCommitter::~TLeaderCommitter()
{ }

void TLeaderCommitter::Flush(bool rotateChangeLog)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TGuard<TSpinLock> guard(BatchSpinLock);
    // If no current batch exists but the changelog is about to be rotated
    // we have to create a dummy batch and flush it to update ping version.
    if (rotateChangeLog && !CurrentBatch) {
        auto version = DecoratedState->GetVersion();
        GetOrCreateBatch(version);
    }
    if (CurrentBatch) {
        FlushCurrentBatch(rotateChangeLog);
    }
}

TFuture< TValueOrError<TMutationResponse> > TLeaderCommitter::Commit(const TMutationRequest& request)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (request.Id != NullMutationId) {
        TSharedRef responseData;
        if (DecoratedState->FindKeptResponse(request.Id, &responseData)) {
            LOG_DEBUG("Kept response returned (MutationId: %s)", ~request.Id.ToString());
            TMutationResponse response;
            response.Applied = false;
            response.Data = responseData;
            return MakeFuture(TValueOrError<TMutationResponse>(response));
        }
    }

    auto timestamp = TInstant::Now();
    auto randomSeed = RandomNumber<ui64>();

    NProto::TMutationHeader header;
    header.set_mutation_type(request.Type);
    if (request.Id != NullMutationId) {
        *header.mutable_mutation_id() = request.Id.ToProto();
    }
    header.set_timestamp(timestamp.GetValue());
    header.set_random_seed(randomSeed);
    auto recordData = SerializeMutationRecord(header, request.Data);

    PROFILE_AGGREGATED_TIMING (CommitTimeCounter) {
        auto version = DecoratedState->GetVersion();
        LOG_DEBUG("Committing mutation at version %s (MutationId: %s)",
            ~version.ToString(),
            ~request.Id.ToString());

        auto logResult = DecoratedState->LogMutation(version, recordData);
        auto batchResult = AddMutationToBatch(version, recordData, logResult);

        TMutationContext context(
            DecoratedState->GetVersion(),
            request,
            timestamp,
            randomSeed);
        DecoratedState->ApplyMutation(&context);

        MutationApplied_.Fire();
        Profiler.Increment(CommitCounter);

        auto responseData = context.GetResponseData();
        return batchResult.Apply(BIND([=] (TError error) -> TValueOrError<TMutationResponse> {
            if (error.IsOK()) {
                TMutationResponse response;
                response.Applied = true;
                response.Data = responseData;
                return response;
            } else {
                return error;
            }
        }));
    }
}

TAsyncError TLeaderCommitter::AddMutationToBatch(
    const TMetaVersion& version,
    const TSharedRef& recordData,
    TFuture<void> changeLogResult)
{
    TGuard<TSpinLock> guard(BatchSpinLock);
    auto batch = GetOrCreateBatch(version);
    auto result = batch->AddMutation(recordData);
    batch->SetLastChangeLogResult(changeLogResult);
    if (batch->GetMutationCount() >= Config->MaxBatchSize) {
        FlushCurrentBatch(false);
    }
    return result;
}

void TLeaderCommitter::FlushCurrentBatch(bool rotateChangeLog)
{
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);
    YCHECK(CurrentBatch);

    CurrentBatch->FlushMutations(rotateChangeLog);
    TDelayedInvoker::CancelAndClear(BatchTimeoutCookie);
    CurrentBatch.Reset();
    Profiler.Increment(BatchCommitCounter);
}

TLeaderCommitter::TBatchPtr TLeaderCommitter::GetOrCreateBatch(
    const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);

    if (!CurrentBatch) {
        CurrentBatch = New<TBatch>(
            ControlInvoker,
            CellManager,
            DecoratedState,
            Config,
            version,
            EpochId);

        YCHECK(!BatchTimeoutCookie);
        BatchTimeoutCookie = TDelayedInvoker::Submit(
            BIND(&TLeaderCommitter::OnBatchTimeout, MakeWeak(this), CurrentBatch)
                .Via(ControlInvoker),
            Config->MaxBatchDelay);
    }

    return CurrentBatch;
}

void TLeaderCommitter::OnBatchTimeout(TBatchPtr batch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TGuard<TSpinLock> guard(BatchSpinLock);
    if (batch != CurrentBatch)
        return;

    LOG_DEBUG("Flushing batched mutations");

    FlushCurrentBatch(false);
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TDecoratedMetaStatePtr metaState,
    IInvokerPtr controlInvoker,
    IInvokerPtr epochStateInvoker)
    : TCommitter(metaState, controlInvoker, epochStateInvoker)
{ }

TFollowerCommitter::~TFollowerCommitter()
{ }

TAsyncError TFollowerCommitter::Commit(
    const TMetaVersion& expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(!recordsData.empty());

    PROFILE_AGGREGATED_TIMING (CommitTimeCounter) {
        Profiler.Increment(CommitCounter, recordsData.size());
        Profiler.Increment(BatchCommitCounter);

        return
            BIND(
                &TFollowerCommitter::DoCommit,
                MakeStrong(this),
                expectedVersion,
                recordsData)
            .AsyncVia(EpochStateInvoker)
            .Run();
    }
}

TAsyncError TFollowerCommitter::DoCommit(
    const TMetaVersion& expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto currentVersion = DecoratedState->GetVersion();
    if (currentVersion > expectedVersion) {
        return MakeFuture(TError(
            ECommitCode::LateMutations,
            "Late mutations received by follower, ignored: expected %s but got %s",
            ~currentVersion.ToString(),
            ~expectedVersion.ToString()));
    }

    if (currentVersion != expectedVersion) {
        return MakeFuture(TError(
            ECommitCode::OutOfOrderMutations,
            "Out-of-order mutations received by follower: expected %s but got %s",
            ~currentVersion.ToString(),
            ~expectedVersion.ToString()));
    }

    LOG_DEBUG("Applying %d mutations at version %s",
        static_cast<int>(recordsData.size()),
        ~currentVersion.ToString());

    TFuture<void> result;
    FOREACH (const auto& recordData, recordsData) {
        result = DecoratedState->LogMutation(currentVersion, recordData);
        DecoratedState->ApplyMutation(recordData);
        ++currentVersion.RecordCount;
    }

    return result.Apply(BIND([] () -> TError {
        return TError();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
