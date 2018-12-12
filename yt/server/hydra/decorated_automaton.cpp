#include "decorated_automaton.h"
#include "automaton.h"
#include "changelog.h"
#include "config.h"
#include "mutation_context.h"
#include "serialize.h"
#include "snapshot.h"
#include "snapshot_discovery.h"

#include <yt/server/misc/fork_executor.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>
#include <yt/ytlib/hydra/hydra_service.pb.h>

#include <yt/core/actions/invoker_detail.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/blob.h>
#include <yt/core/misc/proc.h>

#include <yt/core/net/connection.h>

#include <yt/core/pipes/pipe.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/response_keeper.h>

#include <yt/core/logging/log_manager.h>

#include <util/random/random.h>

#include <util/system/file.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NElection;
using namespace NHydra::NProto;
using namespace NPipes;
using namespace NProfiling;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const i64 SnapshotTransferBlockSize = 1_MB;

////////////////////////////////////////////////////////////////////////////////

TSystemLockGuard::TSystemLockGuard(TSystemLockGuard&& other)
    : Automaton_(std::move(other.Automaton_))
{ }

TSystemLockGuard::~TSystemLockGuard()
{
    Release();
}

TSystemLockGuard& TSystemLockGuard::operator=(TSystemLockGuard&& other)
{
    Release();
    Automaton_ = std::move(other.Automaton_);
    return *this;
}

void TSystemLockGuard::Release()
{
    if (Automaton_) {
        Automaton_->ReleaseSystemLock();
        Automaton_.Reset();
    }
}

TSystemLockGuard::operator bool() const
{
    return static_cast<bool>(Automaton_);
}

TSystemLockGuard TSystemLockGuard::Acquire(TDecoratedAutomatonPtr automaton)
{
    automaton->AcquireSystemLock();
    return TSystemLockGuard(std::move(automaton));
}

TSystemLockGuard::TSystemLockGuard(TDecoratedAutomatonPtr automaton)
    : Automaton_(std::move(automaton))
{ }

////////////////////////////////////////////////////////////////////////////////

TUserLockGuard::TUserLockGuard(TUserLockGuard&& other)
    : Automaton_(std::move(other.Automaton_))
{ }

TUserLockGuard::~TUserLockGuard()
{
    Release();
}

TUserLockGuard& TUserLockGuard::operator=(TUserLockGuard&& other)
{
    Release();
    Automaton_ = std::move(other.Automaton_);
    return *this;
}

void TUserLockGuard::Release()
{
    if (Automaton_) {
        Automaton_->ReleaseUserLock();
        Automaton_.Reset();
    }
}

TUserLockGuard::operator bool() const
{
    return static_cast<bool>(Automaton_);
}

TUserLockGuard TUserLockGuard::TryAcquire(TDecoratedAutomatonPtr automaton)
{
    return automaton->TryAcquireUserLock()
        ? TUserLockGuard(std::move(automaton))
        : TUserLockGuard();
}

TUserLockGuard::TUserLockGuard(TDecoratedAutomatonPtr automaton)
    : Automaton_(std::move(automaton))
{ }

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TSystemInvoker
    : public TInvokerWrapper
{
public:
    explicit TSystemInvoker(TDecoratedAutomaton* decoratedAutomaton)
        : TInvokerWrapper(decoratedAutomaton->AutomatonInvoker_)
        , Owner_(decoratedAutomaton)
    { }

    virtual void Invoke(TClosure callback) override
    {
        auto lockGuard = TSystemLockGuard::Acquire(Owner_);

        auto doInvoke = [=, this_ = MakeStrong(this), callback = std::move(callback)] (TSystemLockGuard /*lockGuard*/) {
            TCurrentInvokerGuard currentInvokerGuard(this_);
            callback.Run();
        };

        Owner_->AutomatonInvoker_->Invoke(BIND(doInvoke, Passed(std::move(lockGuard))));
    }

private:
    TDecoratedAutomaton* const Owner_;

};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TGuardedUserInvoker
    : public TInvokerWrapper
{
public:
    TGuardedUserInvoker(
        TDecoratedAutomatonPtr decoratedAutomaton,
        IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , Owner_(decoratedAutomaton)
    { }

    virtual void Invoke(TClosure callback) override
    {
        auto lockGuard = TUserLockGuard::TryAcquire(Owner_);
        if (!lockGuard)
            return;

        auto doInvoke = [=, this_ = MakeStrong(this), callback = std::move(callback)] () {
            if (Owner_->GetState() != EPeerState::Leading &&
                Owner_->GetState() != EPeerState::Following)
                return;

            TCurrentInvokerGuard guard(this_);
            callback.Run();
        };

        UnderlyingInvoker_->Invoke(BIND(doInvoke));
    }

private:
    const TDecoratedAutomatonPtr Owner_;

};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TSnapshotBuilderBase
    : public virtual NLogging::TLoggerOwner
    , public virtual TRefCounted
{
public:
    TSnapshotBuilderBase(
        TDecoratedAutomatonPtr owner,
        TVersion snapshotVersion)
        : Owner_(owner)
        , SnapshotVersion_(snapshotVersion)
        , SnapshotId_(SnapshotVersion_.SegmentId + 1)
    {
        Logger = Owner_->Logger;
    }

    ~TSnapshotBuilderBase()
    {
        ReleaseLock();
    }

    TFuture<TRemoteSnapshotParams> Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        Logger.AddTag("SnapshotId: %v", SnapshotId_);

        try {
            TryAcquireLock();

            TSnapshotMeta meta;
            meta.set_prev_record_count(SnapshotVersion_.RecordId);

            SnapshotWriter_ = Owner_->SnapshotStore_->CreateWriter(SnapshotId_, meta);

            return DoRun().Apply(
                BIND(&TSnapshotBuilderBase::OnFinished, MakeStrong(this))
                    .AsyncVia(GetHydraIOInvoker()));
        } catch (const std::exception& ex) {
            ReleaseLock();
            return MakeFuture<TRemoteSnapshotParams>(TError(ex));
        }
    }

protected:
    const TDecoratedAutomatonPtr Owner_;
    const TVersion SnapshotVersion_;
    const int SnapshotId_;

    ISnapshotWriterPtr SnapshotWriter_;


    virtual TFuture<void> DoRun() = 0;

    void TryAcquireLock()
    {
        if (Owner_->BuildingSnapshot_.test_and_set()) {
            THROW_ERROR_EXCEPTION("Cannot start building snapshot %v since another snapshot is still being constructed",
                SnapshotId_);
        }
        LockAcquired_ = true;

        YT_LOG_INFO("Snapshot builder lock acquired");
    }

    void ReleaseLock()
    {
        if (LockAcquired_) {
            Owner_->BuildingSnapshot_.clear();
            LockAcquired_ = false;

            YT_LOG_INFO("Snapshot builder lock released");
        }
    }

private:
    bool LockAcquired_ = false;


    TRemoteSnapshotParams OnFinished(const TError& error)
    {
        ReleaseLock();

        error.ThrowOnError();

        const auto& params = SnapshotWriter_->GetParams();

        TRemoteSnapshotParams remoteParams;
        remoteParams.PeerId = Owner_->CellManager_->GetSelfPeerId();
        remoteParams.SnapshotId = SnapshotId_;
        static_cast<TSnapshotParams&>(remoteParams) = params;
        return remoteParams;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TForkSnapshotBuilder
    : public TSnapshotBuilderBase
    , public TForkExecutor
{
public:
    TForkSnapshotBuilder(
        TDecoratedAutomatonPtr owner,
        TVersion snapshotVersion)
        : TDecoratedAutomaton::TSnapshotBuilderBase(owner, snapshotVersion)
    { }

private:
    IAsyncInputStreamPtr InputStream_;
    std::unique_ptr<TFile> OutputFile_;

    TFuture<void> AsyncTransferResult_;


    virtual TFuture<void> DoRun() override
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        auto pipe = TPipeFactory().Create();
        YT_LOG_INFO("Snapshot transfer pipe opened (Pipe: %v)",
            pipe);

        InputStream_ = pipe.CreateAsyncReader();
        OutputFile_ = std::make_unique<TFile>(FHANDLE(pipe.ReleaseWriteFD()));

        AsyncTransferResult_ = BIND(&TForkSnapshotBuilder::TransferLoop, MakeStrong(this))
            .AsyncVia(GetWatchdogInvoker())
            .Run();

        return Fork().Apply(
            BIND(&TForkSnapshotBuilder::OnFinished, MakeStrong(this))
                .AsyncVia(GetHydraIOInvoker()));
    }

    virtual TDuration GetTimeout() const override
    {
        return Owner_->Config_->SnapshotBuildTimeout;
    }

    virtual void RunChild() override
    {
        CloseAllDescriptors({
            2, // stderr
            int(OutputFile_->GetHandle())
        });
        TUnbufferedFileOutput output(*OutputFile_);
        auto writer = CreateAsyncAdapter(&output);
        Owner_->SaveSnapshot(writer)
            .Get()
            .ThrowOnError();
        OutputFile_->Close();
    }

    virtual void RunParent() override
    {
        OutputFile_->Close();
    }

    virtual void Cleanup() override
    {
        ReleaseLock();
    }

    void TransferLoop()
    {
        YT_LOG_INFO("Snapshot transfer loop started");

        WaitFor(SnapshotWriter_->Open())
            .ThrowOnError();

        auto zeroCopyReader = CreateZeroCopyAdapter(InputStream_, SnapshotTransferBlockSize);
        auto zeroCopyWriter = CreateZeroCopyAdapter(SnapshotWriter_);

        TFuture<void> lastWriteResult;
        i64 size = 0;

        while (true) {
            auto block = WaitFor(zeroCopyReader->Read())
                .ValueOrThrow();

            if (!block)
                break;

            size += block.Size();
            lastWriteResult = zeroCopyWriter->Write(block);
        }

        if (lastWriteResult) {
            WaitFor(lastWriteResult)
                .ThrowOnError();
        }

        YT_LOG_INFO("Snapshot transfer loop completed (Size: %v)",
            size);
    }

    void OnFinished()
    {
        YT_LOG_INFO("Waiting for transfer loop to finish");
        WaitFor(AsyncTransferResult_)
            .ThrowOnError();
        YT_LOG_INFO("Transfer loop finished");

        YT_LOG_INFO("Waiting for snapshot writer to close");
        WaitFor(SnapshotWriter_->Close())
            .ThrowOnError();
        YT_LOG_INFO("Snapshot writer closed");
    }
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  The stream goes through the following sequence of states:
 *  1. initially it is created in sync mode
 *  2. then it is suspended
 *  3. then it is resumed in async mode
 *
 */
class TDecoratedAutomaton::TSwitchableSnapshotWriter
    : public IAsyncOutputStream
{
public:
    explicit TSwitchableSnapshotWriter(const NLogging::TLogger& logger)
        : Logger(logger)
    { }

    void Suspend()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        SuspendedPromise_ = NewPromise<void>();
    }

    void ResumeAsAsync(IAsyncOutputStreamPtr underlyingStream)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto suspendedPromise = SuspendedPromise_;
        SuspendedPromise_.Reset();
        UnderlyingStream_ = CreateZeroCopyAdapter(underlyingStream);
        for (const auto& syncBlock : SyncBlocks_) {
            ForwardBlock(syncBlock);
        }
        SyncBlocks_.clear();
        guard.Release();
        suspendedPromise.Set();
    }

    void Abort()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto suspendedPromise = SuspendedPromise_;
        guard.Release();

        if (suspendedPromise) {
            suspendedPromise.TrySet(TError("Snapshot writer aborted"));
        }
    }

    virtual TFuture<void> Close() override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return LastForwardResult_;
    }

    virtual TFuture<void> Write(const TSharedRef& block) override
    {
        // NB: We are not allowed to store by-ref copies of #block, cf. #IAsyncOutputStream::Write.
        struct TBlockTag { };
        auto blockCopy = TSharedRef::MakeCopy<TBlockTag>(block);

        TGuard<TSpinLock> guard(SpinLock_);
        if (UnderlyingStream_) {
            YT_LOG_TRACE("Got async snapshot block (Size: %v)", blockCopy.Size());
            AsyncSize_ += block.Size();
            return ForwardBlock(blockCopy);
        } else {
            YT_LOG_TRACE("Got sync snapshot block (Size: %v)", blockCopy.Size());
            SyncBlocks_.push_back(blockCopy);
            SyncSize_ += block.Size();
            return SuspendedPromise_ ? SuspendedPromise_.ToFuture() : VoidFuture;
        }
    }

    i64 GetSyncSize() const
    {
        YCHECK(UnderlyingStream_);
        return SyncSize_;
    }

    i64 GetAsyncSize() const
    {
        YCHECK(UnderlyingStream_);
        return AsyncSize_;
    }

private:
    const NLogging::TLogger Logger;

    TSpinLock SpinLock_;
    TPromise<void> SuspendedPromise_;
    i64 SyncSize_ = 0;
    i64 AsyncSize_ = 0;
    IAsyncZeroCopyOutputStreamPtr UnderlyingStream_;
    std::vector<TSharedRef> SyncBlocks_;
    TFuture<void> LastForwardResult_ = VoidFuture;


    TFuture<void> ForwardBlock(const TSharedRef& block)
    {
        return LastForwardResult_ = UnderlyingStream_->Write(block);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TNoForkSnapshotBuilder
    : public TSnapshotBuilderBase
{
public:
    TNoForkSnapshotBuilder(
        TDecoratedAutomatonPtr owner,
        TVersion snapshotVersion)
        : TDecoratedAutomaton::TSnapshotBuilderBase(owner, snapshotVersion)
    { }

    ~TNoForkSnapshotBuilder()
    {
        if (SwitchableSnapshotWriter_) {
            SwitchableSnapshotWriter_->Abort();
        }
    }

private:
    TIntrusivePtr<TSwitchableSnapshotWriter> SwitchableSnapshotWriter_;

    TFuture<void> AsyncOpenWriterResult_;
    TFuture<void> AsyncSaveSnapshotResult_;


    virtual TFuture<void> DoRun() override
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        SwitchableSnapshotWriter_ = New<TSwitchableSnapshotWriter>(Logger);

        AsyncOpenWriterResult_ = SnapshotWriter_->Open();

        YT_LOG_INFO("Snapshot sync phase started");

        AsyncSaveSnapshotResult_ = Owner_->SaveSnapshot(SwitchableSnapshotWriter_);

        YT_LOG_INFO("Snapshot sync phase completed");

        SwitchableSnapshotWriter_->Suspend();

        return BIND(&TNoForkSnapshotBuilder::DoRunAsync, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

    void DoRunAsync()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        WaitFor(AsyncOpenWriterResult_)
            .ThrowOnError();

        YT_LOG_INFO("Switching to async snapshot writer");

        SwitchableSnapshotWriter_->ResumeAsAsync(SnapshotWriter_);

        WaitFor(AsyncSaveSnapshotResult_)
            .ThrowOnError();

        YT_LOG_INFO("Snapshot async phase completed (SyncSize: %v, AsyncSize: %v)",
            SwitchableSnapshotWriter_->GetSyncSize(),
            SwitchableSnapshotWriter_->GetAsyncSize());

        WaitFor(SwitchableSnapshotWriter_->Close())
            .ThrowOnError();

        WaitFor(SnapshotWriter_->Close())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TDecoratedAutomaton::TDecoratedAutomaton(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TCellManagerPtr cellManager,
    IAutomatonPtr automaton,
    IInvokerPtr automatonInvoker,
    IInvokerPtr controlInvoker,
    ISnapshotStorePtr snapshotStore,
    const NProfiling::TProfiler& profiler)
    : Config_(std::move(config))
    , Options_(options)
    , CellManager_(std::move(cellManager))
    , Automaton_(std::move(automaton))
    , AutomatonInvoker_(std::move(automatonInvoker))
    , DefaultGuardedUserInvoker_(CreateGuardedUserInvoker(AutomatonInvoker_))
    , ControlInvoker_(std::move(controlInvoker))
    , SystemInvoker_(New<TSystemInvoker>(this))
    , SnapshotStore_(std::move(snapshotStore))
    , Logger(NLogging::TLogger(HydraLogger)
        .AddTag("CellId: %v", CellManager_->GetCellId()))
    , Profiler(profiler)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(Automaton_);
    YCHECK(ControlInvoker_);
    YCHECK(SnapshotStore_);
    VERIFY_INVOKER_THREAD_AFFINITY(AutomatonInvoker_, AutomatonThread);
    VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);

    StopEpoch();
}

void TDecoratedAutomaton::Initialize()
{
    AutomatonInvoker_->Invoke(BIND([=, this_ = MakeStrong(this)] () {
        Automaton_->Clear();
        Automaton_->SetZeroState();
    }));
}

void TDecoratedAutomaton::OnStartLeading(TEpochContextPtr epochContext)
{
    YCHECK(State_ == EPeerState::Stopped);
    State_ = EPeerState::LeaderRecovery;
    StartEpoch(epochContext);
}

void TDecoratedAutomaton::OnLeaderRecoveryComplete()
{
    YCHECK(State_ == EPeerState::LeaderRecovery);
    State_ = EPeerState::Leading;
    LastSnapshotTime_ = TInstant::Now();
}

void TDecoratedAutomaton::OnStopLeading()
{
    YCHECK(State_ == EPeerState::Leading || State_ == EPeerState::LeaderRecovery);
    State_ = EPeerState::Stopped;
    StopEpoch();
}

void TDecoratedAutomaton::OnStartFollowing(TEpochContextPtr epochContext)
{
    YCHECK(State_ == EPeerState::Stopped);
    State_ = EPeerState::FollowerRecovery;
    StartEpoch(epochContext);
}

void TDecoratedAutomaton::OnFollowerRecoveryComplete()
{
    YCHECK(State_ == EPeerState::FollowerRecovery);
    State_ = EPeerState::Following;
    LastSnapshotTime_ = TInstant::Now();
}

void TDecoratedAutomaton::OnStopFollowing()
{
    YCHECK(State_ == EPeerState::Following || State_ == EPeerState::FollowerRecovery);
    State_ = EPeerState::Stopped;
    StopEpoch();
}

IInvokerPtr TDecoratedAutomaton::CreateGuardedUserInvoker(IInvokerPtr underlyingInvoker)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return New<TGuardedUserInvoker>(this, underlyingInvoker);
}

IInvokerPtr TDecoratedAutomaton::GetDefaultGuardedUserInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return DefaultGuardedUserInvoker_;
}

IInvokerPtr TDecoratedAutomaton::GetSystemInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SystemInvoker_;
}

TFuture<void> TDecoratedAutomaton::SaveSnapshot(IAsyncOutputStreamPtr writer)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // Context switches are not allowed during sync phase.
    TForbidContextSwitchGuard contextSwitchGuard;
    return Automaton_->SaveSnapshot(writer);
}

void TDecoratedAutomaton::LoadSnapshot(
    int snapshotId,
    TVersion version,
    IAsyncZeroCopyInputStreamPtr reader)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_LOG_INFO("Started loading snapshot (SnapshotId: %v, Version: %v)",
        snapshotId,
        version);

    PROFILE_TIMING ("/snapshot_load_time") {
        Automaton_->Clear();
        try {
            AutomatonVersion_ = CommittedVersion_ = TVersion(-1, -1);
            Automaton_->LoadSnapshot(reader);
        } catch (...) {
            // Don't leave the state corrupted.
            // NB: We could be in an arbitrary thread here.
            AutomatonInvoker_->Invoke(BIND(&IAutomaton::Clear, Automaton_));
            throw;
        }
    }

    YT_LOG_INFO("Finished loading snapshot");

    // YT-3926
    AutomatonVersion_ = CommittedVersion_ = TVersion(snapshotId, 0);
}

void TDecoratedAutomaton::ApplyMutationDuringRecovery(const TSharedRef& recordData)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMutationHeader header;
    TSharedRef requestData;
    DeserializeMutationRecord(recordData, &header, &requestData);

    auto mutationVersion = TVersion(header.segment_id(), header.record_id());
    RotateAutomatonVersionIfNeeded(mutationVersion);

    RecoveryRecordCount_ += 1;
    RecoveryDataSize_ += recordData.Size();

    TMutationRequest request;
    request.Type = header.mutation_type();
    if (header.has_mutation_id()) {
        request.MutationId = FromProto<TMutationId>(header.mutation_id());
    }
    request.Data = std::move(requestData);

    TMutationContext context(
        AutomatonVersion_,
        request,
        FromProto<TInstant>(header.timestamp()),
        header.random_seed());

    DoApplyMutation(&context);
}

const TMutationRequest& TDecoratedAutomaton::LogLeaderMutation(
    TInstant commitStartTime,
    TMutationRequest&& request,
    TSharedRef* recordData,
    TFuture<void>* localFlushResult,
    TFuture<TMutationResponse>* commitResult)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(recordData);
    Y_ASSERT(localFlushResult);
    Y_ASSERT(commitResult);
    Y_ASSERT(!RotatingChangelog_);

    PendingMutations_.emplace(
        LoggedVersion_,
        std::move(request),
        commitStartTime,
        RandomNumber<ui64>());
    const auto& pendingMutation = PendingMutations_.back();

    MutationHeader_.Clear(); // don't forget to cleanup the pooled instance
    MutationHeader_.set_mutation_type(pendingMutation.Request.Type);
    MutationHeader_.set_timestamp(pendingMutation.Timestamp.GetValue());
    MutationHeader_.set_random_seed(pendingMutation.RandomSeed);
    MutationHeader_.set_segment_id(pendingMutation.Version.SegmentId);
    MutationHeader_.set_record_id(pendingMutation.Version.RecordId);
    if (pendingMutation.Request.MutationId) {
        ToProto(MutationHeader_.mutable_mutation_id(), pendingMutation.Request.MutationId);
    }

    *recordData = SerializeMutationRecord(MutationHeader_, pendingMutation.Request.Data);
    *localFlushResult = Changelog_->Append(*recordData);
    *commitResult = pendingMutation.CommitPromise;

    LoggedVersion_ = pendingMutation.Version.Advance();
    YCHECK(EpochContext_->ReachableVersion < LoggedVersion_);

    return pendingMutation.Request;
}

TFuture<TMutationResponse> TDecoratedAutomaton::TryBeginKeptRequest(const TMutationRequest& request)
{
    YCHECK(State_ == EPeerState::Leading);

    if (!Options_.ResponseKeeper) {
        return TFuture<TMutationResponse>();
    }

    if (!request.MutationId) {
        return TFuture<TMutationResponse>();
    }

    auto asyncResponseData = Options_.ResponseKeeper->TryBeginRequest(request.MutationId, request.Retry);
    if (!asyncResponseData) {
        PendingMutationIds_.push(request.MutationId);
        return TFuture<TMutationResponse>();
    }

    return asyncResponseData.Apply(BIND([] (const TSharedRefArray& data) {
        return TMutationResponse{data};
    }));
}

void TDecoratedAutomaton::LogFollowerMutation(
    const TSharedRef& recordData,
    TFuture<void>* logResult)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(!RotatingChangelog_);

    TSharedRef mutationData;
    DeserializeMutationRecord(recordData, &MutationHeader_, &mutationData);

    auto version = LoggedVersion_.load();

    TMutationRequest request;
    request.Type = std::move(*MutationHeader_.mutable_mutation_type());
    request.Data = std::move(mutationData);
    request.MutationId = MutationHeader_.has_mutation_id()
        ? FromProto<TMutationId>(MutationHeader_.mutation_id())
        : TMutationId();

    PendingMutations_.emplace(
        version,
        std::move(request),
        FromProto<TInstant>(MutationHeader_.timestamp()),
        MutationHeader_.random_seed());

    if (Changelog_) {
        auto actualLogResult = Changelog_->Append(recordData);
        if (logResult) {
            *logResult = std::move(actualLogResult);
        }
    }

    LoggedVersion_ = version.Advance();
    YCHECK(EpochContext_->ReachableVersion < LoggedVersion_);
}

TFuture<TRemoteSnapshotParams> TDecoratedAutomaton::BuildSnapshot()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    CancelSnapshot();

    auto loggedVersion = GetLoggedVersion();

    YT_LOG_INFO("Snapshot scheduled (Version: %v)",
        loggedVersion);

    LastSnapshotTime_ = TInstant::Now();
    SnapshotVersion_ = loggedVersion;
    SnapshotParamsPromise_ = NewPromise<TRemoteSnapshotParams>();

    MaybeStartSnapshotBuilder();

    return SnapshotParamsPromise_;
}

TFuture<void> TDecoratedAutomaton::RotateChangelog()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto loggedVersion = GetLoggedVersion();

    YT_LOG_INFO("Rotating changelog (Version: %v)",
        loggedVersion);

    YCHECK(!RotatingChangelog_);
    RotatingChangelog_ = true;

    return BIND(&TDecoratedAutomaton::DoRotateChangelog, MakeStrong(this))
        .AsyncVia(EpochContext_->EpochUserAutomatonInvoker)
        .Run();
}

void TDecoratedAutomaton::DoRotateChangelog()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto loggedVersion = GetLoggedVersion();
    auto rotatedVersion = loggedVersion.Rotate();

    if (Changelog_) {
        WaitFor(Changelog_->Flush())
            .ThrowOnError();

        YCHECK(loggedVersion.RecordId == Changelog_->GetRecordCount());

        TChangelogMeta meta;
        meta.set_prev_record_count(loggedVersion.RecordId);

        auto asyncNewChangelog = EpochContext_->ChangelogStore->CreateChangelog(
            rotatedVersion.SegmentId,
            meta);
        Changelog_ = WaitFor(asyncNewChangelog)
            .ValueOrThrow();
    }

    LoggedVersion_ = rotatedVersion;
    RecoveryRecordCount_ = 0;
    RecoveryDataSize_ = 0;

    YCHECK(RotatingChangelog_);
    RotatingChangelog_ = false;

    YCHECK(EpochContext_->ReachableVersion < LoggedVersion_);

    YT_LOG_INFO("Changelog rotated");
}

void TDecoratedAutomaton::CommitMutations(TVersion version, bool mayYield)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (version > CommittedVersion_) {
        CommittedVersion_ = version;
        YT_LOG_DEBUG("Committed version promoted (Version: %v)",
            version);
    }

    ApplyPendingMutations(mayYield);
}

bool TDecoratedAutomaton::HasReadyMutations() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PendingMutations_.empty()) {
        return false;
    }

    const auto& pendingMutation = PendingMutations_.front();
    return pendingMutation.Version < CommittedVersion_;
}

void TDecoratedAutomaton::ApplyPendingMutations(bool mayYield)
{
    TForbidContextSwitchGuard contextSwitchGuard;

    TWallTimer timer;
    PROFILE_AGGREGATED_TIMING (BatchCommitTimeCounter_) {
        while (!PendingMutations_.empty()) {
            auto& pendingMutation = PendingMutations_.front();
            if (pendingMutation.Version >= CommittedVersion_) {
                break;
            }

            RotateAutomatonVersionIfNeeded(pendingMutation.Version);

            TMutationContext context(
                AutomatonVersion_,
                pendingMutation.Request,
                pendingMutation.Timestamp,
                pendingMutation.RandomSeed);

            // Cf. YT-6908; see below.
            auto commitPromise = pendingMutation.CommitPromise;

            DoApplyMutation(&context);

            if (commitPromise) {
                commitPromise.Set(context.Response());
            }

            PendingMutations_.pop();

            MaybeStartSnapshotBuilder();

            if (mayYield && timer.GetElapsedTime() > Config_->MaxCommitBatchDuration) {
                EpochContext_->EpochUserAutomatonInvoker->Invoke(
                    BIND(&TDecoratedAutomaton::ApplyPendingMutations, MakeStrong(this), true));
                break;
            }
        }
    }
}

void TDecoratedAutomaton::RotateAutomatonVersionIfNeeded(TVersion mutationVersion)
{
    auto automatonVersion = GetAutomatonVersion();
    if (mutationVersion.SegmentId == automatonVersion.SegmentId) {
        YCHECK(mutationVersion.RecordId == automatonVersion.RecordId);
    } else {
        YCHECK(mutationVersion.SegmentId > automatonVersion.SegmentId);
        YCHECK(mutationVersion.RecordId == 0);
        RotateAutomatonVersion(mutationVersion.SegmentId);
    }
}

void TDecoratedAutomaton::DoApplyMutation(TMutationContext* context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto automatonVersion = GetAutomatonVersion();

    // Cannot access #request after the handler has been invoked since the latter
    // could submit more mutations and cause #PendingMutations_ to be reallocated.
    // So we'd better make the needed copies right away.
    // Cf. YT-6908.
    auto mutationId = context->Request().MutationId;

    {
        TMutationContextGuard guard(context);
        Automaton_->ApplyMutation(context);
    }

    if (Options_.ResponseKeeper && mutationId) {
        if (State_ == EPeerState::Leading) {
            YCHECK(mutationId == PendingMutationIds_.front());
            PendingMutationIds_.pop();
        }
        const auto& response = context->Response();
        Options_.ResponseKeeper->EndRequest(mutationId, response.Data);
    }

    AutomatonVersion_ = automatonVersion.Advance();
    if (CommittedVersion_.load() < automatonVersion) {
        CommittedVersion_ = automatonVersion;
    }
}

EPeerState TDecoratedAutomaton::GetState() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return State_;
}

TVersion TDecoratedAutomaton::GetLoggedVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return LoggedVersion_;
}

void TDecoratedAutomaton::SetLoggedVersion(TVersion version)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    LoggedVersion_ = version;
}

void TDecoratedAutomaton::SetChangelog(IChangelogPtr changelog)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Changelog_ = std::move(changelog);
}

int TDecoratedAutomaton::GetRecordCountSinceLastCheckpoint() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return GetLoggedVersion().RecordId + RecoveryRecordCount_;
}

i64 TDecoratedAutomaton::GetDataSizeSinceLastCheckpoint() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return Changelog_->GetDataSize() + RecoveryDataSize_;
}

TInstant TDecoratedAutomaton::GetLastSnapshotTime() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return LastSnapshotTime_;
}

TVersion TDecoratedAutomaton::GetAutomatonVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return AutomatonVersion_.load();
}

void TDecoratedAutomaton::RotateAutomatonVersion(int segmentId)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(GetAutomatonVersion().SegmentId < segmentId);

    auto automatonVersion = TVersion(segmentId, 0);
    AutomatonVersion_ = automatonVersion;
    if (CommittedVersion_.load() < automatonVersion) {
        CommittedVersion_ = automatonVersion;
    }

    YT_LOG_INFO("Automaton version rotated (Version: %v)",
        automatonVersion);
}

TVersion TDecoratedAutomaton::GetCommittedVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CommittedVersion_.load();
}

TVersion TDecoratedAutomaton::GetPingVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (State_ == EPeerState::LeaderRecovery) {
        return EpochContext_->ReachableVersion;
    }

    auto loggedVersion = GetLoggedVersion();
    if (RotatingChangelog_) {
        return loggedVersion.Rotate();
    }

    return loggedVersion;
}

bool TDecoratedAutomaton::TryAcquireUserLock()
{
    if (SystemLock_.load() != 0) {
        return false;
    }
    ++UserLock_;
    if (SystemLock_.load() != 0) {
        --UserLock_;
        return false;
    }
    return true;
}

void TDecoratedAutomaton::ReleaseUserLock()
{
    --UserLock_;
}

void TDecoratedAutomaton::AcquireSystemLock()
{
    int result = ++SystemLock_;
    while (UserLock_.load() != 0) {
        SpinLockPause();
    }
    YT_LOG_DEBUG("System lock acquired (Lock: %v)",
        result);
}

void TDecoratedAutomaton::ReleaseSystemLock()
{
    int result = --SystemLock_;
    YT_LOG_DEBUG("System lock released (Lock: %v)",
        result);
}

void TDecoratedAutomaton::StartEpoch(TEpochContextPtr epochContext)
{
    YCHECK(!EpochContext_);
    EpochContext_ = std::move(epochContext);

    // Enable batching for log messages.
    NLogging::TLogManager::Get()->SetPerThreadBatchingPeriod(Config_->AutomatonThreadLogBatchingPeriod);
}

void TDecoratedAutomaton::CancelSnapshot()
{
    if (SnapshotParamsPromise_ && SnapshotParamsPromise_.ToFuture().Cancel()) {
        YT_LOG_INFO("Snapshot canceled");
    }
    SnapshotParamsPromise_.Reset();
}

void TDecoratedAutomaton::StopEpoch()
{
    auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");

    while (!PendingMutations_.empty()) {
        auto& pendingMutation = PendingMutations_.front();
        if (pendingMutation.CommitPromise) {
            pendingMutation.CommitPromise.Set(error);
        }
        PendingMutations_.pop();
    }

    while (!PendingMutationIds_.empty()) {
        Options_.ResponseKeeper->CancelRequest(PendingMutationIds_.front(), error);
        PendingMutationIds_.pop();
    }

    RotatingChangelog_ = false;
    Changelog_.Reset();
    EpochContext_.Reset();
    SnapshotVersion_ = TVersion();
    LoggedVersion_ = TVersion();
    CommittedVersion_ = TVersion();
    CancelSnapshot();
    RecoveryRecordCount_ = 0;
    RecoveryDataSize_ = 0;

    // Disable batching for log messages.
    NLogging::TLogManager::Get()->SetPerThreadBatchingPeriod(TDuration::Zero());
}

void TDecoratedAutomaton::MaybeStartSnapshotBuilder()
{
    if (GetAutomatonVersion() != SnapshotVersion_)
        return;

    auto builder = Options_.UseFork
       ? TIntrusivePtr<TSnapshotBuilderBase>(New<TForkSnapshotBuilder>(this, SnapshotVersion_))
       : TIntrusivePtr<TSnapshotBuilderBase>(New<TNoForkSnapshotBuilder>(this, SnapshotVersion_));
    SnapshotParamsPromise_.SetFrom(builder->Run());
}

bool TDecoratedAutomaton::IsRecovery()
{
    return
        State_ == EPeerState::LeaderRecovery ||
        State_ == EPeerState::FollowerRecovery;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
