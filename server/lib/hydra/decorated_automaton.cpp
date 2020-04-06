#include "decorated_automaton.h"
#include "automaton.h"
#include "changelog.h"
#include "config.h"
#include "mutation_context.h"
#include "serialize.h"
#include "snapshot.h"
#include "snapshot_discovery.h"

#include <yt/server/lib/misc/fork_executor.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hydra/proto/hydra_manager.pb.h>
#include <yt/ytlib/hydra/proto/hydra_service.pb.h>

#include <yt/core/actions/invoker_detail.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/misc/blob.h>
#include <yt/core/misc/proc.h>

#include <yt/core/net/connection.h>

#include <yt/core/utilex/random.h>

#include <yt/library/process/pipe.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/response_keeper.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/tracing/trace_context.h>

#include <util/random/random.h>

#include <util/system/file.h>

#include <algorithm> // for std::max

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
    explicit TSnapshotBuilderBase(TDecoratedAutomatonPtr owner)
        : Owner_(owner)
        , SequenceNumber_(Owner_->SequenceNumber_)
        , SnapshotId_(Owner_->SnapshotVersion_.SegmentId + 1)
        , RandomSeed_(Owner_->RandomSeed_)
        , EpochContext_(Owner_->EpochContext_)
    {
        Logger = NLogging::TLogger(Owner_->Logger)
            .AddTag("SnapshotId: %v", SnapshotId_);
    }

    ~TSnapshotBuilderBase()
    {
        ReleaseLock();
    }

    TFuture<TRemoteSnapshotParams> Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        try {
            TryAcquireLock();

            TSnapshotMeta meta;
            meta.set_sequence_number(SequenceNumber_);
            meta.set_random_seed(RandomSeed_);

            SnapshotWriter_ = Owner_->SnapshotStore_->CreateWriter(SnapshotId_, meta);

            return DoRun().Apply(
                BIND(&TSnapshotBuilderBase::OnFinished, MakeStrong(this))
                    .AsyncVia(GetHydraIOInvoker()));
        } catch (const std::exception& ex) {
            ReleaseLock();
            return MakeFuture<TRemoteSnapshotParams>(TError(ex));
        }
    }

    int GetSnapshotId() const
    {
        return SnapshotId_;
    }

protected:
    const TDecoratedAutomatonPtr Owner_;
    const i64 SequenceNumber_;
    const int SnapshotId_;
    const ui64 RandomSeed_;
    const TEpochContextPtr EpochContext_;

    ISnapshotWriterPtr SnapshotWriter_;


    virtual TFuture<void> DoRun() = 0;

    void TryAcquireLock()
    {
        bool expected = false;
        if (!Owner_->BuildingSnapshot_.compare_exchange_strong(expected, true)) {
            THROW_ERROR_EXCEPTION("Cannot start building snapshot %v since another snapshot is still being constructed",
                SnapshotId_);
        }
        LockAcquired_ = true;

        YT_LOG_INFO("Snapshot builder lock acquired");
    }

    void ReleaseLock()
    {
        if (LockAcquired_) {
            auto delay = Owner_->Config_->BuildSnapshotDelay;
            if (delay != TDuration::Zero()) {
                YT_LOG_DEBUG("Working in testing mode, sleeping (BuildSnapshotDelay: %v)", delay);
                TDelayedExecutor::WaitForDuration(delay);
            }

            Owner_->BuildingSnapshot_.store(false);
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
        remoteParams.PeerId = EpochContext_->CellManager->GetSelfPeerId();
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
    using TSnapshotBuilderBase::TSnapshotBuilderBase;

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
        YT_VERIFY(UnderlyingStream_);
        return SyncSize_;
    }

    i64 GetAsyncSize() const
    {
        YT_VERIFY(UnderlyingStream_);
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
    using TSnapshotBuilderBase::TSnapshotBuilderBase;

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
    IAutomatonPtr automaton,
    IInvokerPtr automatonInvoker,
    IInvokerPtr controlInvoker,
    ISnapshotStorePtr snapshotStore,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : Config_(std::move(config))
    , Options_(options)
    , Automaton_(std::move(automaton))
    , AutomatonInvoker_(std::move(automatonInvoker))
    , DefaultGuardedUserInvoker_(CreateGuardedUserInvoker(AutomatonInvoker_))
    , ControlInvoker_(std::move(controlInvoker))
    , SystemInvoker_(New<TSystemInvoker>(this))
    , SnapshotStore_(std::move(snapshotStore))
    , Logger(logger)
    , Profiler(profiler)
{
    YT_VERIFY(Config_);
    YT_VERIFY(Automaton_);
    YT_VERIFY(ControlInvoker_);
    YT_VERIFY(SnapshotStore_);
    VERIFY_INVOKER_THREAD_AFFINITY(AutomatonInvoker_, AutomatonThread);
    VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
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
    YT_VERIFY(State_ == EPeerState::Stopped);
    State_ = EPeerState::LeaderRecovery;
    StartEpoch(epochContext);
}

void TDecoratedAutomaton::OnLeaderRecoveryComplete()
{
    YT_VERIFY(State_ == EPeerState::LeaderRecovery);
    State_ = EPeerState::Leading;
    UpdateSnapshotBuildDeadline();
}

void TDecoratedAutomaton::OnStopLeading()
{
    YT_VERIFY(State_ == EPeerState::Leading || State_ == EPeerState::LeaderRecovery);
    State_ = EPeerState::Stopped;
    StopEpoch();
}

void TDecoratedAutomaton::OnStartFollowing(TEpochContextPtr epochContext)
{
    YT_VERIFY(State_ == EPeerState::Stopped);
    State_ = EPeerState::FollowerRecovery;
    StartEpoch(epochContext);
}

void TDecoratedAutomaton::OnFollowerRecoveryComplete()
{
    YT_VERIFY(State_ == EPeerState::FollowerRecovery);
    State_ = EPeerState::Following;
    UpdateSnapshotBuildDeadline();
}

void TDecoratedAutomaton::OnStopFollowing()
{
    YT_VERIFY(State_ == EPeerState::Following || State_ == EPeerState::FollowerRecovery);
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
    i64 sequenceNumber,
    ui64 randomSeed,
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
            RandomSeed_ = 0;
            SequenceNumber_ = 0;
            Automaton_->LoadSnapshot(reader);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Snapshot load failed; clearing state");
            Automaton_->Clear();
            throw;
        } catch (const TFiberCanceledException&) {
            YT_LOG_INFO("Snapshot load fiber was canceled");
            throw;
        } catch (...) {
            YT_LOG_ERROR("Snapshot load failed with an unknown error");
            throw;
        }
    }

    YT_LOG_INFO("Finished loading snapshot");

    // YT-3926
    AutomatonVersion_ = CommittedVersion_ = TVersion(snapshotId, 0);
    RandomSeed_ = randomSeed;
    SequenceNumber_ = sequenceNumber;
}

void TDecoratedAutomaton::ValidateSnapshot(IAsyncZeroCopyInputStreamPtr reader)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(State_ == EPeerState::Stopped);
    State_ = EPeerState::LeaderRecovery;

    LoadSnapshot(0, TVersion{}, 0, 0, reader);

    YT_VERIFY(State_ == EPeerState::LeaderRecovery);
    State_ = EPeerState::Stopped;
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
    request.Reign = header.reign();
    request.Type = header.mutation_type();
    request.MutationId = FromProto<TMutationId>(header.mutation_id());
    request.Data = std::move(requestData);

    TMutationContext context(
        AutomatonVersion_,
        request,
        FromProto<TInstant>(header.timestamp()),
        header.random_seed(),
        header.prev_random_seed(),
        header.sequence_number());

    DoApplyMutation(&context);
}

const TDecoratedAutomaton::TPendingMutation& TDecoratedAutomaton::LogLeaderMutation(
    TInstant timestamp,
    TMutationRequest&& request,
    NTracing::TTraceContextPtr traceContext,
    TSharedRef* recordData,
    TFuture<void>* localFlushFuture)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(recordData);
    YT_ASSERT(localFlushFuture);
    YT_ASSERT(!RotatingChangelog_);

    auto version = LoggedVersion_.load();

    const auto& pendingMutation = PendingMutations_.emplace(
        version,
        std::move(request),
        timestamp,
        RandomNumber<ui64>(),
        GetLastLoggedRandomSeed(),
        GetLastLoggedSequenceNumber() + 1,
        std::move(traceContext));

    MutationHeader_.Clear(); // don't forget to cleanup the pooled instance
    MutationHeader_.set_reign(pendingMutation.Request.Reign);
    MutationHeader_.set_mutation_type(pendingMutation.Request.Type);
    MutationHeader_.set_timestamp(pendingMutation.Timestamp.GetValue());
    MutationHeader_.set_random_seed(pendingMutation.RandomSeed);
    MutationHeader_.set_segment_id(pendingMutation.Version.SegmentId);
    MutationHeader_.set_record_id(pendingMutation.Version.RecordId);
    MutationHeader_.set_prev_random_seed(pendingMutation.PrevRandomSeed);
    MutationHeader_.set_sequence_number(pendingMutation.SequenceNumber);
    if (pendingMutation.Request.MutationId) {
        ToProto(MutationHeader_.mutable_mutation_id(), pendingMutation.Request.MutationId);
    }

    *recordData = SerializeMutationRecord(MutationHeader_, pendingMutation.Request.Data);
    *localFlushFuture = Changelog_->Append(*recordData);

    auto newLoggedVersion = version.Advance();
    YT_VERIFY(EpochContext_->ReachableVersion < newLoggedVersion);
    LoggedVersion_ = newLoggedVersion;

    return pendingMutation;
}

TFuture<TMutationResponse> TDecoratedAutomaton::TryBeginKeptRequest(const TMutationRequest& request)
{
    YT_VERIFY(State_ == EPeerState::Leading);

    if (!Options_.ResponseKeeper) {
        return TFuture<TMutationResponse>();
    }

    if (!request.MutationId) {
        return TFuture<TMutationResponse>();
    }

    auto asyncResponseData = Options_.ResponseKeeper->TryBeginRequest(request.MutationId, request.Retry);
    if (!asyncResponseData) {
        return TFuture<TMutationResponse>();
    }

    return asyncResponseData.Apply(BIND([] (const TSharedRefArray& data) {
        return TMutationResponse{
            EMutationResponseOrigin::ResponseKeeper,
            data
        };
    }));
}

const TDecoratedAutomaton::TPendingMutation& TDecoratedAutomaton::LogFollowerMutation(
    const TSharedRef& recordData,
    TFuture<void>* localFlushFuture)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(!RotatingChangelog_);

    TSharedRef mutationData;
    DeserializeMutationRecord(recordData, &MutationHeader_, &mutationData);

    auto version = LoggedVersion_.load();

    TMutationRequest request;
    request.Reign = MutationHeader_.reign();
    request.Type = std::move(*MutationHeader_.mutable_mutation_type());
    request.Data = std::move(mutationData);
    request.MutationId = FromProto<TMutationId>(MutationHeader_.mutation_id());

    const auto& pendingMutation = PendingMutations_.emplace(
        version,
        std::move(request),
        FromProto<TInstant>(MutationHeader_.timestamp()),
        MutationHeader_.random_seed(),
        MutationHeader_.prev_random_seed(),
        MutationHeader_.sequence_number(),
        nullptr);

    if (Changelog_) {
        auto future = Changelog_->Append(recordData);
        if (localFlushFuture) {
            *localFlushFuture = std::move(future);
        }
    }

    auto newLoggedVersion = version.Advance();
    YT_VERIFY(EpochContext_->ReachableVersion < newLoggedVersion);
    LoggedVersion_ = newLoggedVersion;

    return pendingMutation;
}

TFuture<TRemoteSnapshotParams> TDecoratedAutomaton::BuildSnapshot()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    CancelSnapshot(TError("Another snapshot is scheduled"));

    auto loggedVersion = GetLoggedVersion();

    YT_LOG_INFO("Snapshot scheduled (Version: %v)",
        loggedVersion);

    UpdateSnapshotBuildDeadline();
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

    YT_VERIFY(!RotatingChangelog_);
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
        int currentChangelogId = loggedVersion.SegmentId;
        int nextChangelogId = rotatedVersion.SegmentId;

        YT_LOG_INFO("Started flushing changelog (ChangelogId: %v)",
            currentChangelogId);
        WaitFor(Changelog_->Flush())
            .ThrowOnError();
        YT_LOG_INFO("Finished flushing changelog (ChangelogId: %v)",
            currentChangelogId);

        YT_VERIFY(loggedVersion.RecordId == Changelog_->GetRecordCount());

        if (NextChangelogFuture_ && NextChangelogFuture_.IsSet() && !NextChangelogFuture_.Get().IsOK()) {
            YT_LOG_INFO("Changelog preallocation failed, trying to open it once again (ChangelogId: %v)",
                nextChangelogId);
            NextChangelogFuture_.Reset();
        }

        if (!NextChangelogFuture_) {
            YT_LOG_INFO("Creating changelog (ChangelogId: %v)", nextChangelogId);
            NextChangelogFuture_ = EpochContext_->ChangelogStore->CreateChangelog(nextChangelogId);
        }

        YT_LOG_INFO("Waiting for changelog to open (ChangelogId: %v)",
            nextChangelogId);
        Changelog_ = WaitFor(NextChangelogFuture_)
            .ValueOrThrow();
        YT_LOG_INFO("Changelog opened (ChangelogId: %v)",
            nextChangelogId);

        NextChangelogFuture_.Reset();

        if (Config_->PreallocateChangelogs) {
            int preallocatedChangelogId = rotatedVersion.SegmentId + 1;
            YT_LOG_INFO("Started preallocating changelog (ChangelogId: %v)",
                preallocatedChangelogId);
            NextChangelogFuture_ =
                EpochContext_->ChangelogStore->CreateChangelog(preallocatedChangelogId)
                    .Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<IChangelogPtr>& changelogOrError) {
                        if (changelogOrError.IsOK()) {
                            YT_LOG_INFO("Finished preallocating changelog (ChangelogId: %v)",
                                preallocatedChangelogId);
                        } else {
                            YT_LOG_WARNING(changelogOrError, "Error preallocating changelog (ChangelogId: %v)",
                                preallocatedChangelogId);
                        }
                        return changelogOrError;
                    }));
        }
    }

    LoggedVersion_ = rotatedVersion;
    RecoveryRecordCount_ = 0;
    RecoveryDataSize_ = 0;

    YT_VERIFY(RotatingChangelog_);
    RotatingChangelog_ = false;

    YT_VERIFY(EpochContext_->ReachableVersion < LoggedVersion_);

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

            // Cf. YT-6908; see below.
            auto commitPromise = pendingMutation.LocalCommitPromise;

            TMutationContext mutationContext(
                AutomatonVersion_,
                pendingMutation.Request,
                pendingMutation.Timestamp,
                pendingMutation.RandomSeed,
                pendingMutation.PrevRandomSeed,
                pendingMutation.SequenceNumber);

            {
                auto traceContext = pendingMutation.TraceContext
                    ? NTracing::CreateChildTraceContext(
                        pendingMutation.TraceContext,
                        ConcatToString(AsStringBuf("HydraManager:"), pendingMutation.Request.Type))
                    : nullptr;
                NTracing::TTraceContextGuard traceContextGuard(std::move(traceContext));
                DoApplyMutation(&mutationContext);
            }

            if (commitPromise) {
                commitPromise.Set(TMutationResponse{
                    EMutationResponseOrigin::Commit,
                    mutationContext.GetResponseData()
                });
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

ui64 TDecoratedAutomaton::GetLastLoggedRandomSeed() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return PendingMutations_.empty() ? RandomSeed_.load() : PendingMutations_.back().RandomSeed;
}

i64 TDecoratedAutomaton::GetLastLoggedSequenceNumber() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return PendingMutations_.empty() ? SequenceNumber_.load() : PendingMutations_.back().SequenceNumber;
}

void TDecoratedAutomaton::RotateAutomatonVersionIfNeeded(TVersion mutationVersion)
{
    auto automatonVersion = GetAutomatonVersion();
    if (mutationVersion.SegmentId == automatonVersion.SegmentId) {
        YT_VERIFY(mutationVersion.RecordId == automatonVersion.RecordId);
    } else {
        YT_VERIFY(mutationVersion.SegmentId > automatonVersion.SegmentId);
        YT_VERIFY(mutationVersion.RecordId == 0);
        RotateAutomatonVersion(mutationVersion.SegmentId);
    }
}

void TDecoratedAutomaton::RotateAutomatonVersionAfterRecovery()
{
    RotateAutomatonVersion(LoggedVersion_.load().SegmentId);
}

void TDecoratedAutomaton::DoApplyMutation(TMutationContext* context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto automatonVersion = GetAutomatonVersion();

    // Cannot access the request after the handler has been invoked since the latter
    // could submit more mutations and cause #PendingMutations_ to be reallocated.
    // So we'd better make the needed copies right away.
    // Cf. YT-6908.
    const auto& request = context->Request();
    auto mutationId = request.MutationId;

    {
        TMutationContextGuard guard(context);
        Automaton_->ApplyMutation(context);
    }

    if (Options_.ResponseKeeper && mutationId && !context->GetResponseKeeperSuppressed()) {
        Options_.ResponseKeeper->EndRequest(mutationId, context->GetResponseData());
    }

    YT_LOG_FATAL_IF(
        RandomSeed_ != context->GetPrevRandomSeed(),
        "Mutation random seeds differ (AutomatonRandomSeed: %llx, MutationRandomSeed: %llx)",
        RandomSeed_.load(),
        context->GetPrevRandomSeed());
    RandomSeed_ = context->GetRandomSeed();
    AutomatonVersion_ = automatonVersion.Advance();

    ++SequenceNumber_;
    YT_LOG_FATAL_IF(
        SequenceNumber_ != context->GetSequenceNumber(),
        "Sequence numbers differ (AutomatonSequenceNumber: %llx, MutationSequenceNumber: %llx)",
        SequenceNumber_.load(),
        context->GetSequenceNumber());

    if (CommittedVersion_.load() < automatonVersion) {
        CommittedVersion_ = automatonVersion;
    }
}

EPeerState TDecoratedAutomaton::GetState() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return State_;
}

TEpochContextPtr TDecoratedAutomaton::GetEpochContext()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(EpochContextLock_);
    return EpochContext_;
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

ui64 TDecoratedAutomaton::GetRandomSeed() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RandomSeed_.load();
}

i64 TDecoratedAutomaton::GetSequenceNumber() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SequenceNumber_.load();
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

TInstant TDecoratedAutomaton::GetSnapshotBuildDeadline() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return SnapshotBuildDeadline_;
}

TVersion TDecoratedAutomaton::GetAutomatonVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return AutomatonVersion_.load();
}

void TDecoratedAutomaton::RotateAutomatonVersion(int segmentId)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YT_VERIFY(GetAutomatonVersion().SegmentId < segmentId);

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
    TWriterGuard guard(EpochContextLock_);
    YT_VERIFY(!EpochContext_);
    std::swap(epochContext, EpochContext_);
}

void TDecoratedAutomaton::CancelSnapshot(const TError& error)
{
    if (SnapshotParamsPromise_ && SnapshotParamsPromise_.ToFuture().Cancel(error)) {
        YT_LOG_INFO(error, "Snapshot canceled");
    }
    SnapshotParamsPromise_.Reset();
}

void TDecoratedAutomaton::StopEpoch()
{
    auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");

    while (!PendingMutations_.empty()) {
        auto& pendingMutation = PendingMutations_.front();
        if (pendingMutation.LocalCommitPromise) {
            pendingMutation.LocalCommitPromise.Set(error);
        }
        PendingMutations_.pop();
    }

    if (Options_.ResponseKeeper) {
        Options_.ResponseKeeper->CancelPendingRequests(error);
    }

    RotatingChangelog_ = false;
    Changelog_.Reset();
    NextChangelogFuture_.Reset();
    {
        TWriterGuard guard(EpochContextLock_);
        TEpochContextPtr epochContext;
        std::swap(epochContext, EpochContext_);
        guard.Release();
    }
    SnapshotVersion_ = TVersion();
    LoggedVersion_ = TVersion();
    CommittedVersion_ = TVersion();
    CancelSnapshot(error);
    RecoveryRecordCount_ = 0;
    RecoveryDataSize_ = 0;
    SnapshotBuildDeadline_ = TInstant::Max();
}

void TDecoratedAutomaton::UpdateLastSuccessfulSnapshotInfo(const TErrorOr<TRemoteSnapshotParams>& snapshotInfoOrError)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!snapshotInfoOrError.IsOK()) {
        return;
    }

    auto snapshotId = snapshotInfoOrError.Value().SnapshotId;
    LastSuccessfulSnapshotId_ = std::max(LastSuccessfulSnapshotId_.load(), snapshotId);
}

void TDecoratedAutomaton::UpdateSnapshotBuildDeadline()
{
    SnapshotBuildDeadline_ =
        TInstant::Now() +
        Config_->SnapshotBuildPeriod +
        RandomDuration(Config_->SnapshotBuildSplay);
}

void TDecoratedAutomaton::MaybeStartSnapshotBuilder()
{
    if (GetAutomatonVersion() != SnapshotVersion_) {
        return;
    }

    auto builder =
        // XXX(babenko): ASAN + fork = possible deadlock; cf. https://st.yandex-team.ru/DEVTOOLS-5425
#ifdef _asan_enabled_
        false
#else
        Options_.UseFork
#endif
        ? TIntrusivePtr<TSnapshotBuilderBase>(New<TForkSnapshotBuilder>(this))
        : TIntrusivePtr<TSnapshotBuilderBase>(New<TNoForkSnapshotBuilder>(this));

    auto buildResult = builder->Run();
    buildResult.Subscribe(
        BIND(&TDecoratedAutomaton::UpdateLastSuccessfulSnapshotInfo, MakeWeak(this))
        .Via(AutomatonInvoker_));

    SnapshotParamsPromise_.SetFrom(buildResult);
}

bool TDecoratedAutomaton::IsRecovery()
{
    return
        State_ == EPeerState::LeaderRecovery ||
        State_ == EPeerState::FollowerRecovery;
}

bool TDecoratedAutomaton::IsBuildingSnapshotNow() const
{
    return BuildingSnapshot_.load();
}

int TDecoratedAutomaton::GetLastSuccessfulSnapshotId() const
{
    return LastSuccessfulSnapshotId_.load();
}

void TDecoratedAutomaton::SetLastLeadingSegmentId(int segmentId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LastLeadingSegmentId_ = segmentId;
}

int TDecoratedAutomaton::GetLastLeadingSegmentId() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return LastLeadingSegmentId_;
}

TReign TDecoratedAutomaton::GetCurrentReign() const
{
    return Automaton_->GetCurrentReign();
}

EFinalRecoveryAction TDecoratedAutomaton::GetFinalRecoveryAction() const
{
    return Automaton_->GetFinalRecoveryAction();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
