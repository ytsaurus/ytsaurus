#include "decorated_automaton.h"

#include <yt/yt/server/lib/hydra_common/automaton.h>
#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/serialize.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>
#include <yt/yt/server/lib/hydra_common/state_hash_checker.h>

#include <yt/yt/server/lib/misc/fork_executor.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>
#include <yt/yt/ytlib/hydra/proto/hydra_service.pb.h>

#include <yt/yt/core/actions/invoker_detail.h>
#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/library/process/pipe.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/logger_owner.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <util/random/random.h>

#include <util/system/file.h>

#include <util/system/spinlock.h>

#include <algorithm> // for std::max

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NElection;
using namespace NHydra::NProto;
using namespace NLogging;
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

class TChangelogDiscarder
    : public IChangelogDiscarder
{
public:
    explicit TChangelogDiscarder(const TLogger& logger)
        : Logger(logger)
    { }

    void CloseChangelog(TFuture<IChangelogPtr> changelogFuture, int changelogId) override
    {
        if (!changelogFuture) {
            return;
        }

        changelogFuture.Subscribe(BIND([this, this_ = MakeStrong(this), changelogId] (const TErrorOr<IChangelogPtr>& changelogOrError) {
            if (changelogOrError.IsOK()) {
                CloseChangelog(changelogOrError.Value(), changelogId);
            } else {
                YT_LOG_INFO(changelogOrError,
                    "Changelog allocation failed but it is already discarded, ignored (ChangelogId: %v)",
                    changelogId);
            }
        }));
    }

    void CloseChangelog(const IChangelogPtr& changelog, int changelogId) override
    {
        if (!changelog) {
            return;
        }

        // NB: Changelog is captured into a closure to prevent
        // its destruction before closing.
        changelog->Close()
            .Subscribe(BIND(&TChangelogDiscarder::OnChangelogDiscarded, MakeStrong(this), changelog, changelogId));
    }

private:
    const TLogger Logger;

    void OnChangelogDiscarded(IChangelogPtr /*changelog*/, int changelogId, const TError& error)
    {
        if (error.IsOK()) {
            YT_LOG_DEBUG("Changelog closed successfully (ChangelogId: %v)",
                changelogId);
        } else {
            YT_LOG_WARNING(error, "Failed to close changelog (ChangelogId: %v)",
                changelogId);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TSystemInvoker
    : public TInvokerWrapper
{
public:
    explicit TSystemInvoker(TDecoratedAutomaton* decoratedAutomaton)
        : TInvokerWrapper(decoratedAutomaton->AutomatonInvoker_)
        , Owner_(decoratedAutomaton)
    { }

    void Invoke(TClosure callback) override
    {
        auto lockGuard = TSystemLockGuard::Acquire(Owner_);

        auto doInvoke = [this, this_ = MakeStrong(this), callback = std::move(callback)] (TSystemLockGuard /*lockGuard*/) {
            TCurrentInvokerGuard currentInvokerGuard(this);
            callback();
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

    void Invoke(TClosure callback) override
    {
        auto lockGuard = TUserLockGuard::TryAcquire(Owner_);
        if (!lockGuard)
            return;

        auto doInvoke = [=, this, this_ = MakeStrong(this), callback = std::move(callback)] () {
            if (Owner_->GetState() != EPeerState::Leading &&
                Owner_->GetState() != EPeerState::Following)
            {
                return;
            }

            TCurrentInvokerGuard guard(this);
            callback();
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
        , SnapshotReadOnly_(Owner_->SnapshotReadOnly_)
        , RandomSeed_(Owner_->RandomSeed_)
        , StateHash_(Owner_->StateHash_)
        , Timestamp_(Owner_->Timestamp_)
        , EpochContext_(Owner_->EpochContext_)
    {
        Logger = Owner_->Logger.WithTag("SnapshotId: %v", SnapshotId_);
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
            meta.set_state_hash(StateHash_);
            meta.set_timestamp(Timestamp_.GetValue());

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
    const bool SnapshotReadOnly_;
    const ui64 RandomSeed_;
    const ui64 StateHash_;
    const TInstant Timestamp_;
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

        TRemoteSnapshotParams remoteParams{
            .PeerId = EpochContext_->CellManager->GetSelfPeerId(),
            .SnapshotId = SnapshotId_,
            .SnapshotReadOnly = SnapshotReadOnly_,
        };
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
    TForkSnapshotBuilder(TDecoratedAutomatonPtr owner, TForkCountersPtr counters)
        : TSnapshotBuilderBase(owner)
        , TForkExecutor(std::move(counters))
    { }

private:
    IAsyncInputStreamPtr InputStream_;
    std::unique_ptr<TFile> OutputFile_;

    TFuture<void> AsyncTransferResult_;


    TFuture<void> DoRun() override
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

    TDuration GetTimeout() const override
    {
        return Owner_->Config_->SnapshotBuildTimeout;
    }

    TDuration GetForkTimeout() const override
    {
        return Owner_->Config_->SnapshotForkTimeout;
    }

    void RunChild() override
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

    void RunParent() override
    {
        OutputFile_->Close();
    }

    void Cleanup() override
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
        auto guard = Guard(SpinLock_);
        SuspendedPromise_ = NewPromise<void>();
    }

    void ResumeAsAsync(IAsyncOutputStreamPtr underlyingStream)
    {
        auto guard = Guard(SpinLock_);
        auto suspendedPromise = SuspendedPromise_;
        SuspendedPromise_.Reset();
        UnderlyingStream_ = CreateZeroCopyAdapter(underlyingStream);
        for (const auto& syncBlock : SyncBlocks_) {
            YT_UNUSED_FUTURE(ForwardBlock(syncBlock));
        }
        SyncBlocks_.clear();
        guard.Release();
        suspendedPromise.Set();
    }

    void Abort()
    {
        auto guard = Guard(SpinLock_);
        auto suspendedPromise = SuspendedPromise_;
        guard.Release();

        if (suspendedPromise) {
            suspendedPromise.TrySet(TError("Snapshot writer aborted"));
        }
    }

    TFuture<void> Close() override
    {
        auto guard = Guard(SpinLock_);
        return LastForwardResult_;
    }

    TFuture<void> Write(const TSharedRef& block) override
    {
        // NB: We are not allowed to store by-ref copies of #block, cf. #IAsyncOutputStream::Write.
        struct TBlockTag { };
        auto blockCopy = TSharedRef::MakeCopy<TBlockTag>(block);

        auto guard = Guard(SpinLock_);
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

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
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


    TFuture<void> DoRun() override
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
    TStateHashCheckerPtr stateHashChecker,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : Logger(logger)
    , Config_(std::move(config))
    , Options_(options)
    , Automaton_(std::move(automaton))
    , AutomatonInvoker_(std::move(automatonInvoker))
    , DefaultGuardedUserInvoker_(CreateGuardedUserInvoker(AutomatonInvoker_))
    , ControlInvoker_(std::move(controlInvoker))
    , SystemInvoker_(New<TSystemInvoker>(this))
    , SnapshotStore_(std::move(snapshotStore))
    , StateHashChecker_(std::move(stateHashChecker))
    , ChangelogDiscarder_(New<TChangelogDiscarder>(Logger))
    , BatchCommitTimer_(profiler.Timer("/batch_commit_time"))
    , SnapshotLoadTime_(profiler.TimeGauge("/snapshot_load_time"))
    , ForkCounters_(New<TForkCounters>(profiler))
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
    AutomatonInvoker_->Invoke(BIND([=, this, this_ = MakeStrong(this)] {
        THydraContext hydraContext(
            TVersion(),
            TInstant::Zero(),
            /*randomSeed*/ 0,
            /*isMutationLoggingEnabled*/ true);
        THydraContextGuard hydraContextGuard(&hydraContext);

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
    // No affinity annotation here since this could have been called
    // from a forked process.

    // Context switches are not allowed during sync phase.
    TForbidContextSwitchGuard contextSwitchGuard;

    return Automaton_->SaveSnapshot(writer);
}

void TDecoratedAutomaton::LoadSnapshot(
    int snapshotId,
    TVersion version,
    i64 sequenceNumber,
    ui64 randomSeed,
    ui64 stateHash,
    TInstant timestamp,
    IAsyncZeroCopyInputStreamPtr reader)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_LOG_INFO("Started loading snapshot (SnapshotId: %v, Version: %v)",
        snapshotId,
        version);

    TWallTimer timer;
    auto finally = Finally([&] {
        SnapshotLoadTime_.Update(timer.GetElapsedTime());
    });

    Automaton_->Clear();
    try {
        AutomatonVersion_ = CommittedVersion_ = TVersion();
        RandomSeed_ = 0;
        SequenceNumber_ = 0;
        StateHash_ = 0;
        Timestamp_ = {};
        {
            Automaton_->LoadSnapshot(reader);

            // Snapshot preparation is a "mutation" that is executed before first mutation
            // in changelog.
            TVersion hydraContextVersion(snapshotId, -1);
            // NB: #randomSeed is used as a random seed for the first mutation
            // in changelog, so ad-hoc seed is used here.
            auto hydraContextRandomSeed = randomSeed;
            HashCombine(hydraContextRandomSeed, snapshotId);

            THydraContext hydraContext(
                hydraContextVersion,
                timestamp,
                hydraContextRandomSeed,
                /*isMutationLoggingEnabled*/ true);
            THydraContextGuard hydraContextGuard(&hydraContext);

            Automaton_->PrepareState();
        }
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

    YT_LOG_INFO("Finished loading snapshot");

    // YT-3926
    AutomatonVersion_ = CommittedVersion_ = TVersion(snapshotId, 0);
    RandomSeed_ = randomSeed;
    SequenceNumber_ = sequenceNumber;
    StateHash_ = stateHash;
    Timestamp_ = timestamp;
}

void TDecoratedAutomaton::CheckInvariants()
{
    YT_LOG_INFO("Invariants check started");

    Automaton_->CheckInvariants();

    YT_LOG_INFO("Invariants check completed");
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

    TMutationContext mutationContext(
        AutomatonVersion_,
        &request,
        FromProto<TInstant>(header.timestamp()),
        header.random_seed(),
        header.prev_random_seed(),
        header.sequence_number(),
        StateHash_,
        header.term(),
        IsMutationLoggingEnabled());

    DoApplyMutation(&mutationContext);
}

const TDecoratedAutomaton::TPendingMutation& TDecoratedAutomaton::LogLeaderMutation(
    TInstant timestamp,
    std::unique_ptr<TMutationRequest> request,
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
        GetLastLoggedSequenceNumber() + 1);

    MutationHeader_.Clear(); // don't forget to cleanup the pooled instance
    MutationHeader_.set_reign(pendingMutation.Request->Reign);
    MutationHeader_.set_mutation_type(pendingMutation.Request->Type);
    MutationHeader_.set_timestamp(pendingMutation.Timestamp.GetValue());
    MutationHeader_.set_random_seed(pendingMutation.RandomSeed);
    MutationHeader_.set_segment_id(pendingMutation.Version.SegmentId);
    MutationHeader_.set_record_id(pendingMutation.Version.RecordId);
    MutationHeader_.set_prev_random_seed(pendingMutation.PrevRandomSeed);
    MutationHeader_.set_sequence_number(pendingMutation.SequenceNumber);
    if (pendingMutation.Request->MutationId) {
        ToProto(MutationHeader_.mutable_mutation_id(), pendingMutation.Request->MutationId);
    }

    *recordData = SerializeMutationRecord(MutationHeader_, pendingMutation.Request->Data);
    *localFlushFuture = Changelog_->Append({*recordData});

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

    auto request = std::make_unique<TMutationRequest>();
    request->Reign = MutationHeader_.reign();
    request->Type = std::move(*MutationHeader_.mutable_mutation_type());
    request->Data = std::move(mutationData);
    request->MutationId = FromProto<TMutationId>(MutationHeader_.mutation_id());

    const auto& pendingMutation = PendingMutations_.emplace(
        version,
        std::move(request),
        FromProto<TInstant>(MutationHeader_.timestamp()),
        MutationHeader_.random_seed(),
        MutationHeader_.prev_random_seed(),
        MutationHeader_.sequence_number());

    if (Changelog_) {
        auto future = Changelog_->Append({recordData});
        if (localFlushFuture) {
            *localFlushFuture = std::move(future);
        }
    }

    auto newLoggedVersion = version.Advance();
    YT_VERIFY(EpochContext_->ReachableVersion < newLoggedVersion);
    LoggedVersion_ = newLoggedVersion;

    return pendingMutation;
}

TFuture<TRemoteSnapshotParams> TDecoratedAutomaton::BuildSnapshot(bool readOnly)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    CancelSnapshot(TError("Another snapshot is scheduled"));

    auto loggedVersion = GetLoggedVersion();

    YT_LOG_INFO("Snapshot scheduled (Version: %v)",
        loggedVersion);

    UpdateSnapshotBuildDeadline();
    SnapshotVersion_ = loggedVersion;
    SnapshotReadOnly_ = readOnly;
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
            NextChangelogFuture_ = EpochContext_->ChangelogStore->CreateChangelog(nextChangelogId, /* meta */{});
        }

        if (Config_->CloseChangelogs) {
            ChangelogDiscarder_->CloseChangelog(Changelog_, currentChangelogId);
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
                EpochContext_->ChangelogStore->CreateChangelog(preallocatedChangelogId, /* meta */{})
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<IChangelogPtr>& changelogOrError) {
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
    TEventTimerGuard timerGuard(BatchCommitTimer_);
    while (!PendingMutations_.empty()) {
        auto& pendingMutation = PendingMutations_.front();
        if (pendingMutation.Version >= CommittedVersion_) {
            break;
        }

        RotateAutomatonVersionIfNeeded(pendingMutation.Version);

        // Cannot access #pendingMutation after the handler has been invoked since the latter
        // could submit more mutations and cause #PendingMutations_ to be reallocated.
        // So we'd better make the needed copies right away.
        // Cf. YT-6908.
        auto commitPromise = pendingMutation.LocalCommitPromise;

        TMutationContext mutationContext(
            AutomatonVersion_,
            pendingMutation.Request.get(),
            pendingMutation.Timestamp,
            pendingMutation.RandomSeed,
            pendingMutation.PrevRandomSeed,
            pendingMutation.SequenceNumber,
            StateHash_,
            /*term*/ 0,
            IsMutationLoggingEnabled());

        {
            NTracing::TTraceContextGuard traceContextGuard(mutationContext.Request().TraceContext);
            DoApplyMutation(&mutationContext);
        }

        PendingMutations_.pop();

        if (commitPromise) {
            commitPromise.Set(TMutationResponse{
                EMutationResponseOrigin::Commit,
                mutationContext.GetResponseData()
            });
        }

        MaybeStartSnapshotBuilder();

        if (mayYield && timer.GetElapsedTime() > Config_->MaxCommitBatchDuration) {
            EpochContext_->EpochUserAutomatonInvoker->Invoke(
                BIND(&TDecoratedAutomaton::ApplyPendingMutations, MakeStrong(this), true));
            break;
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

void TDecoratedAutomaton::DoApplyMutation(TMutationContext* mutationContext)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto automatonVersion = GetAutomatonVersion();

    auto mutationId = mutationContext->Request().MutationId;
    {
        TMutationContextGuard mutationContextGuard(mutationContext);
        Automaton_->ApplyMutation(mutationContext);

        if (Options_.ResponseKeeper &&
            mutationId &&
            !mutationContext->GetResponseKeeperSuppressed() &&
            mutationContext->GetResponseData()) // Null when mutation idempotizer kicks in.
        {
            if (auto setResponseKeeperPromise =
                Options_.ResponseKeeper->EndRequest(mutationId, mutationContext->GetResponseData()))
            {
                setResponseKeeperPromise();
            }
        }
    }

    mutationContext->CombineStateHash(mutationContext->GetRandomSeed());
    StateHash_ = mutationContext->GetStateHash();

    Timestamp_ = mutationContext->GetTimestamp();

    YT_LOG_FATAL_IF(
        RandomSeed_ != mutationContext->GetPrevRandomSeed(),
        "Mutation random seeds differ (AutomatonRandomSeed: %x, MutationRandomSeed: %x)",
        RandomSeed_.load(),
        mutationContext->GetPrevRandomSeed());
    RandomSeed_ = mutationContext->GetRandomSeed();
    AutomatonVersion_ = automatonVersion.Advance();

    ++SequenceNumber_;
    YT_LOG_FATAL_IF(
        SequenceNumber_ != mutationContext->GetSequenceNumber(),
        "Sequence numbers differ (AutomatonSequenceNumber: %x, MutationSequenceNumber: %x)",
        SequenceNumber_.load(),
        mutationContext->GetSequenceNumber());

    if (CommittedVersion_.load() < automatonVersion) {
        CommittedVersion_ = automatonVersion;
    }

    if (Config_->EnableStateHashChecker) {
        StateHashChecker_->Report(SequenceNumber_.load(), StateHash_);
    }

    if (auto invariantsCheckProbability = Config_->InvariantsCheckProbability) {
        if (RandomNumber<double>() <= invariantsCheckProbability) {
            CheckInvariants();
        }
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

    auto guard = ReaderGuard(EpochContextLock_);
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

ui64 TDecoratedAutomaton::GetStateHash() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StateHash_.load();
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
    auto guard = WriterGuard(EpochContextLock_);
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

    if (Config_->CloseChangelogs) {
        auto currentChangelogId = LoggedVersion_.load().SegmentId;
        ChangelogDiscarder_->CloseChangelog(Changelog_, currentChangelogId);
        ChangelogDiscarder_->CloseChangelog(NextChangelogFuture_, currentChangelogId + 1);
    }

    Changelog_.Reset();
    NextChangelogFuture_.Reset();
    {
        auto guard = WriterGuard(EpochContextLock_);
        TEpochContextPtr epochContext;
        std::swap(epochContext, EpochContext_);
        guard.Release();
    }
    SnapshotVersion_ = TVersion();
    SnapshotReadOnly_ = false;
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

    const auto& snapshotInfo = snapshotInfoOrError.Value();
    auto snapshotId = snapshotInfo.SnapshotId;
    if (snapshotId > LastSuccessfulSnapshotId_.load()) {
        LastSuccessfulSnapshotId_ = snapshotId;
        LastSuccessfulSnapshotReadOnly_ = snapshotInfo.SnapshotReadOnly;
    }
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
        ? TIntrusivePtr<TSnapshotBuilderBase>(New<TForkSnapshotBuilder>(this, ForkCounters_))
        : TIntrusivePtr<TSnapshotBuilderBase>(New<TNoForkSnapshotBuilder>(this));

    auto buildResult = builder->Run();
    buildResult.Subscribe(
        BIND(&TDecoratedAutomaton::UpdateLastSuccessfulSnapshotInfo, MakeWeak(this))
        .Via(AutomatonInvoker_));

    SnapshotParamsPromise_.SetFrom(buildResult);
}

bool TDecoratedAutomaton::IsRecovery() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        State_ == EPeerState::LeaderRecovery ||
        State_ == EPeerState::FollowerRecovery;
}

bool TDecoratedAutomaton::IsMutationLoggingEnabled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return !IsRecovery() || Config_->ForceMutationLogging;
}

bool TDecoratedAutomaton::IsBuildingSnapshotNow() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BuildingSnapshot_.load();
}

int TDecoratedAutomaton::GetLastSuccessfulSnapshotId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return LastSuccessfulSnapshotId_.load();
}

bool TDecoratedAutomaton::GetLastSuccessfulSnapshotReadOnly() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return LastSuccessfulSnapshotReadOnly_;
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
