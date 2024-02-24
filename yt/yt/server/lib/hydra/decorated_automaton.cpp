#include "decorated_automaton.h"

#include "automaton.h"
#include "changelog.h"
#include "config.h"
#include "helpers.h"
#include "serialize.h"
#include "snapshot.h"
#include "state_hash_checker.h"

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
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/logger_owner.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/process/pipe.h>

#include <library/cpp/yt/logging/backends/stream/stream_log_manager.h>

#include <util/random/random.h>

#include <util/system/file.h>
#include <util/system/spinlock.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NElection;
using namespace NLogging;
using namespace NPipes;
using namespace NProfiling;
using namespace NRpc;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const i64 SnapshotTransferBlockSize = 1_MB;
static const i64 LogTransferBlockSize = 1_KB;

////////////////////////////////////////////////////////////////////////////////

TPendingMutation::TPendingMutation(
    TVersion version,
    TMutationRequest&& request,
    TInstant timestamp,
    ui64 randomSeed,
    ui64 prevRandomSeed,
    i64 sequenceNumber,
    int term,
    TSharedRef serializedMutation,
    TPromise<TMutationResponse> promise)
    : Version(version)
    , Request(request)
    , Timestamp(timestamp)
    , RandomSeed(randomSeed)
    , PrevRandomSeed(prevRandomSeed)
    , SequenceNumber(sequenceNumber)
    , Term(term)
    , RecordData(serializedMutation)
    , LocalCommitPromise(std::move(promise))
{ }

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
    VERIFY_THREAD_AFFINITY_ANY();

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
    VERIFY_THREAD_AFFINITY_ANY();

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
    explicit TSystemInvoker(
        TDecoratedAutomatonPtr decoratedAutomaton)
        : TInvokerWrapper(decoratedAutomaton->AutomatonInvoker_)
        , Owner_(decoratedAutomaton)
    { }

    void Invoke(TClosure callback) override
    {
        auto owner = Owner_.Lock();
        if (!owner) {
            return;
        }

        auto lockGuard = TSystemLockGuard::Acquire(owner);

        auto doInvoke = [this, this_ = MakeStrong(this), callback = std::move(callback)] (TSystemLockGuard /*lockGuard*/) {
            TCurrentInvokerGuard currentInvokerGuard(this);
            callback();
        };

        UnderlyingInvoker_->Invoke(BIND(doInvoke, Passed(std::move(lockGuard))));
    }

private:
    const TWeakPtr<TDecoratedAutomaton> Owner_;
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
        auto owner = Owner_.Lock();
        if (!owner) {
            return;
        }

        auto lockGuard = TUserLockGuard::TryAcquire(owner);
        if (!lockGuard) {
            return;
        }

        auto doInvoke = [=, this, this_ = MakeStrong(this), callback = std::move(callback)] () {
            auto state = owner->GetState();
            if (state != EPeerState::Leading && state != EPeerState::Following) {
                return;
            }

            TCurrentEpochIdGuard epochIdGuard(owner->GetEpochId());
            TCurrentInvokerGuard invokerGuard(this);
            callback();
        };

        UnderlyingInvoker_->Invoke(BIND(doInvoke));
    }

private:
    const TWeakPtr<TDecoratedAutomaton> Owner_;
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
        , SnapshotId_(Owner_->NextSnapshotId_)
        , SnapshotReadOnly_(Owner_->NextSnapshotReadOnly_)
        , RandomSeed_(Owner_->RandomSeed_)
        , StateHash_(Owner_->StateHash_)
        , Timestamp_(Owner_->Timestamp_)
        , SelfPeerId_(owner->GetEpochContext()->CellManager->GetSelfPeerId())
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

            NHydra::NProto::TSnapshotMeta meta;
            meta.set_sequence_number(SequenceNumber_);
            meta.set_random_seed(RandomSeed_);
            meta.set_state_hash(StateHash_);
            meta.set_timestamp(Timestamp_.GetValue());
            auto automatonVersion = Owner_->AutomatonVersion_.load();
            meta.set_last_segment_id(automatonVersion.SegmentId);
            meta.set_last_record_id(automatonVersion.RecordId);
            meta.set_last_mutation_term(Owner_->LastMutationTerm_);
            meta.set_read_only(SnapshotReadOnly_);

            SnapshotWriter_ = Owner_->SnapshotStore_->CreateWriter(SnapshotId_, meta);

            return DoRun().Apply(
                BIND(&TSnapshotBuilderBase::OnFinished, MakeStrong(this))
                    .AsyncVia(Owner_->ControlInvoker_));
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
    const int SelfPeerId_;

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
            auto delay = Owner_->Config_->Get()->BuildSnapshotDelay;
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
            .PeerId = SelfPeerId_,
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
    class TCommunicationChannel
    {
    public:
        TCommunicationChannel(
            const NLogging::TLogger& Logger,
            const TString& kind)
        {
            Pipe_ = TPipeFactory().Create();
            YT_LOG_INFO("Communication channel created (Kind: %v, Pipe: %v)",
                kind,
                Pipe_);

            InputStream_ = Pipe_.CreateAsyncReader();
            OutputFile_ = std::make_unique<TFile>(FHANDLE(Pipe_.ReleaseWriteFD()));
        }

        const TPipe& GetPipe() const
        {
            return Pipe_;
        }

        const IAsyncInputStreamPtr& GetInputStream() const
        {
            return InputStream_;
        }

        TFile* GetOutputFile() const
        {
            return OutputFile_.get();
        }

    private:
        TPipe Pipe_;
        IAsyncInputStreamPtr InputStream_;
        std::unique_ptr<TFile> OutputFile_;
    };

    std::unique_ptr<TCommunicationChannel> SnapshotChannel_;
    std::unique_ptr<TCommunicationChannel> LogChannel_;


    TFuture<void> SnapshotTransferFuture_;
    TFuture<void> LogTransferFuture_;


    TFuture<void> DoRun() override
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        SnapshotChannel_ = std::make_unique<TCommunicationChannel>(Logger, "Snapshot");
        LogChannel_ = std::make_unique<TCommunicationChannel>(Logger, "Log");

        SnapshotTransferFuture_ = BIND(&TForkSnapshotBuilder::SnapshotTransferLoop, MakeStrong(this))
            .AsyncVia(GetWatchdogInvoker())
            .Run();
        LogTransferFuture_ = BIND(&TForkSnapshotBuilder::LogTransferLoop, MakeStrong(this))
            .AsyncVia(GetWatchdogInvoker())
            .Run();

        return Fork().Apply(
            BIND(&TForkSnapshotBuilder::OnFinished, MakeStrong(this))
                .AsyncVia(Owner_->ControlInvoker_));
    }

    TDuration GetTimeout() const override
    {
        return Owner_->Config_->Get()->SnapshotBuildTimeout;
    }

    TDuration GetForkTimeout() const override
    {
        return Owner_->Config_->Get()->SnapshotForkTimeout;
    }

    void RunChild() override
    {
        CloseAllDescriptors({
            STDERR_FILENO,
            static_cast<int>(SnapshotChannel_->GetOutputFile()->GetHandle()),
            static_cast<int>(LogChannel_->GetOutputFile()->GetHandle()),
        });

        TUnbufferedFileOutput snapshotOutput(*SnapshotChannel_->GetOutputFile());
        auto writer = CreateAsyncAdapter(&snapshotOutput);

        TUnbufferedFileOutput logOutput(*LogChannel_->GetOutputFile());
        auto streamLogManager = NLogging::CreateStreamLogManager(&logOutput);
        auto logger = NLogging::TLogger(streamLogManager.get(), Logger.GetCategory()->Name);

        TSnapshotSaveContext context{
            .Writer = std::move(writer),
            .Logger = std::move(logger),
        };

        const auto& Logger = context.Logger;
        YT_LOG_INFO("Child process forked");

        Owner_->SaveSnapshot(context)
            .Get()
            .ThrowOnError();

        YT_LOG_INFO("Child process is exiting");

        CloseChannelOutputs();
    }

    void RunParent() override
    {
        CloseChannelOutputs();
    }

    void CloseChannelOutputs()
    {
        SnapshotChannel_->GetOutputFile()->Close();
        LogChannel_->GetOutputFile()->Close();
    }

    void Cleanup() override
    {
        ReleaseLock();
    }

    void SnapshotTransferLoop()
    {
        YT_LOG_INFO("Snapshot transfer loop started");

        WaitFor(SnapshotWriter_->Open())
            .ThrowOnError();

        auto zeroCopyReader = CreateZeroCopyAdapter(SnapshotChannel_->GetInputStream(), SnapshotTransferBlockSize);
        auto zeroCopyWriter = CreateZeroCopyAdapter(SnapshotWriter_);

        TFuture<void> lastWriteResult;
        i64 size = 0;

        while (true) {
            auto block = WaitFor(zeroCopyReader->Read())
                .ValueOrThrow();

            if (!block) {
                break;
            }

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

    void LogTransferLoop()
    {
        YT_LOG_INFO("Log transfer loop started");

        auto zeroCopyReader = CreateZeroCopyAdapter(LogChannel_->GetInputStream(), LogTransferBlockSize);

        TString message;

        while (true) {
            auto block = WaitFor(zeroCopyReader->Read())
                .ValueOrThrow();

            if (!block) {
                break;
            }

            message += ToString(block);

            while (true) {
                auto newlineIndex = message.find('\n');
                if (newlineIndex == TString::npos) {
                    break;
                }
                LogMessageFromChild(TStringBuf(message.begin(), newlineIndex));
                message = message.substr(newlineIndex + 1);
            }
        }

        LogMessageFromChild(message);

        YT_LOG_INFO("Log transfer loop completed");
    }

    void LogMessageFromChild(TStringBuf message)
    {
        if (!message.empty()) {
            YT_LOG_INFO("Message from child: %v", message);
        }
    }

    void OnFinished()
    {
        YT_LOG_INFO("Waiting for transfer loop to finish");
        WaitFor(SnapshotTransferFuture_)
            .ThrowOnError();
        YT_LOG_INFO("Transfer loop finished");

        YT_LOG_INFO("Waiting for log loop to finish");
        WaitFor(LogTransferFuture_)
            .ThrowOnError();
        YT_LOG_INFO("Transfer log finished");

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

    TFuture<void> OpenWriterFuture_;
    TFuture<void> SaveSnapshotFuture_;


    TFuture<void> DoRun() override
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        SwitchableSnapshotWriter_ = New<TSwitchableSnapshotWriter>(Logger);

        OpenWriterFuture_ = SnapshotWriter_->Open();

        YT_LOG_INFO("Snapshot sync phase started");

        TSnapshotSaveContext context{
            .Writer = SwitchableSnapshotWriter_,
            .Logger = Logger,
        };
        SaveSnapshotFuture_ = Owner_->SaveSnapshot(context);

        YT_LOG_INFO("Snapshot sync phase completed");

        SwitchableSnapshotWriter_->Suspend();

        return BIND(&TNoForkSnapshotBuilder::DoRunAsync, MakeStrong(this))
            .AsyncVia(Owner_->ControlInvoker_)
            .Run();
    }

    void DoRunAsync()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        WaitFor(OpenWriterFuture_)
            .ThrowOnError();

        YT_LOG_INFO("Switching to async snapshot writer");

        SwitchableSnapshotWriter_->ResumeAsAsync(SnapshotWriter_);

        WaitFor(SaveSnapshotFuture_)
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

struct TDecoratedAutomaton::TMutationApplicationResult
{
    // Null during recovery.
    TPromise<TMutationResponse> LocalCommitPromise;
    TMutationId MutationId;
    TSharedRefArray ResponseData;
    // May be null if response keeper says so (or if it's disabled, suppressed etc.)
    std::function<void()> ResponseKeeperPromiseSetter;
};

////////////////////////////////////////////////////////////////////////////////

TDecoratedAutomaton::TDecoratedAutomaton(
    TConfigWrapperPtr config,
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
    ResetState();
}

void TDecoratedAutomaton::ResetState()
{
    VERIFY_THREAD_AFFINITY_ANY();

    AutomatonInvoker_->Invoke(BIND([=, this, this_ = MakeStrong(this)] {
        THydraContext hydraContext(
            TVersion(),
            TInstant::Zero(),
            /*randomSeed*/ 0,
            /*localHostNameOverride*/ TSharedRef::FromString("<unknown>"));
        THydraContextGuard hydraContextGuard(&hydraContext);

        ClearState();
        Automaton_->SetZeroState();
    }));
}

void TDecoratedAutomaton::ClearState()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    AutomatonVersion_ = TVersion();
    RandomSeed_ = 0;
    SequenceNumber_ = 0;
    ReliablyAppliedSequenceNumber_ = 0;
    StateHash_ = 0;
    Timestamp_ = {};

    Automaton_->Clear();
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

TFuture<void> TDecoratedAutomaton::SaveSnapshot(const TSnapshotSaveContext& context)
{
    // No affinity annotation here since this could have been called
    // from a forked process.

    // Context switches are not allowed during sync phase.
    TForbidContextSwitchGuard contextSwitchGuard;

    const auto& Logger = context.Logger;
    YT_LOG_INFO("Started saving snapshot");

    return
        Automaton_->SaveSnapshot(context).Apply(
            BIND([Logger = Logger] (const TError& error) {
                if (error.IsOK()) {
                    YT_LOG_INFO("Snapshot saved successfully");
                } else {
                    YT_LOG_ERROR(error, "Error saving snapshot");
                    THROW_ERROR(error);
                }
            }));
}

void TDecoratedAutomaton::LoadSnapshot(
    int snapshotId,
    int lastMutationTerm,
    TVersion version,
    i64 sequenceNumber,
    bool readOnly,
    ui64 randomSeed,
    ui64 stateHash,
    TInstant timestamp,
    IAsyncZeroCopyInputStreamPtr reader)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_LOG_INFO("Started loading snapshot (SnapshotId: %v)",
        snapshotId);

    TWallTimer timer;
    auto finally = Finally([&] {
        SnapshotLoadTime_.Update(timer.GetElapsedTime());
    });

    ClearState();

    try {
        TSnapshotLoadContext context{
            .Reader = reader,
        };
        Automaton_->LoadSnapshot(context);

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
            SanitizedLocalHostName_);
        THydraContextGuard hydraContextGuard(&hydraContext);

        Automaton_->PrepareState();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Snapshot load failed; clearing state");
        ClearState();
        throw;
    } catch (const TFiberCanceledException&) {
        YT_LOG_INFO("Snapshot load fiber was canceled");
        throw;
    } catch (...) {
        YT_LOG_ERROR("Snapshot load failed with an unknown error");
        throw;
    }

    YT_LOG_INFO("Finished loading snapshot");

    AutomatonVersion_ = version;
    RandomSeed_ = randomSeed;
    SequenceNumber_ = sequenceNumber;
    ReliablyAppliedSequenceNumber_ = sequenceNumber;
    StateHash_ = stateHash;
    Timestamp_ = timestamp;
    // This protects us from building a snapshot with the same id twice.
    // If we join an active quorum and a leader is currently building a snapshot with id N,
    // we will be asked to recover to version (N - 1, M) may be using snapshot N (it might be already
    // built on some peers).
    // After recovery leader may still ask us to build snapshot N, but we already downloaded it from another peer,
    // so just refuse.
    LastSuccessfulSnapshotId_ = snapshotId;
    LastSuccessfulSnapshotReadOnly_ = readOnly;
    ReadOnly_ = readOnly;
    LastMutationTerm_ = lastMutationTerm;
    MutationCountSinceLastSnapshot_ = 0;
    MutationSizeSinceLastSnapshot_ = 0;
}

void TDecoratedAutomaton::CheckInvariants()
{
    YT_LOG_INFO("Invariants check started");

    Automaton_->CheckInvariants();

    YT_LOG_INFO("Invariants check completed");
}

void TDecoratedAutomaton::ApplyMutationsDuringRecovery(const std::vector<TSharedRef>& recordsData)
{
    std::vector<TMutationApplicationResult> results;
    results.reserve(recordsData.size());
    for (const auto& recordData : recordsData)  {
        results.push_back(ApplyMutationDuringRecovery(recordData));
    }

    // NB: may be offloaded to a different thread but probably not worth it.
    PublishMutationApplicationResults(std::move(results));
}

TDecoratedAutomaton::TMutationApplicationResult TDecoratedAutomaton::ApplyMutationDuringRecovery(const TSharedRef& recordData)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    NHydra::NProto::TMutationHeader header;
    TSharedRef requestData;
    DeserializeMutationRecord(recordData, &header, &requestData);

    auto mutationVersion = TVersion(header.segment_id(), header.record_id());

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
        SanitizedLocalHostName_);

    TFiberMinLogLevelGuard minLogLevelGuard(Config_->Get()->RecoveryMinLogLevel);

    TMutationApplicationResult result;
    DoApplyMutation(&mutationContext, mutationVersion, &result);
    result.MutationId = request.MutationId;
    result.ResponseData = mutationContext.TakeResponseData();
    // NB: result.LocalCommitPromise is left null.
    return result;
}

TFuture<TMutationResponse> TDecoratedAutomaton::TryBeginKeptRequest(const TMutationRequest& request)
{
    VERIFY_THREAD_AFFINITY_ANY();

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

TFuture<TRemoteSnapshotParams> TDecoratedAutomaton::BuildSnapshot(
    int snapshotId,
    i64 sequenceNumber,
    bool readOnly)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // TODO(aleksandra-zh): this should be considered a success.
    if (LastSuccessfulSnapshotId_ >= snapshotId) {
        TError error("Cannot build a snapshot %v because last built snapshot id %v is greater",
            snapshotId,
            LastSuccessfulSnapshotId_.load());
        YT_LOG_INFO(error, "Error building snapshot");
        return MakeFuture<TRemoteSnapshotParams>(error);
    }

    if (SequenceNumber_ > sequenceNumber) {
        TError error("Cannot build a snapshot %v from sequence number %v because automaton sequence number is greater %v",
            snapshotId,
            sequenceNumber,
            SequenceNumber_.load());
        YT_LOG_INFO(error, "Error building snapshot");
        return MakeFuture<TRemoteSnapshotParams>(error);
    }

    // We are already building this snapshot.
    if (NextSnapshotId_ == snapshotId) {
        YT_LOG_INFO("Snapshot is already being built (SnapshotId: %v)",
            NextSnapshotId_);
        return SnapshotParamsPromise_;
    }

    YT_VERIFY(NextSnapshotId_ < snapshotId);

    YT_LOG_INFO("Will build snapshot (SnapshotId: %v, SequenceNumber: %v, ReadOnly: %v)",
        snapshotId,
        sequenceNumber,
        readOnly);

    NextSnapshotSequenceNumber_ = sequenceNumber;
    NextSnapshotId_ = snapshotId;
    NextSnapshotReadOnly_ = readOnly;
    SnapshotParamsPromise_ = NewPromise<TRemoteSnapshotParams>();

    MaybeStartSnapshotBuilder();

    return SnapshotParamsPromise_;
}

void TDecoratedAutomaton::ApplyMutations(const std::vector<TPendingMutationPtr>& mutations)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    std::vector<TMutationApplicationResult> results;
    results.reserve(mutations.size());
    for (const auto& mutation : mutations) {
        results.push_back(ApplyMutation(mutation));
    }

    NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
        BIND(&TDecoratedAutomaton::PublishMutationApplicationResults, MakeStrong(this), Passed(std::move(results))));
}

void TDecoratedAutomaton::PublishMutationApplicationResults(std::vector<TMutationApplicationResult>&& results)
{
    VERIFY_THREAD_AFFINITY_ANY();

    for (const auto& result : results) {
        try {
            if (const auto& setPromises = result.ResponseKeeperPromiseSetter) {
                setPromises();
            }
        } catch (const std::exception& ex) { // COMPAT(shakurov): Just being paranoid.
            YT_LOG_ALERT(ex,
                "Finalizing request end has thrown (MutationId: %v)",
                result.MutationId);
        }

        try {
            if (const auto& promise = result.LocalCommitPromise) {
                promise.TrySet(TMutationResponse{
                    EMutationResponseOrigin::Commit,
                    result.ResponseData
                });
            }
        } catch (const std::exception& ex) { // COMPAT(shakurov): Just being paranoid.
            YT_LOG_ALERT(ex,
                "Setting a commit promise has thrown (MutationId: %v)",
                result.MutationId);
        }
    }
}

TSharedRef TDecoratedAutomaton::SanitizeLocalHostName() const
{
    auto localHost = ReadLocalHostName();
    if (Options_.EnableLocalHostSanitizing) {
        THashSet<TString> hosts;
        for (const auto& peer : GetEpochContext()->CellManager->GetClusterPeersAddresses()) {
            TStringBuf host;
            int port;
            ParseServiceAddress(peer, &host, &port);
            hosts.insert(TString(host));
        }

        if (auto sanitizedLocalHost = NHydra::SanitizeLocalHostName(hosts, localHost)) {
            YT_LOG_INFO("Local host name sanitized (Hosts: %v, LocalHost: %v, SanitizedLocalHost)",
                hosts,
                localHost,
                *sanitizedLocalHost);
            return *sanitizedLocalHost;
        }

        YT_LOG_ALERT("Failed to sanitize local host name, perhaps the hosts have different lengths (Hosts: %v, LocalHost: %v)",
            hosts,
            localHost);
    }
    return TSharedRef::FromString(ToString(localHost));
}

TDecoratedAutomaton::TMutationApplicationResult TDecoratedAutomaton::ApplyMutation(
    const TPendingMutationPtr& mutation)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    TForbidContextSwitchGuard contextSwitchGuard;

    TMutationContext mutationContext(
        AutomatonVersion_,
        &mutation->Request,
        mutation->Timestamp,
        mutation->RandomSeed,
        mutation->PrevRandomSeed,
        mutation->SequenceNumber,
        StateHash_,
        mutation->Term,
        SanitizedLocalHostName_);

    TMutationApplicationResult result;

    {
        NTracing::TTraceContextGuard traceContextGuard(mutation->Request.TraceContext);
        YT_VERIFY(ReliablyAppliedSequenceNumber_.load() < mutation->SequenceNumber);
        ReliablyAppliedSequenceNumber_ = mutation->SequenceNumber;
        DoApplyMutation(&mutationContext, mutation->Version, &result);
    }

    if (mutation->LocalCommitPromise) {
        YT_VERIFY(GetState() == EPeerState::Leading);
    } else {
        YT_VERIFY(GetState() == EPeerState::Following ||
            GetState() == EPeerState::FollowerRecovery);
    }

    // Mutation could remain alive for quite a while even after it has been applied at leader,
    // e.g. when some follower is down. The handler could be holding something heavy and needs to be dropped.
    mutation->Request.Handler.Reset();

    MaybeStartSnapshotBuilder();

    result.LocalCommitPromise = mutation->LocalCommitPromise;
    result.MutationId = mutation->Request.MutationId;
    result.ResponseData = mutationContext.TakeResponseData();

    return result;
}

void TDecoratedAutomaton::DoApplyMutation(
    TMutationContext* mutationContext,
    TVersion mutationVersion,
    TMutationApplicationResult* result)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto automatonVersion = GetAutomatonVersion();

    // Cannot access the request after the handler has been invoked since the latter
    // could submit more mutations and cause #PendingMutations_ to be reallocated.
    // So we'd better make the needed copies right away.
    // Cf. YT-6908.
    const auto& request = mutationContext->Request();
    auto mutationId = request.MutationId;
    auto mutationSize = request.Data.Size();
    auto term = mutationContext->GetTerm();

    {
        TMutationContextGuard mutationContextGuard(mutationContext);
        if (request.Type == EnterReadOnlyMutationType || request.Type == ExitReadOnlyMutationType) {
            ReadOnly_ = request.Type == EnterReadOnlyMutationType;

            YT_LOG_DEBUG("Received %v read-only mutation (Version: %v, SequenceNumber: %v, MutationId: %v)",
                request.Type == EnterReadOnlyMutationType ? "enable" : "disable",
                mutationContext->GetVersion(),
                mutationContext->GetSequenceNumber(),
                mutationId);
        } else {
            Automaton_->ApplyMutation(mutationContext);
        }

        if (Options_.ResponseKeeper &&
            mutationId &&
            !mutationContext->GetResponseKeeperSuppressed() &&
            mutationContext->GetResponseData()) // Null when mutation idempotizer kicks in. TODO(shakurov): suppress keeper instead?
        {
            result->ResponseKeeperPromiseSetter =
                Options_.ResponseKeeper->EndRequest(mutationId, mutationContext->GetResponseData());
        }
    }

    mutationContext->CombineStateHash(mutationContext->GetRandomSeed());
    StateHash_ = mutationContext->GetStateHash();

    Timestamp_ = mutationContext->GetTimestamp();

    auto sequenceNumber = ++SequenceNumber_;

    YT_LOG_FATAL_IF(
        sequenceNumber != mutationContext->GetSequenceNumber(),
        "Sequence numbers differ (AutomatonSequenceNumber: %v, MutationSequenceNumber: %v)",
        sequenceNumber,
        mutationContext->GetSequenceNumber());

    YT_LOG_FATAL_IF(
        RandomSeed_ != mutationContext->GetPrevRandomSeed(),
        "Mutation random seeds differ (AutomatonRandomSeed: %x, MutationPrevRandomSeed: %x, MutationRandomSeed: %x, MutationSequenceNumber: %v)",
        RandomSeed_.load(),
        mutationContext->GetPrevRandomSeed(),
        mutationContext->GetRandomSeed(),
        mutationContext->GetSequenceNumber());
    RandomSeed_ = mutationContext->GetRandomSeed();

    if (mutationVersion.SegmentId == automatonVersion.SegmentId) {
        YT_VERIFY(mutationVersion.RecordId == automatonVersion.RecordId);
    } else {
        YT_VERIFY(mutationVersion.SegmentId > automatonVersion.SegmentId);
        YT_VERIFY(mutationVersion.RecordId == 0);
    }
    AutomatonVersion_ = mutationVersion.Advance();

    LastMutationTerm_ = term;
    ++MutationCountSinceLastSnapshot_;
    MutationSizeSinceLastSnapshot_ += mutationSize;

    if (Config_->Get()->EnableStateHashChecker) {
        auto peerId = GetEpochContext()->CellManager->GetSelfPeerId();
        StateHashChecker_->Report(sequenceNumber, StateHash_, peerId);
    }

    if (auto invariantsCheckProbability = Config_->Get()->InvariantsCheckProbability) {
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

TEpochContextPtr TDecoratedAutomaton::GetEpochContext() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EpochContext_.Acquire();
}

TEpochId TDecoratedAutomaton::GetEpochId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EpochContext_.AcquireHazard()->EpochId;
}

ui64 TDecoratedAutomaton::GetStateHash() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StateHash_.load();
}

i64 TDecoratedAutomaton::GetSequenceNumber() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SequenceNumber_.load();
}

i64 TDecoratedAutomaton::GetRandomSeed() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RandomSeed_.load();
}

int TDecoratedAutomaton::GetLastMutationTerm() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return LastMutationTerm_.load();
}

i64 TDecoratedAutomaton::GetReliablyAppliedSequenceNumber() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ReliablyAppliedSequenceNumber_.load();
}

TReachableState TDecoratedAutomaton::GetReachableState() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return {AutomatonVersion_.load().SegmentId, SequenceNumber_.load()};
}

TVersion TDecoratedAutomaton::GetAutomatonVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return AutomatonVersion_.load();
}

bool TDecoratedAutomaton::TryAcquireUserLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

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
    VERIFY_THREAD_AFFINITY_ANY();

    --UserLock_;
}

void TDecoratedAutomaton::AcquireSystemLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    int result = ++SystemLock_;
    while (UserLock_.load() != 0) {
        SpinLockPause();
    }
    YT_LOG_DEBUG("System lock acquired (Lock: %v)",
        result);
}

void TDecoratedAutomaton::ReleaseSystemLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    int result = --SystemLock_;
    YT_LOG_DEBUG("System lock released (Lock: %v)",
        result);
}

void TDecoratedAutomaton::StartEpoch(TEpochContextPtr epochContext)
{
    YT_VERIFY(!EpochContext_.Exchange(std::move(epochContext)));

    SanitizedLocalHostName_ = SanitizeLocalHostName();
    YT_VERIFY(!SanitizedLocalHostName_.Empty());
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

    if (Options_.ResponseKeeper) {
        Options_.ResponseKeeper->CancelPendingRequests(error);
    }

    CancelSnapshot(error);

    EpochContext_.Store(nullptr);
    SanitizedLocalHostName_.Reset();
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

    MutationCountSinceLastSnapshot_ = 0;
    MutationSizeSinceLastSnapshot_ = 0;
}

void TDecoratedAutomaton::MaybeStartSnapshotBuilder()
{
    if (GetSequenceNumber() != NextSnapshotSequenceNumber_) {
        return;
    }

    YT_LOG_INFO("Building snapshot (SnapshotId: %v, SequenceNumber: %v, ReadOnly: %v)",
        NextSnapshotId_,
        NextSnapshotSequenceNumber_,
        NextSnapshotReadOnly_);

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

bool TDecoratedAutomaton::IsBuildingSnapshotNow() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BuildingSnapshot_.load();
}

i64 TDecoratedAutomaton::GetMutationCountSinceLastSnapshot() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MutationCountSinceLastSnapshot_.load();
}

i64 TDecoratedAutomaton::GetMutationSizeSinceLastSnapshot() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MutationSizeSinceLastSnapshot_.load();
}

int TDecoratedAutomaton::GetLastSuccessfulSnapshotId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return LastSuccessfulSnapshotId_.load();
}

bool TDecoratedAutomaton::GetLastSuccessfulSnapshotReadOnly() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return LastSuccessfulSnapshotReadOnly_.load();
}

bool TDecoratedAutomaton::GetReadOnly() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ReadOnly_.load();
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
