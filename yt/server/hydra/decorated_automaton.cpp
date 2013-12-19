#include "stdafx.h"
#include "decorated_automaton.h"
#include "config.h"
#include "snapshot.h"
#include "changelog.h"
#include "automaton.h"
#include "serialize.h"
#include "response_keeper.h"
#include "mutation_context.h"
#include "snapshot_discovery.h"

#include <core/concurrency/fiber.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service.pb.h>

#include <server/misc/snapshot_builder_detail.h>

#include <util/random/random.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

static auto& Profiler = HydraProfiler;

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TGuardedUserInvoker
    : public IInvoker
{
public:
    TGuardedUserInvoker(
        TDecoratedAutomatonPtr decoratedAutomaton,
        IInvokerPtr underlyingInvoker)
        : DecoratedAutomaton(decoratedAutomaton)
        , UnderlyingInvoker(underlyingInvoker)
    { }

    virtual bool Invoke(const TClosure& action) override
    {
        if (!DecoratedAutomaton->AcquireUserEnqueueLock()) {
            return false;
        }
        if (DecoratedAutomaton->GetState() != EPeerState::Leading &&
            DecoratedAutomaton->GetState() != EPeerState::Following)
        {
            DecoratedAutomaton->ReleaseUserEnqueueLock();
            return false;
        }
        TCurrentInvokerGuard guard(this);
        bool result = UnderlyingInvoker->Invoke(action);
        DecoratedAutomaton->ReleaseUserEnqueueLock();
        return result;
    }

    virtual NConcurrency::TThreadId GetThreadId() const override
    {
        return UnderlyingInvoker->GetThreadId();
    }

private:
    TDecoratedAutomatonPtr DecoratedAutomaton;
    IInvokerPtr UnderlyingInvoker;

};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TSystemInvoker
    : public IInvoker
{
public:
    explicit TSystemInvoker(TDecoratedAutomaton* decoratedAutomaton)
        : DecoratedAutomaton(decoratedAutomaton)
    { }

    virtual bool Invoke(const TClosure& action) override
    {
        DecoratedAutomaton->AcquireSystemLock();

        auto this_ = MakeStrong(this);
        bool result = DecoratedAutomaton->AutomatonInvoker->Invoke(BIND([this, this_, action] () {
            try {
                TCurrentInvokerGuard guard(this_);
                action.Run();
            } catch (...) {
                DecoratedAutomaton->ReleaseSystemLock();
                throw;
            }
            DecoratedAutomaton->ReleaseSystemLock();
        }));

        if (!result) {
            DecoratedAutomaton->ReleaseSystemLock();
        }

        return result;
    }

    virtual NConcurrency::TThreadId GetThreadId() const override
    {
        return DecoratedAutomaton->AutomatonInvoker->GetThreadId();
    }

private:
    TDecoratedAutomaton* DecoratedAutomaton;

};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton::TSnapshotBuilder
    : public TSnapshotBuilderBase
{
public:
    TSnapshotBuilder(
        TDecoratedAutomatonPtr owner,
        TPromise<TErrorOr<TSnapshotInfo>> promise)
        : Owner(owner)
        , Promise(promise)
    {
        Logger = HydraLogger;
    }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner->AutomatonThread);

        TSnapshotCreateParams params;
        params.PrevRecordCount = Owner->AutomatonVersion.RecordId;
        SnapshotId = Owner->AutomatonVersion.SegmentId + 1;
        Writer = Owner->SnapshotStore->CreateWriter(SnapshotId, params);

        TSnapshotBuilderBase::Run().Subscribe(
            BIND(&TSnapshotBuilder::OnFinished, MakeStrong(this)));
    }

private:
    TDecoratedAutomatonPtr Owner;
    TPromise<TErrorOr<TSnapshotInfo>> Promise;

    int SnapshotId;
    ISnapshotWriterPtr Writer;


    TDuration GetTimeout() const override
    {
        return Owner->Config->SnapshotTimeout;
    }

    void Build() override
    {
        Owner->Automaton->SaveSnapshot(Writer->GetStream());
        Writer->Close();
    }

    void OnFinished(TError error)
    {
        if (!error.IsOK()) {
            Promise.Set(error);
            return;
        }

        Writer->Confirm();

        auto params = Owner->SnapshotStore->TryGetSnapshotParams(SnapshotId);
        YCHECK(params);
        
        TSnapshotInfo info;
        info.PeerId = Owner->CellManager->GetSelfId();
        info.SnapshotId = SnapshotId;
        info.Length = params->CompressedLength;
        info.Checksum = params->Checksum;
        Promise.Set(info);
    }

};

////////////////////////////////////////////////////////////////////////////////

TDecoratedAutomaton::TDecoratedAutomaton(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IAutomatonPtr automaton,
    IInvokerPtr automatonInvoker,
    IInvokerPtr controlInvoker,
    ISnapshotStorePtr snapshotStore,
    IChangelogStorePtr changelogStore)
    : State_(EPeerState::Stopped)
    , Config(config)
    , CellManager(cellManager)
    , Automaton(automaton)
    , AutomatonInvoker(automatonInvoker)
    , ControlInvoker(controlInvoker)
    , UserEnqueueLock(0)
    , SystemLock(0)
    , SystemInvoker(New<TSystemInvoker>(this))
    , SnapshotStore(snapshotStore)
    , ChangelogStore(changelogStore)
    , MutationContext(nullptr)
    , BatchCommitTimeCounter("/batch_commit_time")
    , Logger(HydraLogger)
{
    YCHECK(Config);
    YCHECK(CellManager);
    YCHECK(Automaton);
    YCHECK(ControlInvoker);
    YCHECK(SnapshotStore);
    YCHECK(ChangelogStore);

    VERIFY_INVOKER_AFFINITY(AutomatonInvoker, AutomatonThread);
    VERIFY_INVOKER_AFFINITY(ControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(HydraIOQueue->GetInvoker(), IOThread);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(CellManager->GetCellGuid())));

    ResponseKeeper = New<TResponseKeeper>(
        config->ResponseKeeper,
        CellManager,
        AutomatonInvoker);

    Reset();
}

void TDecoratedAutomaton::OnStartLeading()
{
    YCHECK(State_ == EPeerState::Stopped);
    State_ = EPeerState::LeaderRecovery;
}

void TDecoratedAutomaton::OnLeaderRecoveryComplete()
{
    YCHECK(State_ == EPeerState::LeaderRecovery);
    State_ = EPeerState::Leading;
}

void TDecoratedAutomaton::OnStopLeading()
{
    YCHECK(State_ == EPeerState::Leading || State_ == EPeerState::LeaderRecovery);
    Reset();
}

void TDecoratedAutomaton::OnStartFollowing()
{
    YCHECK(State_ == EPeerState::Stopped);
    State_ = EPeerState::FollowerRecovery;
}

void TDecoratedAutomaton::OnFollowerRecoveryComplete()
{
    YCHECK(State_ == EPeerState::FollowerRecovery);
    State_ = EPeerState::Following;
}

void TDecoratedAutomaton::OnStopFollowing()
{
    YCHECK(State_ == EPeerState::Following || State_ == EPeerState::FollowerRecovery);
    Reset();
}

IInvokerPtr TDecoratedAutomaton::CreateGuardedUserInvoker(IInvokerPtr underlyingInvoker)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return New<TGuardedUserInvoker>(this, underlyingInvoker);
}

IInvokerPtr TDecoratedAutomaton::GetSystemInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SystemInvoker;
}

IAutomatonPtr TDecoratedAutomaton::GetAutomaton()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Automaton;
}

void TDecoratedAutomaton::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Automaton->Clear();
    ResponseKeeper->Clear();
    Reset();

    {
        TGuard<TSpinLock> guard(VersionSpinLock);
        AutomatonVersion = TVersion();
    }
}

void TDecoratedAutomaton::Save(TOutputStream* output)
{
    YCHECK(output);
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Automaton->SaveSnapshot(output);
}

void TDecoratedAutomaton::Load(int snapshotId, TInputStream* input)
{
    YCHECK(input);
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    LOG_INFO("Started loading snapshot %d", snapshotId);

    CurrentChangelog.Reset();

    PROFILE_TIMING ("/snapshot_load_time") {
        Automaton->Clear();
        Automaton->LoadSnapshot(input);
    }

    LOG_INFO("Finished loading snapshot");

    {
        TGuard<TSpinLock> guard(VersionSpinLock);
        AutomatonVersion = TVersion(snapshotId, 0);
    }
}

void TDecoratedAutomaton::ApplyMutationDuringRecovery(const TSharedRef& recordData)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DoApplyMutation(recordData);

    {
        TGuard<TSpinLock> guard(VersionSpinLock);
        ++AutomatonVersion.RecordId;
    }
}

void TDecoratedAutomaton::RotateChangelogDuringRecovery()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    {
        TGuard<TSpinLock> guard(VersionSpinLock);
        AutomatonVersion = TVersion(AutomatonVersion.SegmentId + 1, 0);
    }
}

void TDecoratedAutomaton::LogMutationAtLeader(
    const TMutationRequest& request,
    TSharedRef* recordData,
    TFuture<void>* logResult,
    TPromise<TErrorOr<TMutationResponse>> commitResult)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(recordData);
    YASSERT(logResult);
    YASSERT(commitResult);

    TPendingMutation pendingMutation;
    pendingMutation.Version = LoggedVersion;
    pendingMutation.Request = request;
    pendingMutation.Timestamp = TInstant::Now();
    pendingMutation.RandomSeed  = RandomNumber<ui64>();
    pendingMutation.CommitPromise = std::move(commitResult);
    PendingMutations.push(pendingMutation);

    MutationHeader.Clear(); // don't forget to cleanup the pooled instance
    MutationHeader.set_mutation_type(request.Type);
    if (request.Id != NullMutationId) {
        ToProto(MutationHeader.mutable_mutation_id(), request.Id);
    }
    MutationHeader.set_timestamp(pendingMutation.Timestamp.GetValue());
    MutationHeader.set_random_seed(pendingMutation.RandomSeed);
    
    *recordData = SerializeMutationRecord(MutationHeader, request.Data);

    LOG_DEBUG("Logging mutation %s at version %s",
        ~ToString(request.Id),
        ~ToString(LoggedVersion));

    auto changelog = GetCurrentChangelog();
    *logResult = changelog->Append(*recordData);
    
    {
        TGuard<TSpinLock> guard(VersionSpinLock);
        ++LoggedVersion.RecordId;
    }
}

void TDecoratedAutomaton::LogMutationAtFollower(
    const TSharedRef& recordData,
    TFuture<void>* logResult)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TSharedRef mutationData;
    DeserializeMutationRecord(recordData, &MutationHeader, &mutationData);

    TPendingMutation pendingMutation;
    pendingMutation.Version = LoggedVersion;
    pendingMutation.Request.Type = MutationHeader.mutation_type();
    pendingMutation.Request.Data = mutationData;
    pendingMutation.Request.Id =
        MutationHeader.has_mutation_id()
        ? FromProto<TMutationId>(MutationHeader.mutation_id())
        : NullMutationId;
    pendingMutation.Timestamp = TInstant(MutationHeader.timestamp());
    pendingMutation.RandomSeed  = MutationHeader.random_seed();
    PendingMutations.push(pendingMutation);

    LOG_DEBUG("Logging mutation %s at version %s",
        ~ToString(pendingMutation.Request.Id),
        ~ToString(LoggedVersion));

    auto changelog = GetCurrentChangelog();
    auto actualLogResult = changelog->Append(recordData);
    if (logResult) {
        *logResult = std::move(actualLogResult);
    }

    {
        TGuard<TSpinLock> guard(VersionSpinLock);
        ++LoggedVersion.RecordId;
    }
}

TFuture<TErrorOr<TSnapshotInfo>> TDecoratedAutomaton::BuildSnapshot()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SnapshotVersion = LoggedVersion;
    auto promise = SnapshotInfoPromise = NewPromise<TErrorOr<TSnapshotInfo>>();

    LOG_INFO("Scheduled snapshot at version %s",
        ~ToString(LoggedVersion));

    MaybeStartSnapshotBuilder();

    return promise;
}

TFuture<void> TDecoratedAutomaton::RotateChangelog()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    LOG_INFO("Rotating changelog at version %s",
        ~ToString(LoggedVersion));

    return
        BIND(
            &TDecoratedAutomaton::DoRotateChangelog,
            MakeStrong(this),
            GetCurrentChangelog())
        .AsyncVia(HydraIOQueue->GetInvoker())
        .Run();
}

void TDecoratedAutomaton::DoRotateChangelog(IChangelogPtr changelog)
{
    VERIFY_THREAD_AFFINITY(IOThread);

    if (CurrentChangelog != changelog)
        return;

    WaitFor(changelog->Flush());
    
    if (changelog->IsSealed()) {
        LOG_WARNING("Changelog %d is already sealed",
            changelog->GetId());
    } else {
        WaitFor(changelog->Seal(changelog->GetRecordCount()));
    }

    if (CurrentChangelog != changelog)
        return;

    TChangelogCreateParams params;
    params.PrevRecordCount = changelog->GetRecordCount();
    auto newChangelog = CurrentChangelog = ChangelogStore->CreateChangelog(
        changelog->GetId() + 1,
        params);

    SwitchTo(AutomatonInvoker);
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (CurrentChangelog != newChangelog)
        return;

    {
        TGuard<TSpinLock> guard(VersionSpinLock);
        YCHECK(LoggedVersion.SegmentId == changelog->GetId());
        LoggedVersion = TVersion(newChangelog->GetId(), 0);
    }

    LOG_INFO("Changelog rotated");
}

void TDecoratedAutomaton::CommitMutations(TVersion version)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    LOG_DEBUG("Applying mutations upto version %s",
        ~ToString(version));

    PROFILE_AGGREGATED_TIMING (BatchCommitTimeCounter) {
        while (!PendingMutations.empty()) {
            auto& pendingMutation = PendingMutations.front();
            if (pendingMutation.Version >= version)
                break;

            LOG_DEBUG("Applying mutation %s at version %s",
                ~ToString(pendingMutation.Request.Id),
                ~ToString(pendingMutation.Version));

            // Check for rotated changelogs, update segmentId if needed.
            if (pendingMutation.Version.SegmentId == AutomatonVersion.SegmentId) {
                YCHECK(pendingMutation.Version.RecordId == AutomatonVersion.RecordId);
            } else {
                YCHECK(pendingMutation.Version.SegmentId > AutomatonVersion.SegmentId);
                YCHECK(pendingMutation.Version.RecordId == 0);
                TGuard<TSpinLock> guard(VersionSpinLock);
                AutomatonVersion = pendingMutation.Version;
            }

            TMutationContext context(
                AutomatonVersion,
                pendingMutation.Request,
                pendingMutation.Timestamp,
                pendingMutation.RandomSeed);

            DoApplyMutation(&context);

            {
                TGuard<TSpinLock> guard(VersionSpinLock);
                ++AutomatonVersion.RecordId;
            }

            if (pendingMutation.CommitPromise) {
                TMutationResponse response;
                response.Data = context.GetResponseData();
                pendingMutation.CommitPromise.Set(std::move(response));
            }

            MaybeStartSnapshotBuilder();

            PendingMutations.pop();
        }
    }

    // Check for rotated changelogs, once again.
    if (version.SegmentId > AutomatonVersion.SegmentId) {
        YCHECK(version.RecordId == 0);
        TGuard<TSpinLock> guard(VersionSpinLock);
        AutomatonVersion = version;
    }

    YCHECK(AutomatonVersion >= version);
}

void TDecoratedAutomaton::DoApplyMutation(const TSharedRef& recordData)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    NProto::TMutationHeader header;
    TSharedRef requestData;
    DeserializeMutationRecord(recordData, &header, &requestData);

    TMutationRequest request(
        header.mutation_type(),
        requestData);

    TMutationContext context(
        AutomatonVersion,
        request,
        TInstant(header.timestamp()),
        header.random_seed());

    DoApplyMutation(&context);
}

void TDecoratedAutomaton::DoApplyMutation(TMutationContext* context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    MutationContext = context;
    const auto& action = context->GetRequestAction();
    if (!action) {
        Automaton->ApplyMutation(context);
    } else {
        action.Run();
    }
    MutationContext = nullptr;
    
    if (context->GetId() != NullMutationId) {
        ResponseKeeper->RegisterResponse(context->GetId(), context->GetResponseData());
    }
}

bool TDecoratedAutomaton::FindKeptResponse(const TMutationId& id, TSharedRef* data)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return ResponseKeeper->FindResponse(id, data);
}

IChangelogPtr TDecoratedAutomaton::GetCurrentChangelog()
{
    if (!CurrentChangelog) {
        CurrentChangelog = ChangelogStore->OpenChangelogOrThrow(LoggedVersion.SegmentId);
    }
    return CurrentChangelog;
}

TVersion TDecoratedAutomaton::GetLoggedVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(VersionSpinLock);
    return LoggedVersion;
}

void TDecoratedAutomaton::SetLoggedVersion(TVersion version)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(VersionSpinLock);
    LoggedVersion = version;
}

TVersion TDecoratedAutomaton::GetAutomatonVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(VersionSpinLock);
    return AutomatonVersion;
}

TMutationContext* TDecoratedAutomaton::GetMutationContext()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return MutationContext;
}

bool TDecoratedAutomaton::AcquireUserEnqueueLock()
{
    if (SystemLock != 0) {
        return false;
    }
    AtomicIncrement(UserEnqueueLock);
    if (AtomicGet(SystemLock) != 0) {
        AtomicDecrement(UserEnqueueLock);
        return false;
    }
    return true;
}

void TDecoratedAutomaton::ReleaseUserEnqueueLock()
{
    AtomicDecrement(UserEnqueueLock);
}

void TDecoratedAutomaton::AcquireSystemLock()
{
    AtomicIncrement(SystemLock);
    while (AtomicGet(UserEnqueueLock) != 0) {
        SpinLockPause();
    }
    LOG_DEBUG("System lock acquired (Lock: %" PRISZT ")",
        SystemLock);
}

void TDecoratedAutomaton::ReleaseSystemLock()
{
    AtomicDecrement(SystemLock);
    LOG_DEBUG("System lock released (Lock: %" PRISZT ")",
        SystemLock);
}

void TDecoratedAutomaton::Reset()
{
    State_ = EPeerState::Stopped;
    PendingMutations.clear();
    CurrentChangelog.Reset();
    SnapshotVersion = TVersion();
    SnapshotInfoPromise.Reset();
}

void TDecoratedAutomaton::MaybeStartSnapshotBuilder()
{
    if (AutomatonVersion != SnapshotVersion)
        return;

    auto builder = New<TSnapshotBuilder>(this, SnapshotInfoPromise);
    builder->Run();

    SnapshotInfoPromise.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
