#include "simple_hydra_manager_mock.h"

#include <yt/yt/server/lib/hydra_common/composite_automaton.h>
#include <yt/yt/server/lib/hydra_common/mutation_context.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/logging/log.h>

#include <util/stream/mem.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger("SimpleHydra");

////////////////////////////////////////////////////////////////////////////////

TSimpleHydraManagerMock::TSimpleHydraManagerMock(
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker,
    TReign reign)
    : Automaton_(std::move(automaton))
    , AutomatonInvoker_(std::move(automatonInvoker))
    , Reign_(reign)
    , EpochId_(TEpochId::Create())
{ }

void TSimpleHydraManagerMock::ApplyUpTo(int sequenceNumber, bool recovery)
{
    if (recovery) {
        SaveLoad();
    }

    WaitFor(BIND(&TSimpleHydraManagerMock::DoApplyUpTo, MakeStrong(this), sequenceNumber)
        .AsyncVia(AutomatonInvoker_)
        .Run())
        .ThrowOnError();
}

void TSimpleHydraManagerMock::ApplyAll(bool recovery)
{
    ApplyUpTo(GetCommittedSequenceNumber(), recovery);
}

int TSimpleHydraManagerMock::GetPendingMutationCount() const
{
    return GetCommittedSequenceNumber() - GetAppliedSequenceNumber();
}

int TSimpleHydraManagerMock::GetAppliedSequenceNumber() const
{
    return AppliedSequenceNumber_;
}

int TSimpleHydraManagerMock::GetCommittedSequenceNumber() const
{
    return std::ssize(MutationRequests_);
}

TSimpleHydraManagerMock::TSnapshot TSimpleHydraManagerMock::SaveSnapshot()
{
    return WaitFor(BIND(&TSimpleHydraManagerMock::DoSaveSnapshot, MakeStrong(this))
        .AsyncVia(AutomatonInvoker_)
        .Run())
        .ValueOrThrow();
}

void TSimpleHydraManagerMock::LoadSnapshot(const TSnapshot& snapshot)
{
    WaitFor(BIND(&TSimpleHydraManagerMock::DoLoadSnapshot, MakeStrong(this), snapshot)
        .AsyncVia(AutomatonInvoker_)
        .Run())
        .ThrowOnError();
}

void TSimpleHydraManagerMock::SaveLoad()
{
    auto snapshot = SaveSnapshot();
    LoadSnapshot(snapshot);
}

void TSimpleHydraManagerMock::DoApplyUpTo(int sequenceNumber)
{
    VERIFY_INVOKER_AFFINITY(AutomatonInvoker_);

    YT_VERIFY(sequenceNumber <= static_cast<int>(MutationRequests_.size()));

    while (AppliedSequenceNumber_ < sequenceNumber) {
        const auto& request = MutationRequests_[AppliedSequenceNumber_];

        TMutationContext mutationContext(
            TVersion(0, AppliedSequenceNumber_),
            &request,
            TInstant::Now(),
            /*randomSeed*/ 0,
            /*prevRandomSeed*/ 0,
            /*sequenceNumber*/ AppliedSequenceNumber_,
            /*stateHash*/ 0,
            /*term*/ 0);

        {
            TMutationContextGuard mutationContextGuard(&mutationContext);
            Automaton_->ApplyMutation(&mutationContext);
        }
        MutationResponsePromises_[AppliedSequenceNumber_].Set(TMutationResponse{.Data = mutationContext.GetResponseData()});

        ++AppliedSequenceNumber_;
    }

    if (InRecoveryUntilSequenceNumber_ >= AppliedSequenceNumber_) {
        InRecoveryUntilSequenceNumber_ = std::nullopt;
    }
}

TSimpleHydraManagerMock::TSnapshot TSimpleHydraManagerMock::DoSaveSnapshot()
{
    VERIFY_INVOKER_AFFINITY(AutomatonInvoker_);

    TString snapshotData;
    TStringOutput output(snapshotData);
    auto writer = CreateAsyncAdapter(&output);

    TFuture<void> future;
    {
        TForbidContextSwitchGuard guard;
        TSnapshotSaveContext context{
            .Writer = writer,
        };
        future = Automaton_->SaveSnapshot(context);
    }
    WaitFor(future)
        .ThrowOnError();

    return TSnapshot{
        .SequenceNumber = AppliedSequenceNumber_,
        .Data = TSharedRef::FromString(std::move(snapshotData))
    };
}

void TSimpleHydraManagerMock::DoLoadSnapshot(const TSnapshot& snapshot)
{
    VERIFY_INVOKER_AFFINITY(AutomatonInvoker_);

    StopLeading_.Fire();

    Automaton_->Clear();

    TMemoryInput input(snapshot.Data.begin(), snapshot.Data.size());
    auto asyncInput = CreateAsyncAdapter(&input);
    auto reader = CreateZeroCopyAdapter(asyncInput);

    TSnapshotLoadContext loadContext{
        .Reader = reader,
    };
    Automaton_->LoadSnapshot(loadContext);

    AppliedSequenceNumber_ = snapshot.SequenceNumber;

    if (AppliedSequenceNumber_ != std::ssize(MutationRequests_)) {
        InRecoveryUntilSequenceNumber_ = std::ssize(MutationRequests_);
    }

    // Do not forget to clear custom mutation handlers.
    for (int sequenceNumber = AppliedSequenceNumber_; sequenceNumber < std::ssize(MutationRequests_); ++sequenceNumber) {
        auto& mutationRequest = MutationRequests_[sequenceNumber];
        mutationRequest.Handler = TCallback<void(TMutationContext*)>();
    }

    THydraContext hydraContext(
        TVersion(),
        /*timestamp*/ TInstant::Zero(),
        /*randomSeed*/ 0);
    THydraContextGuard guard(&hydraContext);

    Automaton_->PrepareState();
}

TFuture<TMutationResponse> TSimpleHydraManagerMock::CommitMutation(TMutationRequest&& request)
{
    YT_VERIFY(IsActiveLeader());

    YT_LOG_DEBUG("Committing mutation (SequenceNumber: %v, Type: %v)",
        MutationRequests_.size(),
        request.Type);
    MutationRequests_.emplace_back(std::move(request));
    auto promise = MutationResponsePromises_.emplace_back(NewPromise<TMutationResponse>());
    return promise;
}

TReign TSimpleHydraManagerMock::GetCurrentReign()
{
    return Reign_;
}

EPeerState TSimpleHydraManagerMock::GetAutomatonState() const
{
    return EPeerState::Leading;
}

bool TSimpleHydraManagerMock::IsActiveLeader() const
{
    return !InRecoveryUntilSequenceNumber_.has_value();
}

bool TSimpleHydraManagerMock::IsActiveFollower() const
{
    return InRecoveryUntilSequenceNumber_.has_value();
}

TEpochId TSimpleHydraManagerMock::GetAutomatonEpochId() const
{
    return EpochId_;
}

TCancelableContextPtr TSimpleHydraManagerMock::GetAutomatonCancelableContext() const
{
    return CancelableContext;
}

TFuture<void> TSimpleHydraManagerMock::Reconfigure(TDynamicDistributedHydraManagerConfigPtr /*config*/)
{
    // Do nothing.
    return VoidFuture;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
