#include "simple_hydra_manager_mock.h"

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/logging/log.h>

#include <util/stream/mem.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TLogger Logger("SimpleHydra");

////////////////////////////////////////////////////////////////////////////////

TSimpleHydraManagerMock::TSimpleHydraManagerMock(
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker,
    TReign reign)
    : Automaton_(std::move(automaton))
    , AutomatonInvoker_(std::move(automatonInvoker))
    , Reign_(reign)
{ }

void TSimpleHydraManagerMock::ApplyUpTo(int sequenceNumber)
{
    WaitFor(BIND(&TSimpleHydraManagerMock::DoApplyUpTo, MakeStrong(this), sequenceNumber)
        .AsyncVia(AutomatonInvoker_)
        .Run())
        .ThrowOnError();
}

void TSimpleHydraManagerMock::ApplyAll()
{
    ApplyUpTo(GetCommittedSequenceNumber());
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
    return MutationRequests_.size();
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
            request,
            TInstant::Now(),
            /*randomSeed*/ 0,
            /*prevRandomSeed*/ 0,
            /*sequenceNumber*/ AppliedSequenceNumber_,
            /*stateHash*/ 0);

        {
            TMutationContextGuard mutationContextGuard(&mutationContext);
            Automaton_->ApplyMutation(&mutationContext);
        }
        ++AppliedSequenceNumber_;
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
        future = Automaton_->SaveSnapshot(writer);
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

    Automaton_->Clear();

    TMemoryInput input(snapshot.Data.begin(), snapshot.Data.size());
    auto asyncInput = CreateAsyncAdapter(&input);
    auto reader = CreateZeroCopyAdapter(asyncInput);

    Automaton_->LoadSnapshot(reader);

    AppliedSequenceNumber_ = snapshot.SequenceNumber;

    // Do not forget to clear custom mutation handlers.
    for (int sequenceNumber = AppliedSequenceNumber_; sequenceNumber < std::ssize(MutationRequests_); ++sequenceNumber)
    {
        auto& mutationRequest = MutationRequests_[sequenceNumber];
        mutationRequest.Handler = TCallback<void(TMutationContext*)>();
    }

    Automaton_->PrepareState();
}

TFuture<TMutationResponse> TSimpleHydraManagerMock::CommitMutation(TMutationRequest&& request)
{
    YT_LOG_DEBUG("Committing mutation (SequenceNumber: %v, Type: %v)",
        MutationRequests_.size(),
        request.Type);
    MutationRequests_.emplace_back(std::move(request));
    return MakeFuture(TMutationResponse{});
}

bool TSimpleHydraManagerMock::IsMutationLoggingEnabled() const
{
    return true;
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
    return true;
}

bool TSimpleHydraManagerMock::IsActiveFollower() const
{
    return false;
}

TCancelableContextPtr TSimpleHydraManagerMock::GetAutomatonCancelableContext() const
{
    return CancelableContext;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
