#include "automaton.h"

#include <yt/yt/server/lib/hydra/serialize.h>

namespace NYT::NHydraStressTest {

using namespace NHydra;
using namespace NYT::NProto;

//////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraStressTestLogger;

//////////////////////////////////////////////////////////////////////////////////

TAutomatonPart::TAutomatonPart(
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker,
    TLinearizabilityCheckerPtr linearizabilityChecker)
    : TCompositeAutomatonPart(
        hydraManager,
        automaton,
        automatonInvoker)
    , LinearizabilityChecker_(linearizabilityChecker)
{
    RegisterMethod(BIND(&TAutomatonPart::HydraCas, Unretained(this)));
    RegisterMethod(BIND(&TAutomatonPart::HydraSequencePart, Unretained(this)));
    RegisterMethod(BIND(&TAutomatonPart::HydraSequence, Unretained(this)));
    RegisterLoader(
        "Part",
        BIND(&TAutomatonPart::Load, Unretained(this)));
    RegisterSaver(
        ESyncSerializationPriority::Values,
        "Part",
        BIND(&TAutomatonPart::Save, Unretained(this)));
}

TValue TAutomatonPart::GetCasValue() const
{
    return CasValue_;
}

std::unique_ptr<TMutation> TAutomatonPart::CreateCasMutation(TCtxCasPtr context)
{
    return CreateMutation(
        HydraManager_,
        std::move(context),
        &TAutomatonPart::HydraCas,
        this);
}

std::unique_ptr<NHydra::TMutation> TAutomatonPart::CreateSequenceMutation(TCtxSequencePtr context)
{
    return CreateMutation(
        HydraManager_,
        std::move(context),
        &TAutomatonPart::HydraSequence,
        this);
}

void TAutomatonPart::Clear()
{
    CasValue_ = {};
    SequenceValue_ = {};
    MutationIndex_ = 0;
}

void TAutomatonPart::SetZeroState()
{
    CasValue_ = {};
    SequenceValue_ = {};
    MutationIndex_ = 0;
}

void TAutomatonPart::Load(TLoadContext& context)
{
    CasValue_ = NYT::Load<TValue>(context);
    SequenceValue_ = NYT::Load<i64>(context);
    MutationIndex_ = NYT::Load<i64>(context);
}

void TAutomatonPart::Save(TSaveContext& context) const
{
    NYT::Save(context, CasValue_);
    NYT::Save(context, SequenceValue_);
    NYT::Save(context, MutationIndex_);
}

void TAutomatonPart::HydraCas(
    const TCtxCasPtr& context,
    TReqCas* request,
    TRspCas* response)
{
    auto expected = request->expected();
    auto desired = request->desired();

    auto* mutationContext = GetCurrentMutationContext();
    if (CasValue_ == expected) {
        CasValue_ = desired;
        response->set_success(true);
        YT_LOG_DEBUG(
            "CAS succeeded (Expected: %v, Desired: %v)",
            expected,
            desired);
        mutationContext->CombineStateHash(1, expected, desired);
    } else {
        response->set_success(false);
        response->set_current(CasValue_);
        YT_LOG_DEBUG(
            "CAS failed (Expected: %v, Desired: %v, Actual: %v)",
            expected,
            desired,
            CasValue_);
        mutationContext->CombineStateHash(2, expected, desired, CasValue_);
    }

    LinearizabilityChecker_->SubmitMutation(mutationContext->GetRandomSeed(), MutationIndex_);
    ++MutationIndex_;

    if (context) {
        context->SetResponseInfo("Success: %v, Current: %v",
            response->success(),
            response->current());
    }
}

void TAutomatonPart::HydraSequencePart(
    const TCtxSequencePartPtr& context,
    TReqSequencePart* request,
    TRspSequencePart* /*response*/)
{
    auto value = request->value();
    auto id = request->id();
    if (context) {
        context->SetRequestInfo("SequenceId: %v, Value: %v",
            value,
            id);
    }

    YT_LOG_DEBUG(
        "Starting sequence iteration (SequenceId: %v, Value: %v)",
        value,
        id);

    if (value > 0) {
        YT_VERIFY(SequenceValue_ == value - 1);
    }

    SequenceValue_ = value;

    if (context) {
        context->SetResponseInfo();
    }
}

void TAutomatonPart::HydraSequence(
    const TCtxSequencePtr& context ,
    TReqSequence* request,
    TRspSequence* /* response */)
{
    auto id = request->id();
    auto count = request->count();

    if (context) {
        context->SetRequestInfo("SequenceId: %v, Count: %v",
            count,
            id);
    }

    YT_LOG_DEBUG(
        "Started sequence (SequenceId: %v, Count: %v)",
        id,
        count);

    for (int i = 0; i < count; ++i) {
        TReqSequencePart req;
        req.set_value(i);
        req.set_id(id);
        YT_UNUSED_FUTURE(CreateMutation(HydraManager_, req)
            ->CommitAndLog(Logger));
    }

    YT_LOG_DEBUG(
        "Finished sequence (SequenceId: %v, Count: %v)",
        id,
        count);

    if (context) {
        context->SetResponseInfo();
    }
}

//////////////////////////////////////////////////////////////////////////////////

TAutomaton::TAutomaton()
    : TCompositeAutomaton(
        nullptr,
        TCellId())
{ }

std::unique_ptr<TSaveContext> TAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
{
    return std::make_unique<TSaveContext>(output, std::move(logger), GetCurrentReign());
}

std::unique_ptr<TLoadContext> TAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>(input);
    TCompositeAutomaton::SetupLoadContext(context.get());
    return context;
}

TReign TAutomaton::GetCurrentReign()
{
    return 2;
}

EFinalRecoveryAction TAutomaton::GetActionToRecoverFromReign(TReign)
{
    return EFinalRecoveryAction::None;
}

EFinalRecoveryAction TAutomaton::GetFinalRecoveryAction()
{
    return EFinalRecoveryAction::None;
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
