#pragma once

#include "public.h"
#include "checkers.h"
#include "helpers.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/checkpointable_stream.h>

#include <yt/yt/experiments/public/hydra_stress_test/peer_service.pb.h>

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

using TCtxCas = NYT::NRpc::TTypedServiceContext<
    NYT::NProto::TReqCas,
    NYT::NProto::TRspCas>;
using TCtxCasPtr = NYT::TIntrusivePtr<TCtxCas>;

using TCtxSequencePart = NYT::NRpc::TTypedServiceContext<
    NYT::NProto::TReqSequencePart,
    NYT::NProto::TRspSequencePart>;
using TCtxSequencePartPtr = NYT::TIntrusivePtr<TCtxSequencePart>;

using TCtxSequence = NYT::NRpc::TTypedServiceContext<
    NYT::NProto::TReqSequence,
    NYT::NProto::TRspSequence>;
using TCtxSequencePtr = NYT::TIntrusivePtr<TCtxSequence>;

//////////////////////////////////////////////////////////////////////////////////

class TAutomatonPart
    : public NHydra::TCompositeAutomatonPart
{
public:
    TAutomatonPart(
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        TLinearizabilityCheckerPtr linearizabilityChecker);

    TValue GetCasValue() const;

    std::unique_ptr<NHydra::TMutation> CreateCasMutation(TCtxCasPtr context);
    std::unique_ptr<NHydra::TMutation> CreateSequenceMutation(TCtxSequencePtr context);

private:
    const TLinearizabilityCheckerPtr LinearizabilityChecker_;

    TValue CasValue_ = {};
    i64 SequenceValue_ = {};

    i64 MutationIndex_ = 0;

    void Clear() override;
    void SetZeroState() override;

    void Load(NHydra::TLoadContext& context);
    void Save(NHydra::TSaveContext& context) const;

    void HydraCas(
        const TCtxCasPtr& context,
        NProto::TReqCas* request,
        NProto::TRspCas* response);

    void HydraSequencePart(
        const TCtxSequencePartPtr& context,
        NProto::TReqSequencePart* request,
        NProto::TRspSequencePart* response);

    void HydraSequence(
        const TCtxSequencePtr& context,
        NProto::TReqSequence* request,
        NProto::TRspSequence* response);
};

DEFINE_REFCOUNTED_TYPE(TAutomatonPart)

//////////////////////////////////////////////////////////////////////////////////

class TAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    TAutomaton();

    std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        NHydra::ICheckpointableOutputStream* output,
        NLogging::TLogger logger) override;
    std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        NHydra::ICheckpointableInputStream* input) override;

    NHydra::TReign GetCurrentReign() override;
    NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign) override;
    NHydra::EFinalRecoveryAction GetFinalRecoveryAction() override;
};

DEFINE_REFCOUNTED_TYPE(TAutomaton)

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
