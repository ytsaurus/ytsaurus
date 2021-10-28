#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra automaton managing a number of tablets.
class TTabletAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    using TCompositeAutomaton::TCompositeAutomaton;

private:
    std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) override;
    std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override;

    NHydra::TReign GetCurrentReign() override;
    NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign) override;
};

DEFINE_REFCOUNTED_TYPE(TTabletAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TTabletAutomatonPart
    : public NHydra::TCompositeAutomatonPart
    , public virtual NLogging::TLoggerOwner
{
protected:
    TTabletAutomatonPart(
        TCellId cellId,
        NHydra::ISimpleHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker);

    bool ValidateSnapshotVersion(int version) override;
    int GetCurrentSnapshotVersion() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
