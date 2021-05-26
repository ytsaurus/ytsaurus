#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra automaton managing chaos cells.
class TChaosAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    TChaosAutomaton(
        IChaosSlotPtr slot,
        IInvokerPtr snapshotInvoker);

private:
    virtual std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) override;
    virtual std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override;

    virtual NHydra::TReign GetCurrentReign() override;
    virtual NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign) override;
};

DEFINE_REFCOUNTED_TYPE(TChaosAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TChaosAutomatonPart
    : public NHydra::TCompositeAutomatonPart
    , public virtual NLogging::TLoggerOwner
{
protected:
    const IChaosSlotPtr Slot_;
    NClusterNode::TBootstrap* const Bootstrap_;

    TChaosAutomatonPart(
        IChaosSlotPtr slot,
        NClusterNode::TBootstrap* bootstrap);

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
