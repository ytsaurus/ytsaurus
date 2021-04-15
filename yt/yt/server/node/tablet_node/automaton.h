#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/yt/server/node/cluster_node/public.h>

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
    TTabletAutomaton(
        ITabletSlotPtr slot,
        IInvokerPtr snapshotInvoker);

private:
    virtual std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) override;
    virtual std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override;

    virtual NHydra::TReign GetCurrentReign() override;
    virtual NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign) override;
};

DEFINE_REFCOUNTED_TYPE(TTabletAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TTabletAutomatonPart
    : public NHydra::TCompositeAutomatonPart
    , public virtual NLogging::TLoggerOwner
{
protected:
    const ITabletSlotPtr Slot_;
    NClusterNode::TBootstrap* const Bootstrap_;


    TTabletAutomatonPart(
        ITabletSlotPtr slot,
        NClusterNode::TBootstrap* bootstrap);

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
