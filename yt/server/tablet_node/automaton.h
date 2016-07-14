#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/hydra/composite_automaton.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{ };

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{ };

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra automaton managing a number of tablets.
class TTabletAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    TTabletAutomaton(
        TTabletSlotPtr slot,
        IInvokerPtr snapshotInvoker);

private:
    virtual std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) override;
    virtual std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override;

};

DEFINE_REFCOUNTED_TYPE(TTabletAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TTabletAutomatonPart
    : public virtual NHydra::TCompositeAutomatonPart
    , public virtual NLogging::TLoggerOwner
{
protected:
    const TTabletSlotPtr Slot_;
    NCellNode::TBootstrap* const Bootstrap_;


    TTabletAutomatonPart(
        TTabletSlotPtr slot,
        NCellNode::TBootstrap* bootstrap);

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
