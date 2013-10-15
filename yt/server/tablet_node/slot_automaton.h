#pragma once

#include "public.h"
#include "serialize.h"

#include <server/hydra/composite_automaton.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra managing a number of tablets.
class TSlotAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    explicit TSlotAutomaton(
        NCellNode::TBootstrap* bootstrap,
        TTabletSlot* slot);

    virtual TSaveContext& SaveContext() override;
    virtual TLoadContext& LoadContext() override;

private:
    TTabletSlot* Slot;

    TSaveContext SaveContext_;
    TLoadContext LoadContext_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
