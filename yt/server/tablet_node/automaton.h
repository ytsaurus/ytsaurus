#pragma once

#include "public.h"
#include "serialize.h"

#include <server/hydra/composite_automaton.h>

#include <server/cell_node/public.h>

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

//! An instance of Hydra managing a number of tablets.
class TTabletAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    explicit TTabletAutomaton(
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

class TTabletAutomatonPart
    : public NHydra::TCompositeAutomatonPart
{
protected:
    TTabletSlot* Slot;
    NCellNode::TBootstrap* Bootstrap;


    explicit TTabletAutomatonPart(
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap);

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;

    void RegisterSaver(
        int priority,
        const Stroka& name,
        TCallback<void(TSaveContext&)> saver);

    void RegisterLoader(
        const Stroka& name,
        TCallback<void(TLoadContext&)> loader);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
