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

    virtual TSaveContext& SaveContext() override;
    virtual TLoadContext& LoadContext() override;

private:
    TSaveContext SaveContext_;
    TLoadContext LoadContext_;

};

DEFINE_REFCOUNTED_TYPE(TTabletAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TTabletAutomatonPart
    : public NHydra::TCompositeAutomatonPart
{
protected:
    const TTabletSlotPtr Slot_;
    NCellNode::TBootstrap* const Bootstrap_;

    NLogging::TLogger Logger;


    explicit TTabletAutomatonPart(
        TTabletSlotPtr slot,
        NCellNode::TBootstrap* bootstrap);

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;

    void RegisterSaver(
        NHydra::ESyncSerializationPriority priority,
        const Stroka& name,
        TCallback<void(TSaveContext&)> saver);

    void RegisterSaver(
        NHydra::EAsyncSerializationPriority priority,
        const Stroka& name,
        TCallback<TCallback<void(TSaveContext&)>()> callback);

    void RegisterLoader(
        const Stroka& name,
        TCallback<void(TLoadContext&)> loader);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
