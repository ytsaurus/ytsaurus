#pragma once

#include "public.h"
#include "serialize.h"

#include <core/misc/public.h>

#include <ytlib/new_table_client/public.h>

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
{
public:
    TLoadContext();

    DEFINE_BYVAL_RW_PROPERTY(TTabletSlot*, Slot);

    TChunkedMemoryPool* GetTempPool() const;
    NVersionedTableClient::TUnversionedRowBuilder* GetRowBuilder() const;

private:
    std::unique_ptr<TChunkedMemoryPool> TempPool_;
    std::unique_ptr<NVersionedTableClient::TUnversionedRowBuilder> RowBuilder_;

};

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra managing a number of tablets.
class TTabletAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    explicit TTabletAutomaton(TTabletSlotPtr slot);

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
    TTabletSlotPtr Slot_;
    NCellNode::TBootstrap* Bootstrap_;


    explicit TTabletAutomatonPart(
        TTabletSlotPtr slot,
        NCellNode::TBootstrap* bootstrap);

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;

    void RegisterSaver(
        NHydra::ESerializationPriority priority,
        const Stroka& name,
        TCallback<void(TSaveContext&)> saver);

    void RegisterLoader(
        const Stroka& name,
        TCallback<void(TLoadContext&)> loader);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
