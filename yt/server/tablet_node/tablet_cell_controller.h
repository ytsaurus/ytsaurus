#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/ytree/public.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/cell_node/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all tablet slots running at this node.
class TTabletCellController
    : public TRefCounted
{
public:
    TTabletCellController(
        NCellNode::TCellNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TTabletCellController();

    void Initialize();


    //! Returns the number of available (not used) slots.
    int GetAvailableTabletSlotCount() const;

    //! Returns the number of currently used slots.
    int GetUsedTableSlotCount() const;


    const std::vector<TTabletSlotPtr>& Slots() const;
    TTabletSlotPtr FindSlot(const NHydra::TCellGuid& id);
    void CreateSlot(const NNodeTrackerClient::NProto::TCreateTabletSlotInfo& createInfo);
    void ConfigureSlot(TTabletSlotPtr slot, const NNodeTrackerClient::NProto::TConfigureTabletSlotInfo& configureInfo);
    void RemoveSlot(TTabletSlotPtr slot);


    // The following section of methods is used to maintain tablet id to slot mapping.
    // It is safe to call them from any thread.

    //! Returns a slot which is know to hold a given tablet or |nullptr| if none.
    TTabletSlotPtr FindSlotByTabletId(const TTabletId& tabletId);

    //! Informs the controller that some slot now serves #tablet.
    void RegisterTablet(TTablet* tablet);

    //! Informs the controller that #tablet is no longer served.
    void UnregisterTablet(TTablet* tablet);

    //! Informs the controller that #slot no longer serves any tablet.
    void UnregisterTablets(TTabletSlotPtr slot);


    NHydra::IChangelogCatalogPtr GetChangelogCatalog();
    NHydra::ISnapshotStorePtr GetSnapshotStore(const TCellGuid& cellGuid);

    NYTree::IYPathServicePtr GetOrchidService();

    DECLARE_SIGNAL(void(TTabletSlotPtr), SlotScan);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletCellController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
