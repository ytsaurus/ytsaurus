#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/ytree/public.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <server/cell_node/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all tablet slots running at this node.
class TTabletSlotManager
    : public TRefCounted
{
public:
    TTabletSlotManager(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TTabletSlotManager();

    void Initialize();

    bool IsOutOfMemory() const;
    bool IsRotationForced(i64 passiveUsage) const;

    //! Returns the number of available (not used) slots.
    int GetAvailableTabletSlotCount() const;

    //! Returns the number of currently used slots.
    int GetUsedTableSlotCount() const;

    const std::vector<TTabletSlotPtr>& Slots() const;
    TTabletSlotPtr FindSlot(const NHydra::TCellId& id);
    void CreateSlot(const NNodeTrackerClient::NProto::TCreateTabletSlotInfo& createInfo);
    void ConfigureSlot(TTabletSlotPtr slot, const NNodeTrackerClient::NProto::TConfigureTabletSlotInfo& configureInfo);
    void RemoveSlot(TTabletSlotPtr slot);


    // The following section of methods is used to maintain tablet id to slot mapping.
    // It is safe to call them from any thread.

    //! Returns the snapshot for a given tablet or |nullptr| if none.
    TTabletSnapshotPtr FindTabletSnapshot(const TTabletId& tabletId);

    //! Returns the snapshot for a given tablet or throws if no such tablet is known.
    TTabletSnapshotPtr GetTabletSnapshotOrThrow(const TTabletId& tabletId);

    //! Informs the controller that some slot now serves #tablet.
    void RegisterTabletSnapshot(TTablet* tablet);

    //! Informs the controller that #tablet is no longer served.
    void UnregisterTabletSnapshot(TTablet* tablet);

    //! Informs the controller that #slot no longer serves any tablet.
    void UnregisterTabletSnapshots(TTabletSlotPtr slot);

    //! Informs the controller that #tablet's snapshot must be updated.
    void UpdateTabletSnapshot(TTablet* tablet);


    NYTree::IYPathServicePtr GetOrchidService();

    DECLARE_SIGNAL(void(), BeginSlotScan);
    DECLARE_SIGNAL(void(TTabletSlotPtr), ScanSlot);
    DECLARE_SIGNAL(void(), EndSlotScan);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
