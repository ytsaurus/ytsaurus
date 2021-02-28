#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/lib/hydra/public.h>

#include <yt/ytlib/tablet_client/proto/heartbeat.pb.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/actions/signal.h>

#include <yt/core/ytree/permission.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all tablet slots running at this node.
struct ISlotManager
    : public TRefCounted
{
    // The following methods have ControlThread affinity.
    
    virtual void Initialize() = 0;

    virtual bool IsOutOfMemory(const std::optional<TString>& poolTag) const = 0;

    //! Returns the total number of tablet slots.
    virtual int GetTotalTabletSlotCount() const = 0;

    //! Returns the number of available (not used) slots.
    virtual int GetAvailableTabletSlotCount() const = 0;

    //! Returns the number of currently used slots.
    virtual int GetUsedTabletSlotCount() const = 0;

    //! Returns |true| if there are free tablet slots and |false| otherwise.
    virtual bool HasFreeTabletSlots() const = 0;

    //! Returns fraction of CPU used by tablet slots (in terms of resource limits).
    virtual double GetUsedCpu(double cpuPerTabletSlot) const = 0;

    virtual const std::vector<TTabletSlotPtr>& Slots() const = 0;
    virtual void CreateSlot(const NTabletClient::NProto::TCreateTabletSlotInfo& createInfo) = 0;
    virtual void ConfigureSlot(const TTabletSlotPtr& slot, const NTabletClient::NProto::TConfigureTabletSlotInfo& configureInfo) = 0;
    virtual TFuture<void> RemoveSlot(const TTabletSlotPtr& slot) = 0;

    // The following methods are safe to call them from any thread.

    //! Finds the slot by cell id, returns null if none.
    virtual TTabletSlotPtr FindSlot(NHydra::TCellId id) = 0;

    //! Returns the list of snapshots for all registered tablets.
    virtual std::vector<TTabletSnapshotPtr> GetTabletSnapshots() = 0;

    //! Returns the snapshot for a given tablet with latest mount revision or |nullptr| if none.
    virtual TTabletSnapshotPtr FindLatestTabletSnapshot(TTabletId tabletId) = 0;

    //! Returns the snapshot for a given tablet with latest mount revision or throws
    //! if no such tablet is known.
    /*!
     *  \param cellId serves as a hint for better diagnostics and could be null.
     */
    virtual TTabletSnapshotPtr GetLatestTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId) = 0;

    //! Returns the snapshot for a given tablet with given mount revision or |nullptr| if none.
    virtual TTabletSnapshotPtr FindTabletSnapshot(
        TTabletId tabletId,
        NHydra::TRevision mountRevision) = 0;

    //! Returns the snapshot for a given tablet with given mount revision
    //! or throws if no such tablet is known.
    /*!
     *  \param cellId serves as a hint for better diagnostics and could be null.
     */
    virtual TTabletSnapshotPtr GetTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId,
        NHydra::TRevision mountRevision) = 0;

    //! If #timestamp is other than #AsyncLastCommitted then checks
    //! that the Hydra instance has a valid leader lease.
    //! Throws on failure.
    virtual void ValidateTabletAccess(
        const TTabletSnapshotPtr& tabletSnapshot,
        NTransactionClient::TTimestamp timestamp) = 0;

    //! Informs the manager that some slot now serves #tablet.
    //! It is fine to update an already registered snapshot.
    virtual void RegisterTabletSnapshot(
        const TTabletSlotPtr& slot,
        TTablet* tablet,
        std::optional<TLockManagerEpoch> epoch = std::nullopt) = 0;

    //! Informs the manager that #tablet is no longer served.
    //! It is fine to attempt to unregister a snapshot that had never been registered.
    virtual void UnregisterTabletSnapshot(
        TTabletSlotPtr slot,
        TTablet* tablet) = 0;

    //! Informs the manager that #slot no longer serves any tablet.
    virtual void UnregisterTabletSnapshots(TTabletSlotPtr slot) = 0;

    //! Informs the manager that the share of tablet dynamic memory
    //! of the corresponding bundle has changed.
    virtual void UpdateTabletCellBundleMemoryPoolWeight(const TString& bundleName) = 0;

    //! Adds slot-related alerts to #alerts.
    virtual void PopulateAlerts(std::vector<TError>* alerts) = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    //! Creates and configures a fake tablet slot and validates the tablet cell snapshot.
    virtual void ValidateCellSnapshot(NConcurrency::IAsyncZeroCopyInputStreamPtr reader) = 0;

    DECLARE_INTERFACE_SIGNAL(void(), BeginSlotScan);
    DECLARE_INTERFACE_SIGNAL(void(TTabletSlotPtr), ScanSlot);
    DECLARE_INTERFACE_SIGNAL(void(), EndSlotScan);
};

DEFINE_REFCOUNTED_TYPE(ISlotManager)

ISlotManagerPtr CreateSlotManager(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
