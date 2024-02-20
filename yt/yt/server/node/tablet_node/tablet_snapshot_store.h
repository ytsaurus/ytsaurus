#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all tablet slots running at this node.
struct ITabletSnapshotStore
    : public TRefCounted
{
    //! Returns the list of snapshots for all registered tablets.
    virtual std::vector<TTabletSnapshotPtr> GetTabletSnapshots() const = 0;

    //! Returns the snapshot for a given tablet with latest mount revision or |nullptr| if none.
    virtual TTabletSnapshotPtr FindLatestTabletSnapshot(TTabletId tabletId) const = 0;

    //! Returns the snapshot for a given tablet with latest mount revision or throws
    //! if no such tablet is known.
    /*!
     *  \param cellId serves as a hint for better diagnostics and could be null.
     */
    virtual TTabletSnapshotPtr GetLatestTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId) const = 0;

    //! Returns the snapshot for a given tablet with given mount revision or |nullptr| if none.
    virtual TTabletSnapshotPtr FindTabletSnapshot(
        TTabletId tabletId,
        NHydra::TRevision mountRevision) const = 0;

    //! Returns the snapshot for a given tablet with given mount revision
    //! or throws if no such tablet is known.
    /*!
     *  \param cellId serves as a hint for better diagnostics and could be null.
     */
    virtual TTabletSnapshotPtr GetTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId,
        NHydra::TRevision mountRevision) const = 0;

    //! If #timestamp is other than #AsyncLastCommitted then checks
    //! that the Hydra instance has a valid leader lease.
    //! Throws on failure.
    virtual void ValidateTabletAccess(
        const TTabletSnapshotPtr& tabletSnapshot,
        NTransactionClient::TTimestamp timestamp) const = 0;

    virtual void ValidateBundleNotBanned(
        const TTabletSnapshotPtr& tabletSnapshot,
        const ITabletSlotPtr& slot = nullptr) const = 0;

    //! Informs the manager that some slot now serves #tablet.
    //! It is fine to update an already registered snapshot.
    virtual void RegisterTabletSnapshot(
        const ITabletSlotPtr& slot,
        TTablet* tablet,
        std::optional<TLockManagerEpoch> epoch = std::nullopt) = 0;

    //! Informs the manager that #tablet is no longer served.
    //! It is fine to attempt to unregister a snapshot that had never been registered.
    virtual void UnregisterTabletSnapshot(
        const ITabletSlotPtr& slot,
        TTablet* tablet) = 0;

    //! Informs the manager that #slot no longer serves any tablet.
    virtual void UnregisterTabletSnapshots(const ITabletSlotPtr& slot) = 0;

    //! Returns orchid service with tablet snapshots.
    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletSnapshotStore)

////////////////////////////////////////////////////////////////////////////////

ITabletSnapshotStorePtr CreateTabletSnapshotStore(
    TTabletNodeConfigPtr config,
    IBootstrap* bootstrap);

//! Dummy snapshot store for testing purposes.
ITabletSnapshotStorePtr CreateDummyTabletSnapshotStore(TTabletSnapshotPtr tabletSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
