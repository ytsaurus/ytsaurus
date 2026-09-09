#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NChaosElection {

////////////////////////////////////////////////////////////////////////////////

//! Creates chaos leases in a given chaos cell bundle.
//!
//! Creations go round-robin over the bundle's metadata cells: a lease creation is a Hydra mutation
//! and every lease is later pinged for its whole lifetime, so pinning one cell would turn it into a
//! hot spot once the leases are counted in tens of thousands. A cell whose lease manager is not
//! serving rejects the creation and the walk simply moves on to the next cell.
//!
//! The candidates are all the cells of the bundle, not its |metadata_cell_ids|: the latter is at
//! most a pair of sibling cells dedicated to replication cards, and of a sibling pair only one
//! serves leases at a time, so rotating over it spreads nothing.
//!
//! Which cells serve leases is learnt from their answers rather than assumed: a cell that rejects
//! a creation is remembered and moved to the back of the rotation until the next list refresh, so
//! the steady state costs no wasted requests, and a cell that starts serving (leases migrate
//! between siblings) is still reached and promoted back.
//!
//! The cell list is cached for |cellIdsExpirationTime| and refetched afterwards, so that cells
//! added to the bundle join the rotation and removed ones leave it. Waiting for the whole cached
//! list to reject a creation would not do: as long as one cached cell still accepts, the list is
//! never questioned.
//!
//! Thread affinity: any.
class TChaosLeaseFactory
    : public TRefCounted
{
public:
    TChaosLeaseFactory(
        NApi::IClientPtr client,
        std::string chaosCellBundle,
        TDuration cellIdsExpirationTime = TDuration::Minutes(1));

    //! Creates a lease that expires |timeout| after the last ping.
    //! Fails if no metadata cell of the bundle accepts the creation.
    TFuture<NChaosClient::TChaosLeaseId> CreateLease(
        TDuration timeout,
        NYTree::IAttributeDictionaryPtr attributes = {});

private:
    const NApi::IClientPtr Client_;
    const std::string ChaosCellBundle_;
    const TDuration CellIdsExpirationTime_;

    std::atomic<ui64> NextCellIndex_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CellIdsLock_);
    std::vector<NObjectClient::TCellId> CellIds_;
    //! Cells last seen rejecting a creation; cleared whenever the cell list is refetched.
    THashSet<NObjectClient::TCellId> NotServingCellIds_;
    TInstant CellIdsDeadline_ = TInstant::Zero();
    //! The refetch in flight, if any; concurrent creations share it instead of each hitting Cypress.
    TFuture<std::vector<NObjectClient::TCellId>> CellIdsFuture_;

    void OnCellServes(NObjectClient::TCellId cellId);
    void OnCellDoesNotServe(NObjectClient::TCellId cellId);

    TFuture<std::vector<NObjectClient::TCellId>> GetCellIds(bool forceRefresh);
    std::vector<NObjectClient::TCellId> RotateCells(std::vector<NObjectClient::TCellId> cellIds);
    TFuture<NChaosClient::TChaosLeaseId> CreateLeaseOnCells(
        std::vector<NObjectClient::TCellId> cellIds,
        int index,
        TDuration timeout,
        NYTree::IAttributeDictionaryPtr attributes,
        bool refreshed);
};

DEFINE_REFCOUNTED_TYPE(TChaosLeaseFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosElection
