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
//! Creations go round-robin over all the cells of the bundle: a lease creation is a Hydra mutation
//! and every lease is later pinged for its whole lifetime, so pinning one cell would turn it into a
//! hot spot once the leases are counted in tens of thousands.
//!
//! Only a part of the bundle's cells serves leases, and which ones is learnt from their answers
//! rather than assumed: a cell that rejects a creation is moved to the back of the rotation, and a
//! cell that starts serving is reached again and promoted back.
//!
//! The cell list is cached for |cellIdsExpirationTime| and refetched afterwards, so that cells
//! added to the bundle join the rotation and removed ones leave it.
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
    //! Fails if no cell of the bundle accepts the creation.
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
    THashSet<NObjectClient::TCellId> NotEnabledCellIds_;
    TInstant CellIdsDeadline_ = TInstant::Zero();
    //! The refetch in flight, if any; concurrent creations share it instead of each hitting Cypress.
    TFuture<std::vector<NObjectClient::TCellId>> CellIdsFuture_;

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
