#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/lock.h>

#include <yt/yt/server/master/transaction_server/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

struct IPrelockTracker
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    // Performs _no_ checks. Use ICypressManager::AcquirePrelock() instead.
    virtual void AcquirePrelockUnchecked(
        NTransactionServer::TTransaction* owningTransaction,
        NTransactionServer::TTransaction* lockingTransaction,
        NCypressServer::TNodeId nodeId,
        const NCypressServer::TLockRequest& lockRequest) = 0;

    virtual TError CheckLock(
        NTransactionServer::TTransaction* lockingTransaction,
        NCypressServer::TNodeId nodeId,
        const NCypressServer::TLockRequest& lockRequest) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IPrelockTracker)

IPrelockTrackerPtr CreatePrelockTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
