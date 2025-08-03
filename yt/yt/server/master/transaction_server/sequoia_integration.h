#pragma once

#include "transaction_manager.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

// NB: modifies original RPC request.
void StartCypressTransactionInSequoiaAndReply(
    NCellMaster::TBootstrap* bootstrap,
    const ITransactionManager::TCtxStartCypressTransactionPtr& context);

TFuture<TSharedRefArray> AbortCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId,
    bool force,
    NRpc::TAuthenticationIdentity authenticationIdentity);

TFuture<TSharedRefArray> AbortExpiredCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId);

TFuture<TSharedRefArray> CommitCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId,
    std::vector<TTransactionId> prerequisiteTransactionIds,
    TTimestamp commitTimestamp,
    NRpc::TAuthenticationIdentity authenticationIdentity);

//! Replicates given Cypress transactions from coordinator to this cell.
TFuture<void> ReplicateCypressTransactionsInSequoiaAndSyncWithLeader(
    NCellMaster::TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
