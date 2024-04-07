#pragma once

#include "transaction_manager.h"

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

// NB: modifies original RPC request.
void StartCypressTransactionInSequoiaAndReply(
    NCellMaster::TBootstrap* bootstrap,
    const ITransactionManager::TCtxStartCypressTransactionPtr& context);

void AbortCypressTransactionInSequoiaAndReply(
    NCellMaster::TBootstrap* bootstrap,
    const ITransactionManager::TCtxAbortCypressTransactionPtr& context);

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
    TRange<TTransactionId> transactionIds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
