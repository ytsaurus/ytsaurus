#pragma once

#include "transaction_manager.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

namespace NYT::NTransactionServer {

namespace NProto {

class TTransactionFinishRequest;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

// NB: modifies original RPC request.
void StartCypressTransactionInSequoiaAndReply(
    NCellMaster::TBootstrap* bootstrap,
    const ITransactionManager::TCtxStartCypressTransactionPtr& context);

TFuture<void> DoomCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId,
    NRpc::TAuthenticationIdentity authenticationIdentity,
    const NProto::TTransactionFinishRequest& request);

TFuture<TSharedRefArray> AbortCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId,
    bool force,
    NRpc::TAuthenticationIdentity authenticationIdentity,
    NRpc::TMutationId mutationId,
    bool retry);

TFuture<TSharedRefArray> AbortExpiredCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId);

TFuture<TSharedRefArray> CommitCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId,
    std::vector<TTransactionId> prerequisiteTransactionIds,
    TTimestamp commitTimestamp,
    NRpc::TAuthenticationIdentity authenticationIdentity,
    NRpc::TMutationId mutationId,
    bool retry);

TFuture<TSharedRefArray> FinishNonAliveCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId,
    NRpc::TMutationId mutationId,
    bool retry);

//! Starts replication of Cypress transactions from coordinator to this cell and
//! returns the future which is set when all transactions can be accessed on
//! current master _peer_.
TFuture<void> ReplicateCypressTransactionsInSequoiaAndSyncWithLeader(
    NCellMaster::TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
