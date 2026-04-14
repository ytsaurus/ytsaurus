#pragma once

#include "public.h"

#include "transaction_finish_request.h"

#include <yt/yt/ytlib/cypress_transaction_client/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>
#include <yt/yt/ytlib/sequoia_client/transaction_options.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

//! Starts Cypress transaction on a given cell.
/*!
 *  NB: modifies #request.
 */
TFuture<NTransactionClient::TTransactionId> StartCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NObjectClient::TCellId cypressTransactionCoordinatorCellId,
    NCypressTransactionClient::NProto::TReqStartTransaction* request,
    NSequoiaClient::TSequoiaTransactionFeatures features,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

//! Mark Cypress transaction and its successor transactions as doomed;
//! revoke leases of those transactions.
//! This should be done before Abort/Commit to avoid write starvation
//! in "transactions" table in Sequoia.
//! See TDoomCypressTransaction for details.
TFuture<void> DoomCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NObjectClient::TCellId cypressTransactionCoordinatorCellId,
    NCypressClient::TTransactionId transactionId,
    const NTransactionServer::NProto::TTransactionFinishRequest& request,
    NSequoiaClient::TSequoiaTransactionFeatures features,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

//! Aborts Cypress transaction when abort is requested by user. Returns
//! serialized |TRspAbortTransaction|.
/*!
 *  NB: modifies #request.
 */
TFuture<TSharedRefArray> AbortCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NObjectClient::TCellId cypressTransactionCoordinatorCellId,
    NCypressClient::TTransactionId transactionId,
    bool force,
    NRpc::TMutationId mutationId,
    bool retry,
    NSequoiaClient::TSequoiaTransactionFeatures features,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

//! Aborts expired Cypress transaction. Similar to |AbortCypressTransaction()|,
//! but log message is different.
TFuture<TSharedRefArray> AbortExpiredCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NObjectClient::TCellId cypressTransactionCoordinatorCellId,
    NTransactionClient::TTransactionId transactionId,
    NSequoiaClient::TSequoiaTransactionFeatures features,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

//! Commits Cypress transactions.
/*!
 *  Note that commit timestamp has to be generated _before_ tx commit. Of
 *  course, it can lead to commit reordering, but it doesn't matter here: the
 *  only known usage of Cypress tx's commit timestamp is bulk insert, which
 *  needs some timestamp before tx's commit but after every action under the
 *  given Cypress tx.
 *
 *  Returns serialized |TRspCommitTransaction|.
 *
 *  NB: modifies #request.
 */
TFuture<TSharedRefArray> CommitCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NObjectClient::TCellId cypressTransactionCoordinatorCellId,
    NTransactionClient::TTransactionId transactionId,
    std::vector<NTransactionClient::TTransactionId> prerequisiteTransactionIds,
    NObjectClient::TCellTag primaryCellTag,
    NTransactionClient::TTimestamp commitTimestamp,
    NRpc::TMutationId mutationId,
    bool retry,
    NSequoiaClient::TSequoiaTransactionFeatures features,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

//! Even if transaction is not alive from master point of view it still
//! necessary to check Sequoia response keeper to handle commit/abort request.
//! Returns either kept response message or response message with "no such
//! transaction" error.
TFuture<TSharedRefArray> FinishNonAliveCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NTransactionClient::TTransactionId transactionId,
    NRpc::TMutationId mutationId,
    bool retry,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

//! Starts replication of active Cypress transactions to the cell and returns
//! the future which is set when replication is done at leader of destination
//! master cell. If #boomerang is not null it's executed on the destination cell
//! in the same mutation as Cypress transaction materialization.
TFuture<void> ReplicateCypressTransactionsToCell(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    std::vector<NTransactionClient::TTransactionId> transactionIds,
    NObjectClient::TCellId destinationCellId,
    std::unique_ptr<NTransactionServer::NProto::TReqReturnBoomerang> boomerang,
    NSequoiaClient::TSequoiaTransactionFeatures features,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

//! Starts replication of active Cypress transaction and returns the future
//! which is set when Sequoia transaction is prepared on all of destination
//! master cells. Since transaction may still be uncommitted it's necessary to
//! wait for Sequoia transaction barrier to observe the result.
TFuture<void> ReplicateCypressTransactionToCells(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NTransactionClient::TTransactionId transactionId,
    NObjectClient::TCellTagList destinationCellTags,
    NObjectClient::TCellId transactionCoordinatorCellId,
    NSequoiaClient::TSequoiaTransactionFeatures features,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

bool ShouldMirrorTransactionAttributeToSequoia(const std::string& attributeName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
