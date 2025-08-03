#pragma once

#include <yt/yt/ytlib/cypress_transaction_client/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

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
    NRpc::TAuthenticationIdentity authenticationIdentity,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

//! Aborts Cypress transaction when abort is requested by user.
/*!
 *  NB: modifies #request.
 */
TFuture<void> AbortCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NObjectClient::TCellId cypressTransactionCoordinatorCellId,
    NCypressClient::TTransactionId transactionId,
    bool force,
    NRpc::TAuthenticationIdentity authenticationIdentity,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

//! Aborts expired Cypress transaction. Similar to |AbortCypressTransaction()|,
//! but log message is different.
TFuture<void> AbortExpiredCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NObjectClient::TCellId cypressTransactionCoordinatorCellId,
    NTransactionClient::TTransactionId transactionId,
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
 *  NB: modifies #request.
 */
TFuture<void> CommitCypressTransaction(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    NObjectClient::TCellId cypressTransactionCoordinatorCellId,
    NTransactionClient::TTransactionId transactionId,
    std::vector<NTransactionClient::TTransactionId> prerequisiteTransactionIds,
    NTransactionClient::TTimestamp commitTimestamp,
    NRpc::TAuthenticationIdentity authenticationIdentity,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

// NB: The common case is the lazy replication from transaction coordinator
// which is initiated on foreign cell. In this case destination cell is the only
// destination, thus typical count is 1.
constexpr int TypicalTransactionReplicationDestinationCellCount = 1;
using TTransactionReplicationDestinationCellTagList =
    TCompactVector<NObjectClient::TCellTag, TypicalTransactionReplicationDestinationCellCount>;

//! Checks that given Cypress transactions are replicated to the cell and
//! registers Sequoia tx actions if needed. Returns future which is set when all
//! necessary checks are performed and Sequoia transaction is committed.
TFuture<void> ReplicateCypressTransactions(
    NSequoiaClient::ISequoiaClientPtr sequoiaClient,
    std::vector<NTransactionClient::TTransactionId> transactionIds,
    TTransactionReplicationDestinationCellTagList destinationCellTags,
    NObjectClient::TCellId hintCoordinatorCellId,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
