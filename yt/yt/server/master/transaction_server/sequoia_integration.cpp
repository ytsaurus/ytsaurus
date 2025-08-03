#include "sequoia_integration.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/lib/sequoia/cypress_transaction.h>

#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/client/hive/timestamp_map.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NRpc;
using namespace NSequoiaServer;

////////////////////////////////////////////////////////////////////////////////

namespace {

const auto CreateStartTransactionResponse = BIND_NO_PROPAGATE([] (TTransactionId transactionId) {
    NProto::TRspStartCypressTransaction rsp;
    ToProto(rsp.mutable_id(), transactionId);
    return CreateResponseMessage(rsp);
});

const auto CreateAbortTransactionResponse = BIND_NO_PROPAGATE([] () {
    return CreateResponseMessage(NCypressTransactionClient::NProto::TRspAbortTransaction{});
});

} // namespace

////////////////////////////////////////////////////////////////////////////////

void StartCypressTransactionInSequoiaAndReply(
    TBootstrap* bootstrap,
    const ITransactionManager::TCtxStartCypressTransactionPtr& context)
{
    context->ReplyFrom(StartCypressTransaction(
        bootstrap->GetSequoiaClient(),
        bootstrap->GetCellId(),
        &context->Request(),
        context->GetAuthenticationIdentity(),
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger())
        .Apply(CreateStartTransactionResponse));
}

TFuture<TSharedRefArray> AbortCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId,
    bool force,
    TAuthenticationIdentity authenticationIdentity)
{
    return AbortCypressTransaction(
        bootstrap->GetSequoiaClient(),
        bootstrap->GetCellId(),
        transactionId,
        force,
        authenticationIdentity,
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger())
        .Apply(CreateAbortTransactionResponse);
}

TFuture<TSharedRefArray> AbortExpiredCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId)
{
    return AbortExpiredCypressTransaction(
        bootstrap->GetSequoiaClient(),
        bootstrap->GetCellId(),
        transactionId,
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger())
        .Apply(CreateAbortTransactionResponse);
}

TFuture<TSharedRefArray> CommitCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId,
    std::vector<TTransactionId> prerequisiteTransactionIds,
    TTimestamp commitTimestamp,
    NRpc::TAuthenticationIdentity authenticationIdentity)
{
    return CommitCypressTransaction(
        bootstrap->GetSequoiaClient(),
        bootstrap->GetCellId(),
        transactionId,
        std::move(prerequisiteTransactionIds),
        commitTimestamp,
        std::move(authenticationIdentity),
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger())
        .Apply(BIND_NO_PROPAGATE([=] () {
            NCypressTransactionClient::NProto::TRspCommitTransaction rsp;
            NHiveClient::TTimestampMap timestampMap;
            timestampMap.Timestamps.emplace_back(bootstrap->GetPrimaryCellTag(), commitTimestamp);
            ToProto(rsp.mutable_commit_timestamps(), timestampMap);
            return NRpc::CreateResponseMessage(rsp);
        }));
}

TFuture<void> ReplicateCypressTransactionsInSequoiaAndSyncWithLeader(
    NCellMaster::TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds)
{
    return ReplicateCypressTransactions(
        bootstrap->GetSequoiaClient(),
        std::move(transactionIds),
        {bootstrap->GetCellTag()},
        bootstrap->GetCellId(),
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger())
        .Apply(BIND([hydraManager = bootstrap->GetHydraFacade()->GetHydraManager()] {
            // NB: |sequoiaTransaction->Commit()| is set when Sequoia tx is
            // committed on leader (and probably some of followers). Since we
            // want to know when replicated tx is actually available on _this_
            // peer sync with leader is needed.
            return hydraManager->SyncWithLeader();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
