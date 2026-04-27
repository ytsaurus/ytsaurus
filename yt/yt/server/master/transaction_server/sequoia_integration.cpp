#include "sequoia_integration.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/sequoia_server/sequoia_manager.h>

#include <yt/yt/server/lib/sequoia/cypress_transaction.h>

#include <yt/yt/server/lib/sequoia/proto/transaction_manager.pb.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/ytlib/sequoia_client/connection.h>

#include <yt/yt/client/hive/timestamp_map.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NObjectClient;
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

NSequoiaClient::TSequoiaTransactionFeatures GetSequoiaTransactionFeatures(TBootstrap* bootstrap)
{
    const auto& sequoiaManager = bootstrap->GetSequoiaManager();
    return sequoiaManager->GetSequoiaTransactionFeatures();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void StartCypressTransactionInSequoiaAndReply(
    TBootstrap* bootstrap,
    const ITransactionManager::TCtxStartCypressTransactionPtr& context)
{
    context->ReplyFrom(StartCypressTransaction(
        bootstrap
            ->GetSequoiaConnection()
            ->CreateClient(context->GetAuthenticationIdentity()),
        bootstrap->GetCellId(),
        &context->Request(),
        GetSequoiaTransactionFeatures(bootstrap),
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger())
        .Apply(CreateStartTransactionResponse));
}

TFuture<void> DoomCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId,
    TAuthenticationIdentity authenticationIdentity,
    const NProto::TTransactionFinishRequest& request)
{
    return DoomCypressTransaction(
        bootstrap
            ->GetSequoiaConnection()
            ->CreateClient(std::move(authenticationIdentity)),
        bootstrap->GetCellId(),
        transactionId,
        request,
        GetSequoiaTransactionFeatures(bootstrap),
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger());
}

TFuture<TSharedRefArray> AbortCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId,
    bool force,
    TAuthenticationIdentity authenticationIdentity,
    TMutationId mutationId,
    bool retry)
{
    return AbortCypressTransaction(
        bootstrap
            ->GetSequoiaConnection()
            ->CreateClient(std::move(authenticationIdentity)),
        bootstrap->GetCellId(),
        transactionId,
        force,
        mutationId,
        retry,
        GetSequoiaTransactionFeatures(bootstrap),
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger());
}

TFuture<TSharedRefArray> AbortExpiredCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId)
{
    return AbortExpiredCypressTransaction(
        bootstrap
            ->GetSequoiaConnection()
            ->CreateClient(GetRootAuthenticationIdentity()),
        bootstrap->GetCellId(),
        transactionId,
        GetSequoiaTransactionFeatures(bootstrap),
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger());
}

TFuture<TSharedRefArray> CommitCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId,
    std::vector<TTransactionId> prerequisiteTransactionIds,
    TTimestamp commitTimestamp,
    NRpc::TAuthenticationIdentity authenticationIdentity,
    TMutationId mutationId,
    bool retry)
{
    return CommitCypressTransaction(
        bootstrap
            ->GetSequoiaConnection()
            ->CreateClient(std::move(authenticationIdentity)),
        bootstrap->GetCellId(),
        transactionId,
        std::move(prerequisiteTransactionIds),
        bootstrap->GetPrimaryCellTag(),
        commitTimestamp,
        mutationId,
        retry,
        GetSequoiaTransactionFeatures(bootstrap),
        TDispatcher::Get()->GetHeavyInvoker(),
        TransactionServerLogger());
}

TFuture<TSharedRefArray> FinishNonAliveCypressTransactionInSequoia(
    NCellMaster::TBootstrap* bootstrap,
    TTransactionId transactionId,
    NRpc::TMutationId mutationId,
    bool retry)
{
    return FinishNonAliveCypressTransaction(
        bootstrap
            ->GetSequoiaConnection()
            ->CreateClient(GetRootAuthenticationIdentity()),
        transactionId,
        mutationId,
        retry,
        TransactionServerLogger());
}

TFuture<void> ReplicateCypressTransactionsInSequoiaAndSyncWithLeader(
    NCellMaster::TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds)
{
    auto features = GetSequoiaTransactionFeatures(bootstrap);

    TCellId coordinatorCellId = bootstrap->GetCellId();
    if (!transactionIds.empty()) {
        const auto& multicellManager = bootstrap->GetMulticellManager();
        coordinatorCellId = multicellManager->GetCellId(CellTagFromId(transactionIds.front()));
    }

    auto replicationFuture = OKFuture;
    if (!transactionIds.empty()) {
        replicationFuture = ReplicateCypressTransactions(
            bootstrap
                ->GetSequoiaConnection()
                ->CreateClient(GetRootAuthenticationIdentity()),
            std::move(transactionIds),
            {bootstrap->GetCellTag()},
            coordinatorCellId,
            features,
            TDispatcher::Get()->GetHeavyInvoker(),
            TransactionServerLogger());
    }

    return replicationFuture
        .Apply(BIND([
            hydraManager = bootstrap->GetHydraFacade()->GetHydraManager(),
            latePrepare = coordinatorCellId == bootstrap->GetCellId(),
            transactionSupervisor = bootstrap->GetTransactionSupervisor()
        ] {
            // NB: |sequoiaTransaction->Commit()| is set when Sequoia tx is
            // prepared on leader (and probably some of followers). Since we
            // want to know when replicated tx is actually available on _this_
            // peer sync with leader is needed.
            // Note that waiting for strongly ordered tx barrier isn't needed
            // here because Sequoia transaction is coordinated by current cell:
            // thanks to late prepare mode after transaction is prepared on
            // coordinator its effects can be immediately observed on
            // coordinator.
            auto future = hydraManager->SyncWithLeader();

            if (!latePrepare) {
                future = future.Apply(BIND([transactionSupervisor] {
                    return transactionSupervisor->WaitUntilPreparedTransactionsFinished();
                }));
            }

            return future;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
