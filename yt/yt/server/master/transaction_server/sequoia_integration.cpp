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
    return std::pair(
        CreateResponseMessage(rsp),
        NLogging::TLoggingTagList().With("TransactionId", transactionId));
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
    context->ReplyAndLogFrom(
        StartCypressTransaction(
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
    std::vector<TTransactionId> allTransactionIds,
    std::unique_ptr<NProto::TReqReturnBoomerang> boomerang,
    NCypressClient::TNodeId sequoiaNodeIdToLock,
    bool useSeparateSequoiaTransactionPerCoordinator)
{
    auto features = GetSequoiaTransactionFeatures(bootstrap);

    auto needWaitUntilPreparedTransactionsFinished = false;

    auto doReplicateCypressTransactions = [&] (
        std::vector<TTransactionId> transactionIds,
        std::unique_ptr<NProto::TReqReturnBoomerang> boomerang) {

        TCellId cypressTransactionCoordinatorCellId = {};
        if (!transactionIds.empty()) {
            const auto& multicellManager = bootstrap->GetMulticellManager();
            cypressTransactionCoordinatorCellId = multicellManager->GetCellId(CellTagFromId(transactionIds.front()));
        }

        auto [result, sequoiaTransactionCoordinatorCellId] = ReplicateCypressTransactionsToCell(
            bootstrap
                ->GetSequoiaConnection()
                ->CreateClient(GetRootAuthenticationIdentity()),
            std::move(transactionIds),
            bootstrap->GetCellId(),
            std::move(boomerang),
            sequoiaNodeIdToLock,
            cypressTransactionCoordinatorCellId,
            features,
            TDispatcher::Get()->GetHeavyInvoker(),
            TransactionServerLogger());

        if (sequoiaTransactionCoordinatorCellId && sequoiaTransactionCoordinatorCellId != bootstrap->GetCellId()) {
            needWaitUntilPreparedTransactionsFinished = true;
        }

        return result;
    };

    auto replicationFuture = OKFuture;

    if (useSeparateSequoiaTransactionPerCoordinator && !allTransactionIds.empty()) {
        std::vector<TFuture<void>> asyncResults;

        THashMap<TCellTag, std::vector<TTransactionId>> cellTagToTransactionIds;
        for (auto transactionId : allTransactionIds) {
            cellTagToTransactionIds[CellTagFromId(transactionId)].push_back(transactionId);
        }

        for (auto& [cellTag, cellTransactionIds] : cellTagToTransactionIds) {
            std::unique_ptr<NProto::TReqReturnBoomerang> boomerangCopy;
            if (boomerang) {
                boomerangCopy = std::make_unique<NProto::TReqReturnBoomerang>(*boomerang);
            }
            asyncResults.push_back(doReplicateCypressTransactions(
                std::move(cellTransactionIds),
                std::move(boomerangCopy)));
        }

        replicationFuture = AllSucceeded(asyncResults);
    } else { // COMPAT(shakurov)
        replicationFuture = doReplicateCypressTransactions(
            std::move(allTransactionIds),
            std::move(boomerang));
    }

    return replicationFuture
        .Apply(BIND([
            hydraManager = bootstrap->GetHydraFacade()->GetHydraManager(),
            needWaitUntilPreparedTransactionsFinished,
            transactionManager = bootstrap->GetTransactionManager()
        ] {
            // NB: |sequoiaTransaction->Commit()| is set when Sequoia tx is
            // prepared on leader (and probably some of followers). Since we
            // want to know when replicated tx is actually available on _this_
            // peer, sync with leader is needed.
            // Additionally, it may be necessary to wait for strongly ordered tx barrier
            // if Sequoia transaction is not coordinated by the current (i.e. local) cell.
            // (If it is, no waiting is necessary thanks to late prepare.)
            auto future = hydraManager->SyncWithLeader();

            if (needWaitUntilPreparedTransactionsFinished) {
                future = future.Apply(BIND([transactionManager] {
                    return transactionManager->WaitUntilPreparedTransactionsFinished({NApi::NNative::SequoiaCypressOrderingTag});
                }));
            }

            return future;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
