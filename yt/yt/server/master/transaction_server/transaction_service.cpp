#include "transaction_service.h"
#include "transaction_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TTransactionService
    : public TMasterHydraServiceBase
{
public:
    explicit TTransactionService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TTransactionServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::TransactionService,
            TransactionServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterTransactionActions)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReplicateTransactions)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(IssueLeases)
            .SetHeavy(true));

        DeclareServerFeature(EMasterFeature::PortalExitSynchronization);
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, StartTransaction)
    {
        ValidatePeer(EPeerKind::Leader);

        // NB: no upstream sync should be necessary here.

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto timeout = FromProto<TDuration>(request->timeout());
        auto deadline = request->has_deadline() ? std::make_optional(FromProto<TInstant>(request->deadline())) : std::nullopt;
        auto title = request->has_title() ? std::make_optional(request->title()) : std::nullopt;
        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        auto isCypressTransaction = request->is_cypress_transaction();

        context->SetRequestInfo(
            "ParentId: %v, PrerequisiteTransactionIds: %v, Timeout: %v, "
            "Title: %v, Deadline: %v, IsCypressTransaction: %v",
            parentId,
            prerequisiteTransactionIds,
            timeout,
            title,
            deadline,
            isCypressTransaction);

        NTransactionServer::NProto::TReqStartTransaction hydraRequest;
        hydraRequest.mutable_attributes()->Swap(request->mutable_attributes());
        hydraRequest.mutable_parent_id()->Swap(request->mutable_parent_id());
        hydraRequest.mutable_prerequisite_transaction_ids()->Swap(request->mutable_prerequisite_transaction_ids());
        hydraRequest.set_timeout(request->timeout());
        if (request->has_deadline()) {
            hydraRequest.set_deadline(request->deadline());
        }
        hydraRequest.mutable_replicate_to_cell_tags()->Swap(request->mutable_replicate_to_cell_tags());
        if (title) {
            hydraRequest.set_title(*title);
        }
        hydraRequest.set_is_cypress_transaction(isCypressTransaction);
        NRpc::WriteAuthenticationIdentityToProto(&hydraRequest, context->GetAuthenticationIdentity());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto mutation = transactionManager->CreateStartTransactionMutation(context, hydraRequest);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, RegisterTransactionActions)
    {
        ValidatePeer(EPeerKind::Leader);

        // Wait for transaction to appear on secondary master.
        SyncWithUpstream();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v, ActionCount: %v",
            transactionId,
            request->actions_size());

        auto cellTag = CellTagFromId(transactionId);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellId = multicellManager->GetCellId(cellTag);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        WaitFor(hiveManager->SyncWith(cellId, true))
            .ThrowOnError();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto mutation = transactionManager->CreateRegisterTransactionActionsMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, ReplicateTransactions)
    {
        ValidatePeer(EPeerKind::Leader);

        // NB: no sync with upstream should be necessary here.

        auto transactionIds = FromProto<std::vector<TTransactionId>>(request->transaction_ids());
        auto destinationCellTag = request->destination_cell_tag();
        auto boomerangWaveId = FromProto<TBoomerangWaveId>(request->boomerang_wave_id());
        auto boomerangWaveSize = request->boomerang_wave_size();
        auto boomerangMutationId = FromProto<TMutationId>(request->boomerang_mutation_id());

        context->SetRequestInfo("TransactionIds: %v, DestinationCellTag: %v, BoomerangWaveId: %v, BoomerangWaveSize: %v, BoomerangMutationId: %v)",
            transactionIds,
            destinationCellTag,
            boomerangWaveId,
            boomerangWaveSize,
            boomerangMutationId);

        ValidateTransactionsAreCoordinatedByThisCell(transactionIds);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto mutation = transactionManager->CreateReplicateTransactionsMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, IssueLeases)
    {
        ValidatePeer(EPeerKind::Leader);

        auto transactionIds = FromProto<std::vector<TTransactionId>>(request->transaction_ids());
        auto cellId = FromProto<TCellId>(request->cell_id());

        context->SetRequestInfo("TransactionIds: %v, CellId: %v",
            transactionIds,
            cellId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto mutation = transactionManager->CreateIssueLeasesMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void ValidateTransactionsAreCoordinatedByThisCell(const std::vector<TTransactionId>& transactionIds)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellTag = multicellManager->GetCellTag();

        for (auto transactionId : transactionIds) {
            if (CellTagFromId(transactionId) != cellTag) {
                THROW_ERROR_EXCEPTION("Transaction is not coordinated by this cell (TransactionId: %v, CellTag: %v)",
                    transactionId,
                    cellTag);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTransactionService(TBootstrap* bootstrap)
{
    return New<TTransactionService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
