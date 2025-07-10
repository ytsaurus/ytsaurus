#include "cypress_transaction_service.h"

#include "transaction_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/lib/hydra/persistent_response_keeper.h>

#include <yt/yt/server/lib/transaction_server/private.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

#include <yt/yt/ytlib/cypress_transaction_client/cypress_transaction_service_proxy.h>
#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressTransactionClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TCypressTransactionService
    : public TMasterHydraServiceBase
{
public:
    explicit TCypressTransactionService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TCypressTransactionServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::CypressTransactionService,
            TransactionServerLogger())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction)
            .SetInvoker(GetSyncInvoker()));

        DeclareServerFeature(EMasterFeature::PortalExitSynchronization);
    }

private:
    void SyncWithSequoiaTransactions()
    {
        const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        WaitFor(transactionSupervisor->WaitUntilPreparedTransactionsFinished())
            .ThrowOnError();
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressTransactionClient::NProto, StartTransaction)
    {
        ValidatePeer(EPeerKind::Leader);

        // NB: No upstream sync should be necessary here.

        auto title = request->has_title() ? std::optional(request->title()) : std::nullopt;
        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        auto replicateToCellTags = FromProto<TCellTagList>(request->replicate_to_cell_tags());
        auto timeout = FromProto<TDuration>(request->timeout());
        auto deadline = request->has_deadline() ? std::optional(FromProto<TInstant>(request->deadline())) : std::nullopt;

        context->SetRequestInfo("Title: %v, ParentId: %v, PrerequisiteTransactionIds: %v, ReplicateToCellTags: %v, Timeout: %v, Deadline: %v",
            title,
            parentId,
            prerequisiteTransactionIds,
            replicateToCellTags,
            timeout,
            deadline);

        SyncWithSequoiaTransactions();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->StartCypressTransaction(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressTransactionClient::NProto, CommitTransaction)
    {
        ValidatePeer(EPeerKind::Leader);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto prerequisiteTransactionIds = GetPrerequisiteTransactionIds(context->GetRequestHeader());

        context->SetRequestInfo("TransactionId: %v, PrerequisiteTransactionIds: %v",
            transactionId,
            prerequisiteTransactionIds);

        if (context->GetMutationId()) {
            const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
            if (auto result = responseKeeper->FindRequest(context->GetMutationId(), context->IsRetry())) {
                context->ReplyFrom(std::move(result));
                return;
            }
        }

        SyncWithSequoiaTransactions();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->CommitCypressTransaction(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressTransactionClient::NProto, AbortTransaction)
    {
        ValidatePeer(EPeerKind::Leader);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        bool force = request->force();

        context->SetRequestInfo("TransactionId: %v, Force: %v",
            transactionId,
            force);

        if (context->GetMutationId()) {
            const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
            if (auto result = responseKeeper->FindRequest(context->GetMutationId(), context->IsRetry())) {
                context->ReplyFrom(std::move(result));
                return;
            }
        }

        SyncWithSequoiaTransactions();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->AbortCypressTransaction(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressTransactionClient::NProto, PingTransaction)
    {
        ValidatePeer(EPeerKind::Leader);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        bool pingAncestors = request->ping_ancestors();

        context->SetRequestInfo("TransactionId: %v, PingAncestors: %v",
            transactionId,
            pingAncestors);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        context->ReplyFrom(transactionManager->PingTransaction(transactionId, pingAncestors));
    }
};

IServicePtr CreateCypressTransactionService(TBootstrap* bootstrap)
{
    return New<TCypressTransactionService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
