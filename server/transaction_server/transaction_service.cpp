#include "transaction_service.h"
#include "transaction_manager.h"
#include "private.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/master_hydra_service.h>

#include <yt/server/transaction_server/transaction_manager.pb.h>

#include <yt/ytlib/transaction_client/transaction_service_proxy.h>

namespace NYT {
namespace NTransactionServer {

using namespace NRpc;
using namespace NTransactionClient;
using namespace NHydra;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TTransactionService
    : public TMasterHydraServiceBase
{
public:
    explicit TTransactionService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TTransactionServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::TransactionSupervisor,
            TransactionServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterTransactionActions));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, StartTransaction)
    {
        ValidatePeer(EPeerKind::Leader);

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto timeout = FromProto<TDuration>(request->timeout());
        auto title = request->has_title() ? MakeNullable(request->title()) : Null;
        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());

        context->SetRequestInfo("ParentId: %v, PrerequisiteTransactionIds: %v, Timeout: %v, Title: %v",
            parentId,
            prerequisiteTransactionIds,
            timeout,
            title);

        NTransactionServer::NProto::TReqStartTransaction hydraRequest;
        hydraRequest.mutable_attributes()->Swap(request->mutable_attributes());
        hydraRequest.mutable_parent_id()->Swap(request->mutable_parent_id());
        hydraRequest.mutable_prerequisite_transaction_ids()->Swap(request->mutable_prerequisite_transaction_ids());
        hydraRequest.set_timeout(request->timeout());
        hydraRequest.set_user_name(context->GetUser());
        hydraRequest.mutable_hint_id()->Swap(request->mutable_hint_id());
        hydraRequest.mutable_replicate_to_cell_tags()->Swap(request->mutable_replicate_to_cell_tags());

        if (title) {
            hydraRequest.set_title(*title);
        }

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager
            ->CreateStartTransactionMutation(context, hydraRequest)
            ->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, RegisterTransactionActions)
    {
        ValidatePeer(EPeerKind::Leader);

        SyncWithUpstream();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v, ActionCount: %v",
            transactionId,
            request->actions_size());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager
            ->CreateRegisterTransactionActionsMutation(context)
            ->CommitAndReply(context);
    }
};

IServicePtr CreateTransactionService(TBootstrap* bootstrap)
{
    return New<TTransactionService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
