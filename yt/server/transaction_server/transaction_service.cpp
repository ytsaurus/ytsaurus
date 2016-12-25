#include "transaction_service.h"
#include "transaction_manager.h"
#include "private.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/master_hydra_service.h>

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
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, StartTransaction)
    {
        ValidatePeer(EPeerKind::Leader);

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto timeout = FromProto<TDuration>(request->timeout());

        context->SetRequestInfo("ParentId: %v, Timeout: %v",
            parentId,
            timeout);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager
            ->CreateStartTransactionMutation(context)
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
