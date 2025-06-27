#include "sequoia_transaction_service.h"

#include "helpers.h"
#include "private.h"
#include "sequoia_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/master/sequoia_server/ground_update_queue_manager.h>

#include <yt/yt/ytlib/sequoia_client/transaction_service_proxy.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTransactionService
    : public TMasterHydraServiceBase
{
public:
    explicit TSequoiaTransactionService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TSequoiaTransactionServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::Default,
            SequoiaServerLogger())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncWithGroundUpdateQueue)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::GroundUpdateQueueManager)));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NSequoiaClient::NProto, StartTransaction)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        auto sequoiaReign = FromProto<ESequoiaReign>(request->sequoia_reign());

        auto attributes = NYTree::FromProto(request->attributes());
        auto title = attributes->Find<std::string>("title");

        context->SetRequestInfo("TransactionId: %v, Timeout: %v, SequoiaReign: %v, Title: %v",
            FromProto<TTransactionId>(request->id()),
            FromProto<TDuration>(request->timeout()),
            sequoiaReign,
            title);

        ValidateSequoiaReign(sequoiaReign);

        const auto& sequoiaManager = Bootstrap_->GetSequoiaManager();
        sequoiaManager->StartTransaction(request);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NSequoiaClient::NProto, SyncWithGroundUpdateQueue)
    {
        // TODO(danilalexeev): Give the SequoiaTransactionService a more generic name.
        ValidateClusterInitialized();

        context->SetRequestInfo();

        const auto& queueManager = Bootstrap_->GetGroundUpdateQueueManager();
        context->ReplyFrom(
            queueManager->Sync(NSequoiaClient::EGroundUpdateQueue::Sequoia));
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateSequoiaTransactionService(TBootstrap* bootstrap)
{
    return New<TSequoiaTransactionService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
