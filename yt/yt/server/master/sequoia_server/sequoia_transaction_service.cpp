#include "sequoia_transaction_service.h"

#include "helpers.h"
#include "private.h"
#include "sequoia_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/ytlib/sequoia_client/transaction_service_proxy.h>

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
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NSequoiaClient::NProto, StartTransaction)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        auto sequoiaReign = FromProto<ESequoiaReign>(request->sequoia_reign());

        context->SetRequestInfo("TransactionId: %v, Timeout: %v, SequoiaReign: %v",
            FromProto<TTransactionId>(request->id()),
            FromProto<TDuration>(request->timeout()),
            sequoiaReign);

        ValidateSequoiaReign(sequoiaReign);

        const auto& sequoiaManager = Bootstrap_->GetSequoiaManager();
        sequoiaManager->StartTransaction(request);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateSequoiaTransactionService(TBootstrap* bootstrap)
{
    return New<TSequoiaTransactionService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
