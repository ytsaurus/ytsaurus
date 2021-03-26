#include "cypress_service.h"
#include "private.h"
#include "cypress_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/ytlib/cypress_client/cypress_service_proxy.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NCypressClient;
using namespace NHydra;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TCypressService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TCypressService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TCypressServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::CypressService,
            CypressServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TouchNodes));
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressClient::NProto, TouchNodes)
    {
        context->SetRequestInfo("NodeCount: %v",
            request->node_ids_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        for (const auto& protoNodeId : request->node_ids()) {
            auto nodeId = FromProto<TNodeId>(protoNodeId);
            auto* node = cypressManager->FindNode(TVersionedNodeId(nodeId));
            if (!IsObjectAlive(node)) {
                continue;
            }

            cypressManager->SetTouched(node);
        }

        context->Reply();
    }
};

IServicePtr CreateCypressService(TBootstrap* boostrap)
{
    return New<TCypressService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
