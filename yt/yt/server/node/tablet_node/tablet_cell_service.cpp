#include "tablet_cell_service.h"

#include "private.h"

#include <yt/client/object_client/helpers.h>

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/master_connector.h>

#include <yt/server/node/data_node/legacy_master_connector.h>

#include <yt/server/node/tablet_node/master_connector.h>

#include <yt/server/lib/hydra/hydra_service.h>

#include <yt/ytlib/tablet_cell_client/tablet_cell_service_proxy.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellService
    : public TServiceBase
{
public:
    TTabletCellService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            NTabletCellClient::TTabletCellServiceProxy::GetDescriptor(),
            TabletNodeLogger)
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestHeartbeat));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NTabletCellClient::NProto, RequestHeartbeat)
    {
        context->SetRequestInfo();

        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeMasterConnector();
        if (clusterNodeMasterConnector->IsConnected()) {
            auto primaryCellTag = CellTagFromId(Bootstrap_->GetCellId());
            if (clusterNodeMasterConnector->UseNewHeartbeats()) {
                const auto& masterConnector = Bootstrap_->GetTabletNodeMasterConnector();
                masterConnector->ScheduleHeartbeat(primaryCellTag, /* immediately */ true);
            } else {
                Bootstrap_->GetLegacyMasterConnector()->ScheduleNodeHeartbeat(primaryCellTag, /* immediately */ true);
            }
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTabletCellService(NClusterNode::TBootstrap* bootstrap)
{
    return New<TTabletCellService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
