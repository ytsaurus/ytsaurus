#include "tablet_cell_service.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/server/node/cellar_node/master_connector.h>

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/tablet_node/master_connector.h>

#include <yt/yt/server/lib/hydra_common/hydra_service.h>

#include <yt/yt/ytlib/cellar_client/tablet_cell_service_proxy.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellService
    : public TServiceBase
{
public:
    explicit TTabletCellService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            NCellarClient::TTabletCellServiceProxy::GetDescriptor(),
            TabletNodeLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestHeartbeat));
    }

private:
    IBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NCellarClient::NProto, RequestHeartbeat)
    {
        context->SetRequestInfo();

        if (Bootstrap_->IsConnected()) {
            auto primaryCellTag = CellTagFromId(Bootstrap_->GetCellId());
            const auto& masterConnector = Bootstrap_->GetCellarNodeMasterConnector();
            masterConnector->ScheduleHeartbeat(primaryCellTag, /*immediately*/ true);
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTabletCellService(IBootstrap* bootstrap)
{
    return New<TTabletCellService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
