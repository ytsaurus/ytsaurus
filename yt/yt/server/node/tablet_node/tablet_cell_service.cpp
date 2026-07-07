#include "tablet_cell_service.h"

#include "bootstrap.h"

#include <yt/yt/server/node/cellar_node/master_connector.h>

#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/ytlib/cellar_client/tablet_cell_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NTabletNode {

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
            TabletNodeLogger(),
            TServiceOptions{
                .Authenticator = bootstrap->GetNativeAuthenticator(),
            })
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
            masterConnector->ScheduleMasterHeartbeats({primaryCellTag});
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
