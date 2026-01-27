#include "cell_master_service.h"

#include "bootstrap.h"
#include "public.h"
#include "private.h"
#include "master_hydra_service.h"
#include "hydra_facade.h"
#include "multicell_manager.h"

#include <yt/yt/server/master/cell_master/proto/multicell_manager.pb.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/cell_master_client/cell_master_service_proxy.h>

#include <yt/yt/ytlib/cell_master_client/proto/cell_master_service.pb.h>

namespace NYT::NCellMaster {

using namespace NCellMasterClient;
using namespace NHydra;
using namespace NRpc;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TCellMasterService
    : public TMasterHydraServiceBase
{
public:
    explicit TCellMasterService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TCellMasterServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::CellMasterService,
            CellMasterLogger())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResetDynamicallyPropagatedMasterCells)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NCellMasterClient::NProto, ResetDynamicallyPropagatedMasterCells)
    {
        context->SetRequestInfo();

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster()) {
            THROW_ERROR_EXCEPTION("Cannot make masters reliable at secondary master");
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->ValidateAllMasterCellsAreReliable();

        NCellMaster::NProto::TReqResetDynamicallyPropagatedMasterCells req;
        auto mutation = multicellManager->CreateResetDynamicallyPropagatedMasterCellsMutation(req);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateCellMasterService(TBootstrap* bootstrap)
{
    return New<TCellMasterService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
