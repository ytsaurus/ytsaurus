#include "stdafx.h"
#include "tablet_service.h"
#include "tablet_slot.h"
#include "private.h"

#include <ytlib/tablet_client/tablet_service_proxy.h>

#include <server/hydra/hydra_manager.h>

namespace NYT {
namespace NTabletNode {

using namespace NTabletClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

TTabletService::TTabletService(
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : NHydra::THydraServiceBase(
        slot->GetHydraManager(),
        slot->GetAutomatonInvoker(),
        NRpc::TServiceId(TTabletServiceProxy::GetServiceName(), slot->GetCellGuid()),
        TabletNodeLogger.GetCategory())
    , Slot(slot)
    , Bootstrap(bootstrap)
{
    YCHECK(Slot);
    YCHECK(Bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
