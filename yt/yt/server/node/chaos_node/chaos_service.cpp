#include "chaos_service.h"
#include "private.h"
#include "slot_manager.h"
#include "chaos_manager.h"
#include "chaos_slot.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NChaosNode {

using namespace NRpc;
using namespace NChaosClient;
using namespace NHydra;
using namespace NClusterNode;
using namespace NTableClient;
using namespace NTabletClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosService
    : public THydraServiceBase
{
public:
    TChaosService(
        IChaosSlotPtr slot,
        IBootstrap* bootstrap)
        : THydraServiceBase(
            slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Default),
            TChaosServiceProxy::GetDescriptor(),
            ChaosNodeLogger,
            slot->GetCellId())
        , Slot_(slot)
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Slot_);
        YT_VERIFY(Bootstrap_);
    }

private:
    const IChaosSlotPtr Slot_;
    IBootstrap* const Bootstrap_;


    // THydraServiceBase overrides.
    virtual IHydraManagerPtr GetHydraManager() override
    {
        return Slot_->GetHydraManager();
    }
};

IServicePtr CreateChaosService(IChaosSlotPtr slot, IBootstrap* bootstrap)
{
    return New<TChaosService>(slot, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
