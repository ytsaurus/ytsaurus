#include "incumbent_service.h"

#include "incumbent_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/lib/incumbent_client/incumbent_service_proxy.h>

#include <yt/yt/ytlib/incumbent_client/incumbent_descriptor.h>

namespace NYT::NIncumbentServer {

using namespace NCellMaster;
using namespace NIncumbentClient;
using namespace NHydra;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TIncumbentService
    : public TMasterHydraServiceBase
{
public:
    explicit TIncumbentService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TIncumbentServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::IncumbentManager,
            IncumbentServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NIncumbentClient::NProto, Heartbeat)
    {
        context->SetRequestInfo();

        ValidatePeer(EPeerKind::LeaderOrFollower);

        TIncumbentMap descriptors;
        for (const auto& protoDescriptor : request->descriptors()) {
            auto incumbentType = CheckedEnumCast<EIncumbentType>(protoDescriptor.type());
            auto& descriptor = descriptors[incumbentType];
            for (const auto& address : protoDescriptor.addresses()) {
                if (address) {
                    descriptor.Addresses.push_back(address);
                } else {
                    descriptor.Addresses.push_back(std::nullopt);
                }
            }
        }

        auto peerLeaseDeadline = FromProto<TInstant>(request->peer_lease_deadline());

        const auto& incumbentManager = Bootstrap_->GetIncumbentManager();
        incumbentManager->OnHeartbeat(peerLeaseDeadline, descriptors);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateIncumbentService(TBootstrap* bootstrap)
{
    return New<TIncumbentService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
