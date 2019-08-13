#include "private.h"

#include "bootstrap.h"
#include "clickhouse_service.h"
#include "clickhouse_service_proxy.h"

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/service_detail.h>

namespace NYT::NClickHouseServer {

using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

class TClickHouseService
    : public TServiceBase
{
public:
    TClickHouseService(TBootstrap* bootstrap, TString instanceId)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TClickHouseServiceProxy::GetDescriptor(),
            ServerLogger)
        , InstanceId_(std::move(instanceId))
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProcessGossip));
    }

private:
    TString InstanceId_;
    TBootstrap* Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, ProcessGossip)
    {
        Y_UNUSED(request);
        response->set_instance_id(InstanceId_);
        response->set_instance_state(static_cast<int>(Bootstrap_->GetState()));
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateClickHouseService(TBootstrap* bootstrap, TString instanceId)
{
    return New<TClickHouseService>(bootstrap, std::move(instanceId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
