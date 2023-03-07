#include "clickhouse_service.h"

#include "config.h"
#include "host.h"
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
    explicit TClickHouseService(THost* host)
        : TServiceBase(
            host->GetControlInvoker(),
            TClickHouseServiceProxy::GetDescriptor(),
            ClickHouseYtLogger)
        , Host_(host)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProcessGossip));
    }

private:
    THost* Host_;

    DECLARE_RPC_SERVICE_METHOD(NProto, ProcessGossip)
    {
        context->SetRequestInfo("InstanceId: %v, State: %v",
            request->instance_id(),
            static_cast<EInstanceState>(request->instance_state()));

        response->set_instance_id(ToString(Host_->GetConfig()->InstanceId));
        auto state = Host_->GetInstanceState();
        response->set_instance_state(static_cast<int>(state));

        context->SetResponseInfo("SelfInstanceId: %v, SelfState: %v",
            Host_->GetConfig()->InstanceId,
            state);

        Host_->HandleIncomingGossip(request->instance_id(), static_cast<EInstanceState>(request->instance_state()));
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateClickHouseService(THost* host)
{
    return New<TClickHouseService>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
