#include "clickhouse_service.h"

#include "config.h"
#include "host.h"
#include "clickhouse_service_proxy.h"

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/service_detail.h>

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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(InvalidateCachedObjectAttributes));
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

    DECLARE_RPC_SERVICE_METHOD(NProto, InvalidateCachedObjectAttributes)
    {
        auto paths = FromProto<std::vector<TString>>(request->table_paths());

        context->SetRequestInfo("Paths: %v", paths);

        Host_->InvalidateCachedObjectAttributes(paths);

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
