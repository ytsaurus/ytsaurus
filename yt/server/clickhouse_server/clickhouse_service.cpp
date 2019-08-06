#include "private.h"

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
    TClickHouseService(IInvokerPtr invoker, TString instanceId)
        : TServiceBase(
            invoker,
            TClickHouseServiceProxy::GetDescriptor(),
            ServerLogger)
        , InstanceId_(std::move(instanceId))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProcessGossip));
    }

private:
    TString InstanceId_;

    DECLARE_RPC_SERVICE_METHOD(NProto, ProcessGossip)
    {
        Y_UNUSED(request);
        response->set_instance_id(InstanceId_);
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateClickHouseService(IInvokerPtr invoker, TString instanceId)
{
    return New<TClickHouseService>(invoker, std::move(instanceId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
