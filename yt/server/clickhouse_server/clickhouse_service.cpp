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
    TClickHouseService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            TClickHouseServiceProxy::GetDescriptor(),
            ServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProcessGossip));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NProto, ProcessGossip)
    {
        Y_UNUSED(request);
        Y_UNUSED(response);
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateClickHouseService(IInvokerPtr invoker)
{
    return New<TClickHouseService>(invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
