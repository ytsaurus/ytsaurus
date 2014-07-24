#include "stdafx.h"
#include "timestamp_manager.h"
#include "private.h"

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>
#include <core/rpc/dispatcher.h>

#include <ytlib/transaction_client/timestamp_service_proxy.h>
#include <ytlib/transaction_client/timestamp_provider.h>

namespace NYT {
namespace NTransactionServer {

using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TTimestampProxyService
    : public TServiceBase
{
public:
    explicit TTimestampProxyService(ITimestampProviderPtr provider)
        : TServiceBase(
            NRpc::TDispatcher::Get()->GetPoolInvoker(),
            TTimestampServiceProxy::GetServiceName(),
            TransactionServerLogger)
        , Provider_(provider)
    {
        YCHECK(Provider_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateTimestamps));
    }

private:
    ITimestampProviderPtr Provider_;


    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, GenerateTimestamps)
    {
        int count = request->count();
        context->SetRequestInfo("Count: %v", count);

        Provider_->GenerateTimestamps(count).Subscribe(BIND([=] (TErrorOr<TTimestamp> result) {
            if (result.IsOK()) {
                auto timestamp = result.Value();
                context->SetResponseInfo("Timestamp: %v", timestamp);
                response->set_timestamp(timestamp);
                context->Reply();
            } else {
                context->Reply(result);
            }
        }));
    }

};

IServicePtr CreateTimestampProxyService(ITimestampProviderPtr provider)
{
    return New<TTimestampProxyService>(provider);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
