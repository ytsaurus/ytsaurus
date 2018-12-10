#include "timestamp_proxy_service.h"
#include "private.h"
#include "timestamp_manager.h"

#include <yt/client/transaction_client/timestamp_provider.h>
#include <yt/client/transaction_client/timestamp_service_proxy.h>

#include <yt/core/rpc/dispatcher.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

namespace NYT::NTransactionServer {

using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TTimestampProxyService
    : public TServiceBase
{
public:
    explicit TTimestampProxyService(ITimestampProviderPtr provider)
        : TServiceBase(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            TTimestampServiceProxy::GetDescriptor(),
            TransactionServerLogger)
        , Provider_(provider)
    {
        YCHECK(Provider_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateTimestamps));
    }

private:
    const ITimestampProviderPtr Provider_;


    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, GenerateTimestamps)
    {
        int count = request->count();
        context->SetRequestInfo("Count: %v", count);

        Provider_->GenerateTimestamps(count).Subscribe(BIND([=] (const TErrorOr<TTimestamp>& result) {
            if (result.IsOK()) {
                auto timestamp = result.Value();
                context->SetResponseInfo("Timestamp: %llx", timestamp);
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

} // namespace NYT::NTransactionServer
