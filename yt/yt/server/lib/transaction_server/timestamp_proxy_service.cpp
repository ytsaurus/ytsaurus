#include "timestamp_proxy_service.h"
#include "private.h"

#include <yt/yt/client/transaction_client/timestamp_provider.h>
#include <yt/yt/client/transaction_client/timestamp_service_proxy.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NTransactionServer {

using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TTimestampProxyService
    : public TServiceBase
{
public:
    explicit TTimestampProxyService(
        ITimestampProviderPtr provider,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            TTimestampServiceProxy::GetDescriptor(),
            TransactionServerLogger,
            NullRealmId,
            std::move(authenticator))
        , Provider_(std::move(provider))
    {
        YT_VERIFY(Provider_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateTimestamps)
            .SetQueueSizeLimit(100000)
            .SetConcurrencyLimit(100000));
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
                context->SetResponseInfo("Timestamp: %v", timestamp);
                response->set_timestamp(timestamp);
                context->Reply();
            } else {
                context->Reply(result);
            }
        }));
    }

};

IServicePtr CreateTimestampProxyService(
    ITimestampProviderPtr provider,
    IAuthenticatorPtr authenticator)
{
    return New<TTimestampProxyService>(std::move(provider), std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
