#include "stdafx.h"
#include "timestamp_manager.h"
#include "private.h"

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <ytlib/transaction_client/timestamp_service_proxy.h>
#include <ytlib/transaction_client/timestamp_provider.h>

namespace NYT {
namespace NTransactionServer {

using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTimestampProxyService
    : public TServiceBase
{
public:
    TTimestampProxyService(
        IInvokerPtr invoker,
        ITimestampProviderPtr provider)
        : TServiceBase(
            invoker,
            TTimestampServiceProxy::GetServiceName(),
            TransactionServerLogger.GetCategory())
        , Provider_(provider)
    {
        YCHECK(Provider_);
    }

private:
    ITimestampProviderPtr Provider_;


    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, GenerateTimestamps)
    {
        int count = request->count();

        if (Logger.IsEnabled(NLog::ELogLevel::Debug)) {
            context->SetRequestInfo("Count: %d", count);
        }

        Provider_->GenerateTimestamps(count).Subscribe(BIND([=] (TErrorOr<TTimestamp> result) {
            if (result.IsOK()) {
                auto timestamp = result.Value();
                if (Logger.IsEnabled(NLog::ELogLevel::Debug)) {
                    context->SetRequestInfo("Timestamp: %" PRId64, timestamp);
                }
                response->set_timestamp(timestamp);
                context->Reply();
            } else {
                context->Reply(result);
            }
        }));
    }

};

IServicePtr CreateTimestampProxyService(
    IInvokerPtr invoker,
    ITimestampProviderPtr provider)
{
    return New<TTimestampProxyService>(
        invoker,
        provider);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
