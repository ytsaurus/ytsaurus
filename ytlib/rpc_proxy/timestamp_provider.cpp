#include "timestamp_provider.h"
#include "api_service_proxy.h"
#include "connection_impl.h"

#include <yt/ytlib/transaction_client/timestamp_provider_base.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TTimestampProvider
    : public NTransactionClient::TTimestampProviderBase
{
public:
    TTimestampProvider(
        TWeakPtr<TConnection> connection,
        TDuration rpcTimeout)
        : Connection_(std::move(connection))
        , RpcTimeout_(rpcTimeout)
    { }

private:
    const TWeakPtr<TConnection> Connection_;
    const TDuration RpcTimeout_;

    virtual TFuture<NTransactionClient::TTimestamp> DoGenerateTimestamps(int count) override
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return MakeFuture<NTransactionClient::TTimestamp>(TError("Connection is closed"));
        }

        TApiServiceProxy proxy(connection->GetRandomPeerChannel());

        auto req = proxy.GenerateTimestamps();
        req->SetTimeout(RpcTimeout_);
        req->set_count(count);

        return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGenerateTimestampsPtr& rsp) {
            return static_cast<NTransactionClient::TTimestamp>(rsp->timestamp());
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::ITimestampProviderPtr CreateTimestampProvider(
    TWeakPtr<TConnection> connection,
    TDuration rpcTimeout)
{
    return New<TTimestampProvider>(
        std::move(connection),
        rpcTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
