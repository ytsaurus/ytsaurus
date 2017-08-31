#include "rpc_proxy_timestamp_provider.h"
#include "api_service_proxy.h"

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

TRpcProxyTimestampProvider::TRpcProxyTimestampProvider(
    TWeakPtr<TRpcProxyConnection> connection,
    TDuration defaultTimeout)
    : Connection_(std::move(connection))
    , DefaultTimeout_(defaultTimeout)
{ }

TRpcProxyTimestampProvider::~TRpcProxyTimestampProvider() = default;

TFuture<NTransactionClient::TTimestamp> TRpcProxyTimestampProvider::DoGenerateTimestamps(int count)
{
    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<NTransactionClient::TTimestamp>(TError("Connection is closed"));
    }

    TApiServiceProxy proxy(connection->GetRandomPeerChannel());
    proxy.SetDefaultTimeout(DefaultTimeout_);

    auto req = proxy.GenerateTimestamps();
    req->set_count(count);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGenerateTimestampsPtr& rsp) {
        return static_cast<NTransactionClient::TTimestamp>(rsp->timestamp());
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
