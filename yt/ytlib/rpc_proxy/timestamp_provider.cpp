#include "timestamp_provider.h"
#include "api_service_proxy.h"
#include "connection_impl.h"

#include <yt/ytlib/transaction_client/timestamp_provider_base.h>

namespace NYT {
namespace NRpcProxy {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TTimestampProvider
    : public NTransactionClient::TTimestampProviderBase
{
public:
    TTimestampProvider(
        IChannelPtr channel,
        TDuration rpcTimeout)
        : Channel_(std::move(channel))
        , RpcTimeout_(rpcTimeout)
    { }

private:
    const IChannelPtr Channel_;
    const TDuration RpcTimeout_;

    virtual TFuture<NTransactionClient::TTimestamp> DoGenerateTimestamps(int count) override
    {
        TApiServiceProxy proxy(Channel_);

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
    IChannelPtr channel,
    TDuration rpcTimeout)
{
    return New<TTimestampProvider>(
        std::move(channel),
        rpcTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
