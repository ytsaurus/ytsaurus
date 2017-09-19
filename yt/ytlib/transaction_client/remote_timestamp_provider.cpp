#include "remote_timestamp_provider.h"
#include "batching_timestamp_provider.h"
#include "timestamp_provider_base.h"
#include "private.h"
#include "config.h"
#include "timestamp_service_proxy.h"

#include <yt/core/rpc/balancing_channel.h>
#include <yt/core/rpc/retrying_channel.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTransactionClient {

using namespace NRpc;
using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProvider
    : public TTimestampProviderBase
{
public:
    TRemoteTimestampProvider(NRpc::IChannelPtr channel, TDuration defaultTimeout)
        : Proxy_(std::move(channel))
    {
        Proxy_.SetDefaultTimeout(defaultTimeout);
    }

private:
    TTimestampServiceProxy Proxy_;

    virtual TFuture<TTimestamp> DoGenerateTimestamps(int count) override
    {
        auto req = Proxy_.GenerateTimestamps();
        req->set_count(count);
        return req->Invoke().Apply(BIND([] (const TTimestampServiceProxy::TRspGenerateTimestampsPtr& rsp) {
            return static_cast<TTimestamp>(rsp->timestamp());
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    IChannelFactoryPtr channelFactory)
{
    auto endpointDescription = TString("TimestampProvider@");
    auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("timestamp_provider").Value(true)
        .EndMap());
    auto channel = CreateBalancingChannel(
        config,
        channelFactory,
        endpointDescription,
        *endpointAttributes);
    channel = CreateRetryingChannel(
        config,
        channel);

    auto underlying = New<TRemoteTimestampProvider>(std::move(channel), config->RpcTimeout);
    auto wrapped = CreateBatchingTimestampProvider(std::move(underlying), config->UpdatePeriod);

    return wrapped;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT

