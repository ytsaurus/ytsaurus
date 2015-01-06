#include "stdafx.h"
#include "scheduler_channel.h"
#include "config.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <core/ytree/convert.h>
#include <core/ytree/ypath_proxy.h>

#include <core/bus/config.h>
#include <core/bus/tcp_client.h>

#include <core/rpc/roaming_channel.h>
#include <core/rpc/bus_channel.h>
#include <core/rpc/retrying_channel.h>

namespace NYT {
namespace NScheduler {

using namespace NBus;
using namespace NRpc;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerChannelProvider
    : public IRoamingChannelProvider
{
public:
    TSchedulerChannelProvider(IChannelFactoryPtr channelFactory, IChannelPtr masterChannel)
        : ChannelFactory_(std::move(channelFactory))
        , MasterChannel_(std::move(masterChannel))
    { }

    virtual TYsonString GetEndpointDescription() const override
    {
        return ConvertToYsonString(Stroka("<scheduler>"));
    }

    virtual TFuture<IChannelPtr> DiscoverChannel(IClientRequestPtr request) override
    {
        TObjectServiceProxy proxy(MasterChannel_);
        auto req = TYPathProxy::Get("//sys/scheduler/@address");
        auto this_ = MakeStrong(this);
        return proxy
            .Execute(req)
            .Apply(BIND([this, this_] (TYPathProxy::TRspGetPtr rsp) -> IChannelPtr {
                auto address = ConvertTo<Stroka>(TYsonString(rsp->value()));
                return ChannelFactory_->CreateChannel(address);
            }));
    }

private:
    IChannelFactoryPtr ChannelFactory_;
    IChannelPtr MasterChannel_;

};

IChannelPtr CreateSchedulerChannel(
    TSchedulerConnectionConfigPtr config,
    IChannelFactoryPtr channelFactory,
    IChannelPtr masterChannel)
{
    YCHECK(config);
    YCHECK(channelFactory);
    YCHECK(masterChannel);

    auto channelProvider = New<TSchedulerChannelProvider>(channelFactory, masterChannel);
    auto roamingChannel = CreateRoamingChannel(channelProvider);
    roamingChannel->SetDefaultTimeout(config->RpcTimeout);
    return CreateRetryingChannel(config, roamingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
