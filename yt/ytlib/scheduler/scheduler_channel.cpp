#include "stdafx.h"
#include "scheduler_channel.h"
#include "config.h"

#include <ytlib/object_client/object_service_proxy.h>

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

IChannelPtr CreateSchedulerChannel(
    TSchedulerConnectionConfigPtr config,
    IChannelFactoryPtr channelFactory,
    IChannelPtr masterChannel)
{
    auto roamingChannel = CreateRoamingChannel(
        BIND([=] () -> TFuture< TErrorOr<IChannelPtr> > {
            TObjectServiceProxy proxy(masterChannel);
            auto req = TYPathProxy::Get("//sys/scheduler/@address");
            return proxy
                .Execute(req)
                .Apply(BIND([=] (TYPathProxy::TRspGetPtr rsp) -> TErrorOr<IChannelPtr> {
                    if (!rsp->IsOK()) {
                        return rsp->GetError();
                    }

                    auto address = ConvertTo<Stroka>(TYsonString(rsp->value()));
                    return channelFactory->CreateChannel(address);
                }));
        }));

    roamingChannel->SetDefaultTimeout(config->RpcTimeout);
    return CreateRetryingChannel(config, roamingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
