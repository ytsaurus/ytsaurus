#include "stdafx.h"
#include "scheduler_channel.h"

#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/bus/config.h>
#include <ytlib/bus/tcp_client.h>
#include <ytlib/rpc/roaming_channel.h>
#include <ytlib/rpc/bus_channel.h>

namespace NYT {
namespace NScheduler {

using namespace NBus;
using namespace NRpc;
using namespace NObjectServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TValueOrError<IChannelPtr> OnSchedulerAddressFound(TYPathProxy::TRspGetPtr rsp)
{
    if (!rsp->IsOK()) {
        return rsp->GetError();
    }

    auto address = DeserializeFromYson<Stroka>(rsp->value());

    // TODO(babenko): get rid of this hardcoded priority
    auto config = New<TTcpBusClientConfig>(address);
    config->Priority = 6;
    auto client = CreateTcpBusClient(config);
    return CreateBusChannel(client);
}

} // namespace

IChannelPtr CreateSchedulerChannel(
    TNullable<TDuration> defaultTimeout,
    IChannelPtr masterChannel)
{
    return CreateRoamingChannel(
        defaultTimeout,
        BIND([=] () -> TFuture< TValueOrError<IChannelPtr> > {
            TObjectServiceProxy proxy(masterChannel);
            auto req = TYPathProxy::Get("//sys/scheduler/@address");
            return proxy.Execute(req).Apply(BIND(&OnSchedulerAddressFound));
        }));

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
