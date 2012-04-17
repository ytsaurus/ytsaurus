#include "stdafx.h"
#include "scheduler_channel.h"

#include <ytlib/rpc/roaming_channel.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCypress;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TValueOrError<IChannel::TPtr> OnSchedulerAddressFound(TYPathProxy::TRspGet::TPtr rsp)
{
    if (!rsp->IsOK()) {
        return rsp->GetError();
    }

    auto address = DeserializeFromYson<Stroka>(rsp->value());
    return CreateBusChannel(address);
}

} // namespace <anonymous>

IChannel::TPtr CreateSchedulerChannel(
    TNullable<TDuration> defaultTimeout,
    IChannel::TPtr masterChannel)
{
    return CreateRoamingChannel(
        defaultTimeout,
        BIND([=] () -> TFuture< TValueOrError<IChannel::TPtr> >::TPtr {
            TCypressServiceProxy proxy(masterChannel);
            auto req = TYPathProxy::Get("//sys/scheduler/runtime/@address");
            return proxy.Execute(req).Apply(BIND(&OnSchedulerAddressFound));
        }));

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
