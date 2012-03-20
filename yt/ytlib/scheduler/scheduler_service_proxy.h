#pragma once

#include "public.h"
#include "scheduler_service.pb.h"

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/client.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "SchedulerService";
    }

    TSchedulerServiceProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(~channel, GetServiceName())
    { }

    // From clients to scheduler.
    DEFINE_RPC_PROXY_METHOD(NProto, StartTask);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortTask);
    DEFINE_RPC_PROXY_METHOD(NProto, WaitForTask);

    // From nodes to scheduler.
    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
