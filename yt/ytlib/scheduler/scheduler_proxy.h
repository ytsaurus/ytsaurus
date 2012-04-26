#pragma once

#include "public.h"

#include <ytlib/scheduler/scheduler_service.pb.h>
#include <ytlib/rpc/service.h>

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

    TSchedulerServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(~channel, GetServiceName())
    { }

    // From clients to scheduler.
    DEFINE_RPC_PROXY_METHOD(NProto, StartOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortOperation);
    DEFINE_RPC_PROXY_METHOD(NProto, WaitForOperation);

    // From nodes to scheduler.
    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
