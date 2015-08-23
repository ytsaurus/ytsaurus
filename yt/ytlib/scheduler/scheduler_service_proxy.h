#pragma once

#include "public.h"

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <core/rpc/client.h>

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

    static int GetProtocolVersion()
    {
        return 1;
    }

    explicit TSchedulerServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, StartOperation);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, AbortOperation);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, SuspendOperation);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, ResumeOperation);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
