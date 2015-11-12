#pragma once

#include "public.h"

#include <yt/server/exec_agent/supervisor_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "SupervisorService";
    }

    explicit TSupervisorServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, GetJobSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobFinished);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NProto, OnJobProgress);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NProto, UpdateResourceUsage);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
