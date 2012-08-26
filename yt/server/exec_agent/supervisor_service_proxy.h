#pragma once

#include "public.h"

#include <ytlib/rpc/client.h>

#include <server/exec_agent/supervisor_service.pb.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TSupervisorServiceProxy> TPtr;

    static Stroka GetServiceName()
    {
        return "SupervisorService";
    }

    TSupervisorServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, GetJobSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobFinished);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NProto, OnJobProgress);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NProto, OnResourceUtilizationSet);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
