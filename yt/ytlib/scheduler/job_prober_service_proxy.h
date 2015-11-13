#pragma once

#include "public.h"

#include <yt/ytlib/scheduler/job_prober_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TJobProberServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "JobProberService";
    }

    static int GetProtocolVersion()
    {
        return 0;
    }

    explicit TJobProberServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, DumpInputContext);
    DEFINE_RPC_PROXY_METHOD(NProto, Strace);
    DEFINE_RPC_PROXY_METHOD(NProto, SignalJob);
    DEFINE_RPC_PROXY_METHOD(NProto, AbandonJob);
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
