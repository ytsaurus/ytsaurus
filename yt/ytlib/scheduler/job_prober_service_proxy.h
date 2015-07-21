#pragma once

#include "public.h"

#include <ytlib/scheduler/job_prober_service.pb.h>

#include <core/rpc/client.h>

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
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
