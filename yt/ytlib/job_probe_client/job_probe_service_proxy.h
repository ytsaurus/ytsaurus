#pragma once

#include "public.h"

#include <ytlib/job_probe_client/job_probe_service.pb.h>

#include <core/rpc/client.h>

namespace NYT {
namespace NJobProbeClient {

////////////////////////////////////////////////////////////////////

class TJobProbeServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "JobProbeService";
    }

    static int GetProtocolVersion()
    {
        return 0;
    }

    explicit TJobProbeServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NJobProbeClient::NProto, GenerateInputContext);
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProbeClient
} // namespace NYT