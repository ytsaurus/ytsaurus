#pragma once

#include "public.h"

#include <ytlib/job_prober_client/job_prober_service.pb.h>

#include <core/rpc/client.h>

namespace NYT {
namespace NJobProberClient {

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
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NJobProberClient::NProto, DumpInputContext);
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient
} // namespace NYT
