#pragma once

#include "public.h"

#include <core/rpc/client.h>

#include <ytlib/job_tracker_client/job_tracker_service.pb.h>

namespace NYT {
namespace NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "JobTracker";
    }

    static int GetProtocolVersion()
    {
        return 1;
    }

    explicit TJobTrackerServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
