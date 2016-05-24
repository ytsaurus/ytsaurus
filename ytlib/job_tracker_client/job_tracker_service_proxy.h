#pragma once

#include "public.h"

#include <yt/ytlib/job_tracker_client/job_tracker_service.pb.h>

#include <yt/core/rpc/client.h>

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

    explicit TJobTrackerServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
