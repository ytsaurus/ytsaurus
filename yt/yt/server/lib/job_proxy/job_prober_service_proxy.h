#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_proxy/proto/job_prober_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProberServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJobProberServiceProxy, JobProberService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NJobProxy::JobProber::NProto, DumpInputContext);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::JobProber::NProto, GetStderr);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::JobProber::NProto, PollJobShell);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::JobProber::NProto, Interrupt);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::JobProber::NProto, Fail);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::JobProber::NProto, GracefulAbort);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::JobProber::NProto, DumpSensors);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
