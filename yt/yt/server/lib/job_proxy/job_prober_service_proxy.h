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

    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, DumpInputContext);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, GetStderr);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, PollJobShell);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, Interrupt);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, Fail);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, GracefulAbort);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, DumpSensors);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
