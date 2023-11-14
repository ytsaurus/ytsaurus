#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/proto/job_prober_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TJobProberServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJobProberServiceProxy, JobProberService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NProto, AbandonJob);
    DEFINE_RPC_PROXY_METHOD(NProto, InterruptJob);
    DEFINE_RPC_PROXY_METHOD(NProto, GetJobShellDescriptor);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

