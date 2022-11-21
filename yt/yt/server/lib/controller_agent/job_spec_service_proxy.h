#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/proto/job_spec_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TJobSpecServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJobSpecServiceProxy, JobSpecService,
        .SetProtocolVersion(2));

    DEFINE_RPC_PROXY_METHOD(NProto, GetJobSpecs);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
