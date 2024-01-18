#pragma once

#include "public.h"

#include <yt/yt_proto/yt/flow/controller/proto/controller_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

class TControllerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TControllerServiceProxy, ControllerService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, GetFlowView);
    DEFINE_RPC_PROXY_METHOD(NProto, GetSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, SetSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, RemoveSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, GetDynamicSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, SetDynamicSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, RemoveDynamicSpec);

    DEFINE_RPC_PROXY_METHOD(NProto, GetPipelineStatus);

    DEFINE_RPC_PROXY_METHOD(NProto, StartPipeline);
    DEFINE_RPC_PROXY_METHOD(NProto, PausePipeline);
    DEFINE_RPC_PROXY_METHOD(NProto, StopPipeline);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
