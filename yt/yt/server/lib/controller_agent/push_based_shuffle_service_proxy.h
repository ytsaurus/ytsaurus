#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/proto/push_based_shuffle_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TPushBasedShuffleServiceProxy, PushBasedShuffleService);

    DEFINE_RPC_PROXY_METHOD(NProto, GetShuffleWriteSession);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
