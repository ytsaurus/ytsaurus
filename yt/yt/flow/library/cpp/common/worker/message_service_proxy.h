#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/worker/proto/message_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

class TMessageServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TMessageServiceProxy, MessageService,
        .SetProtocolVersion(3));

    DEFINE_RPC_PROXY_METHOD(NProto, PushMessages);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
