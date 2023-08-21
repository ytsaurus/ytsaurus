#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/proto/master_tablet_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TMasterTabletServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TMasterTabletServiceProxy, MasterTabletService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, GetTableBalancingAttributes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
