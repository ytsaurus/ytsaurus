#pragma once

#include <yt/yt/ytlib/chaos_client/proto/chaos_master_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TChaosMasterServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TChaosMasterServiceProxy, ChaosMasterService,
        .SetProtocolVersion(2)
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, SyncAlienCells);
    DEFINE_RPC_PROXY_METHOD(NProto, GetCellDescriptorsByCellBundle);
    DEFINE_RPC_PROXY_METHOD(NProto, GetCellDescriptors);
    DEFINE_RPC_PROXY_METHOD(NProto, FindCellDescriptorsByCellTags);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

