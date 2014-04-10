#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerDiscoveryConfig
    : public NRpc::TRetryingChannelConfig
    , public NRpc::TBalancingChannelConfig
{
public:
    //! Id of the cell.
    TCellGuid CellGuid;

    //! Timeout for RPC requests to peers.
    TDuration RpcTimeout;

    TPeerDiscoveryConfig()
    {
        RegisterParameter("cell_guid", CellGuid)
            .Default();
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TPeerDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
