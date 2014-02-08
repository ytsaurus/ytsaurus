#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerDiscoveryConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! Id of the cell.
    TCellGuid CellGuid;

    //! List of peer addresses.
    std::vector<Stroka> Addresses;

    //! Timeout for RPC requests to peers.
    TDuration RpcTimeout;

    TPeerDiscoveryConfig()
    {
        RegisterParameter("cell_guid", CellGuid)
            .Default();
        RegisterParameter("addresses", Addresses)
            .NonEmpty();
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TPeerDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
