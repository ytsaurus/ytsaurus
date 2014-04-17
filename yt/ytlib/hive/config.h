#pragma once

#include "public.h"

#include <core/rpc/config.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectoryConfig
    : public NRpc::TBalancingChannelConfigBase
{
public:
    //! Timeout for RPC requests to cells.
    TDuration RpcTimeout;

    TCellDirectoryConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////
} // namespace NHive
} // namespace NYT
