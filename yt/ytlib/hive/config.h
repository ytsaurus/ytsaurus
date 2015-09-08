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
    //! Timeout for RPC requests in TCellDirectory::Synchronize.
    TDuration SyncRpcTimeout;

    TCellDirectoryConfig()
    {
        RegisterParameter("sync_rpc_timeout", SyncRpcTimeout)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
