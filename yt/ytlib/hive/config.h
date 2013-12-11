#pragma once

#include "public.h"

#include <ytlib/hydra/config.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectoryConfig
    : public TYsonSerializable
{
public:
    TDuration RpcTimeout;

    TCellDirectoryConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(3));
    }
};

class TRemoteTimestampProviderConfig
    : public NHydra::TPeerDiscoveryConfig
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
