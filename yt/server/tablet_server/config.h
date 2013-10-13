#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public TYsonSerializable
{
public:
    TDuration PeerFailoverTimeout;

    TTabletManagerConfig()
    {
        RegisterParameter("peer_failover_timeout", PeerFailoverTimeout)
            .Default(TDuration::Minutes(1));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
