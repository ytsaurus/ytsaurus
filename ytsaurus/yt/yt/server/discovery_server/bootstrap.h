#pragma once

#include "private.h"

#include <yt/yt/server/lib/discovery_server/public.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;
};

std::unique_ptr<IBootstrap> CreateBootstrap(TClusterDiscoveryServerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
