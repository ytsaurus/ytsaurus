#pragma once

#include "private.h"

#include <yt/yt/server/lib/discovery_server/public.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public NServer::IDaemonBootstrap
{ };

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateDiscoveryServerBootstrap(
    TDiscoveryServerBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
