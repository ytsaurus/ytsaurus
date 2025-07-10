#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/bootstrap.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public NServer::IDaemonBootstrap
{ };

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateOffshoreNodeProxyBootstrap(
    TOffshoreNodeProxyBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
