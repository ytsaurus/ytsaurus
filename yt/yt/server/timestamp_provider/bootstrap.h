#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/bootstrap.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public NServer::IDaemonBootstrap
{ };

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateTimestampProviderBootstrap(
    TTimestampProviderBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
