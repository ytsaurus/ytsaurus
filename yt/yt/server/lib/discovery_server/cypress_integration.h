#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/rpc/config.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateDiscoveryYPathService(TGroupTreePtr groupTree);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
