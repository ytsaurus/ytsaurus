#pragma once

#include "public.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

INodeProxyPtr CreateRootstockProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr sequoiaSession,
    NSequoiaClient::TAbsolutePath resolvedPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
