#pragma once

#include "public.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

INodeProxyPtr CreateRootstockProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr sequoiaSession,
    NSequoiaClient::TAbsoluteYPath resolvedPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
