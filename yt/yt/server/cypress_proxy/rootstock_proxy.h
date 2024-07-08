#pragma once

#include "public.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

TNodeProxyBasePtr CreateRootstockProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr sequoiaSession,
    NSequoiaClient::TAbsoluteYPath resolvedPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
