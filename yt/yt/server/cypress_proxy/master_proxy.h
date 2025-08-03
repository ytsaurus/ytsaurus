#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

INodeProxyPtr CreateMasterProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
