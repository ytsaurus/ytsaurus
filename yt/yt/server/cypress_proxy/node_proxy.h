#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

TNodeProxyBasePtr CreateNodeProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session,
    TSequoiaResolveResult resolveResult);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
