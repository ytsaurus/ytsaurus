#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

INodeProxyPtr CreateMasterProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
