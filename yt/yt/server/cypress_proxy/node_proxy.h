#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct INodeProxy
    : public TRefCounted
{
    virtual EInvokeResult Invoke(const ISequoiaServiceContextPtr& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeProxy)

////////////////////////////////////////////////////////////////////////////////

INodeProxyPtr CreateNodeProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session,
    TSequoiaResolveResult resolveResult,
    std::vector<NSequoiaClient::TResolvedPrerequisiteRevision> resolvedPrerequisiteRevisions,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
