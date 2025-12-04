#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct INodeProxy
    : public TRefCounted
{
    virtual TInvokeResult Invoke(const ISequoiaServiceContextPtr& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeProxy)

////////////////////////////////////////////////////////////////////////////////

INodeProxyPtr CreateNodeProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session,
    TSequoiaResolveResult resolveResult,
    std::vector<NSequoiaClient::TResolvedPrerequisiteRevision> resolvedPrerequisiteRevisions);

INodeProxyPtr CreateUnreachableNodeProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session,
    TUnreachableSequoiaResolveResult resolveResult);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
