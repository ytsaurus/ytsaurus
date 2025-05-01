#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

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
    TSequoiaResolveResult resolveResult);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
