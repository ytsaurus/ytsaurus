#pragma once

#include "public.h"

#include "sequoia_service.h"

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TNodeProxyBase
    : public TRefCounted
{
public:
    void Invoke(const ISequoiaServiceContextPtr& context);

protected:
    IBootstrap* const Bootstrap_;
    const TSequoiaSessionPtr SequoiaSession_;

    explicit TNodeProxyBase(IBootstrap* bootstrap, TSequoiaSessionPtr sequoiaSession);

    virtual void BeforeInvoke(const ISequoiaServiceContextPtr& context);
    virtual bool DoInvoke(const ISequoiaServiceContextPtr& context);
    virtual void AfterInvoke(const ISequoiaServiceContextPtr& context);

    DEFINE_YPATH_CONTEXT_IMPL(ISequoiaServiceContext, TTypedSequoiaServiceContext);
};

using TNodeProxyBasePtr = TIntrusivePtr<TNodeProxyBase>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
