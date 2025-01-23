#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TNodeProxyBase
    : public TRefCounted
{
public:
    EInvokeResult Invoke(const ISequoiaServiceContextPtr& context);

protected:
    IBootstrap* const Bootstrap_;
    const TSequoiaSessionPtr SequoiaSession_;

    // NB: If this field is set to #ForwardToMaster, no change in persistent state should
    // be made, and vice versa.
    //! Most requests are exeucted on Cypress proxy, but there is a chance, that
    //! during the execution of a verb, it becomes apparent, that the request
    //! has to be handeled by master. This override exists for such cases.
    EInvokeResult InvokeResult_ = EInvokeResult::Executed;

    TNodeProxyBase(IBootstrap* bootstrap, TSequoiaSessionPtr sequoiaSession);

    virtual void BeforeInvoke(const ISequoiaServiceContextPtr& context);
    virtual bool DoInvoke(const ISequoiaServiceContextPtr& context);
    virtual void AfterInvoke(const ISequoiaServiceContextPtr& context);
};

DEFINE_REFCOUNTED_TYPE(TNodeProxyBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
