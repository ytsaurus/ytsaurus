#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaServiceContext
    : public virtual NRpc::IServiceContext
{ };

DEFINE_REFCOUNTED_TYPE(ISequoiaServiceContext)

////////////////////////////////////////////////////////////////////////////////

// TODO(kvk1920): do we actually want a separate type for Sequoia context?
class TSequoiaServiceContextWrapper
    : public NRpc::TServiceContextWrapper
    , public ISequoiaServiceContext
{
public:
    explicit TSequoiaServiceContextWrapper(ISequoiaServiceContextPtr underlyingContext);

private:
    const ISequoiaServiceContextPtr UnderlyingContext_;
};

DEFINE_REFCOUNTED_TYPE(TSequoiaServiceContextWrapper)

////////////////////////////////////////////////////////////////////////////////

ISequoiaServiceContextPtr CreateSequoiaServiceContext(TSharedRefArray requestMessage);

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaService
    : public TRefCounted
{
    enum EInvokeResult
    {
        Executed,
        ForwardToMaster,
    };

    //! Either executes request in Sequoia or forwards it to master.
    /*!
     *  There are 2 special cases which are resolved as Cypress but still have
     *  to be handled (partially or not) by Sequoia.
     *
     *  Link node creation:
     *      Since link can point at Sequoia node, all cyclicity check have to be
     *      done with Sequoia tables. We do it right before redirecting request
     *      to master.
     *
     *  Rootstock / scion creation:
     *      Obviously, there is no way to resolve it as Sequoia request, but it
     *      is a Sequoia request.
     */
    virtual EInvokeResult TryInvoke(
        const ISequoiaServiceContextPtr& context,
        const TSequoiaSessionPtr& sequoiaSession,
        const TResolveResult& resolveResult) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaService);

////////////////////////////////////////////////////////////////////////////////

ISequoiaServicePtr CreateSequoiaService(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
