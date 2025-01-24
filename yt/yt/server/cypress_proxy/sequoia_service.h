#pragma once

#include "public.h"
#include "private.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

ISequoiaServiceContextPtr CreateSequoiaServiceContext(TSharedRefArray requestMessage);

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaService
    : public TRefCounted
{
    //! Either executes request in Sequoia or forwards it to master.
    /*!
     *  There are 3 special cases which are resolved as Cypress but still have
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
     *
     *  Cypress to Cypress copy:
     *      Any copy request involving Sequoia should be executed on Cypress
     *      proxy. To avoid resolving both source and destination path for copy
     *      verb for every request, an attempt to execute it on proxy is made.
     *      If the attempt is unsuccessful because both are Cypress paths, then
     *      the request is forwarded to master.
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
