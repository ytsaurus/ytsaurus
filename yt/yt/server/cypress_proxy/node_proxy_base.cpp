#include "node_proxy_base.h"

#include "sequoia_service.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TNodeProxyBase)

void TNodeProxyBase::Invoke(const ISequoiaServiceContextPtr& context)
{
    try {
        if (!DoInvoke(context)) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::NoSuchMethod,
                "%Qv method is not supported",
                context->GetMethod());
        }
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }
}

TNodeProxyBase::TNodeProxyBase(IBootstrap* bootstrap, TSequoiaSessionPtr sequoiaSession)
    : Bootstrap_(bootstrap)
    , SequoiaSession_(std::move(sequoiaSession))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
