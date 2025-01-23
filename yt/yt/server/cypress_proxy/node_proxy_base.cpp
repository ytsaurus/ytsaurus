#include "node_proxy_base.h"

#include "private.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

EInvokeResult TNodeProxyBase::Invoke(const ISequoiaServiceContextPtr& context)
{
    TError error;
    try {
        BeforeInvoke(context);
        if (!DoInvoke(context)) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::NoSuchMethod,
                "%Qv method is not supported",
                context->GetMethod());
        }
    } catch (const std::exception& ex) {
        error = ex;
    }

    AfterInvoke(context);

    if (!error.IsOK()) {
        YT_VERIFY(InvokeResult_ == EInvokeResult::Executed);
        context->Reply(error);
    }

    return InvokeResult_;
}

TNodeProxyBase::TNodeProxyBase(IBootstrap* bootstrap, TSequoiaSessionPtr sequoiaSession)
    : Bootstrap_(bootstrap)
    , SequoiaSession_(std::move(sequoiaSession))
{ }

void TNodeProxyBase::BeforeInvoke(const ISequoiaServiceContextPtr& /*context*/)
{ }

bool TNodeProxyBase::DoInvoke(const ISequoiaServiceContextPtr& /*context*/)
{
    return false;
}

void TNodeProxyBase::AfterInvoke(const ISequoiaServiceContextPtr& /*context*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
