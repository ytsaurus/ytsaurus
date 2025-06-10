#include "node_proxy_base.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NObjectClient;
using namespace NRpc;

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
    , NativeAuthenticatedClient_(
            Bootstrap_->GetNativeConnection()->CreateNativeClient(
                TClientOptions::FromAuthenticationIdentity(GetCurrentAuthenticationIdentity())))
{ }

void TNodeProxyBase::BeforeInvoke(const ISequoiaServiceContextPtr& /*context*/)
{ }

bool TNodeProxyBase::DoInvoke(const ISequoiaServiceContextPtr& /*context*/)
{
    return false;
}

void TNodeProxyBase::AfterInvoke(const ISequoiaServiceContextPtr& /*context*/)
{ }

TCellId TNodeProxyBase::CellIdFromCellTag(TCellTag cellTag) const
{
    return Bootstrap_->GetNativeConnection()->GetMasterCellId(cellTag);
}

TCellId TNodeProxyBase::CellIdFromObjectId(TObjectId id)
{
    return CellIdFromCellTag(CellTagFromId(id));
}

const NNative::IClientPtr& TNodeProxyBase::GetNativeAuthenticatedClient() const
{
    return NativeAuthenticatedClient_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
