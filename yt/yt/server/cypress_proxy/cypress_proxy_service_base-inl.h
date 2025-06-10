#pragma once
#ifndef CYPRESS_PROXY_SERVICE_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include cypress_proxy_service_base.h"
// For the sake of sane code completion.
#include "cypress_proxy_service_base.h"
#endif

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/cypress_client/proto/rpc.pb.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
void TCypressProxyServiceBase::InitContext(TCypressProxyServiceContext<TRequestMessage, TResponseMessage>* context)
{
    if (!context->RequestHeader().HasExtension(NCypressClient::NProto::TTargetMasterPeerExt::target_master_peer_ext)) {
        THROW_ERROR_EXCEPTION("Target master peer extension is missing");
    }

    const auto& ext = context->RequestHeader().GetExtension(NCypressClient::NProto::TTargetMasterPeerExt::target_master_peer_ext);
    auto cellTag = FromProto<NObjectClient::TCellTag>(ext.cell_tag());
    auto kind = FromProto<NApi::EMasterChannelKind>(ext.master_channel_kind());

    context->SetIncrementalRequestInfo("TargetMasterCellTag: %v, TargetMasterChannelKind: %v",
        cellTag,
        kind);

    YT_LOG_ALERT_AND_THROW_UNLESS(
        kind == NApi::EMasterChannelKind::Leader || kind == NApi::EMasterChannelKind::Follower,
        "Unexpected master channel kind received (MasterChannelKind: %v)",
        kind);

    context->SetTargetMasterCellTag(cellTag);
    context->SetTargetMasterChannelKind(kind);
}

template <class TRequestMessage, class TResponseMessage>
NRpc::IChannelPtr TCypressProxyServiceBase::GetTargetMasterPeerChannelOrThrow(TCypressProxyServiceContextPtr<TRequestMessage, TResponseMessage> context) const
{
    auto kind = context->GetTargetMasterChannelKind();
    auto cellTag = context->GetTargetMasterCellTag();

    const auto& masterCellDirectory = Bootstrap_->GetNativeConnection()->GetMasterCellDirectory();
    return masterCellDirectory->GetNakedMasterChannelOrThrow(kind, cellTag);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
