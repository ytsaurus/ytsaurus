#pragma once
#ifndef OBJECT_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_detail.h"
// For the sake of sane code completion.
#include "object_detail.h"
#endif

#include <yt/server/master/cell_master/multicell_manager.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/yson/string.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

// XXX(babenko): move to cpp
template <class TObject>
TFuture<NYson::TYsonString> TNonversionedObjectProxyBase<TObject>::FetchFromShepherd(const NYPath::TYPath& path)
{
    const auto multicellManager = Bootstrap_->GetMulticellManager();
    YT_ASSERT(multicellManager->IsSecondaryMaster());

    auto primaryCellTag = multicellManager->GetPrimaryCellTag();
    auto channel = multicellManager->FindMasterChannel(primaryCellTag, NHydra::EPeerKind::Follower);

    NObjectClient::TObjectServiceProxy proxy(channel);
    auto batchReq = proxy.ExecuteBatch();

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    const auto* user = securityManager->GetAuthenticatedUser();
    batchReq->SetUser(user->GetName());

    auto req = NYTree::TYPathProxy::Get(path);
    batchReq->AddRequest(req);

    return batchReq->Invoke()
        .Apply(BIND([=, this_ = MakeStrong(this)] (const NObjectClient::TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
            auto cumulativeError = GetCumulativeError(batchRspOrError);
            if (!cumulativeError.IsOK()) {
                THROW_ERROR_EXCEPTION("Error fetching %v from primary cell",
                    path)
                    << cumulativeError;
            }

            const auto& batchRsp = batchRspOrError.Value();
            auto rsp = batchRsp->GetResponse<NYTree::TYPathProxy::TRspGet>(0).Value();
            return NYson::TYsonString(rsp->value());
        })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
}

template <class TObject>
template <class T>
TFuture<std::vector<T>> TNonversionedObjectProxyBase<TObject>::FetchFromSwarm(NYTree::TInternedAttributeKey key)
{
    YT_ASSERT(IsPrimaryMaster());

    const auto* object = GetThisImpl();
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    const auto* user = securityManager->GetAuthenticatedUser();

    std::vector<TFuture<T>> asyncResults;

    for (auto cellTag : multicellManager->GetRegisteredMasterCellTags()) {
        auto channel = multicellManager->FindMasterChannel(cellTag, NHydra::EPeerKind::Follower);
        NObjectClient::TObjectServiceProxy proxy(channel);
        auto batchReq = proxy.ExecuteBatch();
        batchReq->SetUser(user->GetName());

        auto attribute = key.Unintern();
        auto path = NObjectClient::FromObjectId(object->GetId()) + "/@" + attribute;
        auto req = NYTree::TYPathProxy::Get(path);
        batchReq->AddRequest(req, "get");

        auto result = batchReq->Invoke()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const NObjectClient::TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                auto cumulativeError = GetCumulativeError(batchRspOrError);
                if (!cumulativeError.IsOK()) {
                    THROW_ERROR_EXCEPTION("Error fetching attribute %Qv from cell %v",
                        attribute,
                        cellTag)
                        << cumulativeError;
                }

                const auto& batchRsp = batchRspOrError.Value();
                auto rsp = batchRsp->GetResponse<NYTree::TYPathProxy::TRspGet>(0).Value();

                auto result = NYTree::ConvertTo<T>(NYson::TYsonString(rsp->value()));
                return result;
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));

        asyncResults.push_back(result);
    }

    return Combine(asyncResults);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
