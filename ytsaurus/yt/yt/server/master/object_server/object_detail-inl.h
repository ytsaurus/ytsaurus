#ifndef OBJECT_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_detail.h"
// For the sake of sane code completion.
#include "object_detail.h"
#endif

#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

// XXX(babenko): move to cpp
template <class TObject>
TFuture<NYson::TYsonString> TNonversionedObjectProxyBase<TObject>::FetchFromShepherd(const NYPath::TYPath& path)
{
    const auto multicellManager = Bootstrap_->GetMulticellManager();
    YT_ASSERT(multicellManager->IsSecondaryMaster());

    NObjectClient::TObjectServiceProxy proxy(
        Bootstrap_->GetClusterConnection(),
        NApi::EMasterChannelKind::Follower,
        NObjectClient::PrimaryMasterCellTagSentinel,
        /*stickyGroupSizeCache*/ nullptr);
    auto batchReq = proxy.ExecuteBatch();

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    const auto* user = securityManager->GetAuthenticatedUser();
    batchReq->SetUser(user->GetName());

    auto req = NYTree::TYPathProxy::Get(path);
    batchReq->AddRequest(req);

    return batchReq->Invoke()
        .Apply(BIND([=] (const NObjectClient::TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
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
        NObjectClient::TObjectServiceProxy proxy(
            Bootstrap_->GetClusterConnection(),
            NApi::EMasterChannelKind::Follower,
            cellTag,
            /*stickyGroupSizeCache*/ nullptr);
        auto batchReq = proxy.ExecuteBatch();
        batchReq->SetUser(user->GetName());

        auto attribute = key.Unintern();
        auto path = NObjectClient::FromObjectId(object->GetId()) + "/@" + attribute;
        auto req = NYTree::TYPathProxy::Get(path);
        batchReq->AddRequest(req, "get");

        auto result = batchReq->Invoke()
            .Apply(BIND([=] (const NObjectClient::TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
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

    return AllSucceeded(asyncResults);
}

template <class T>
/*static*/ void TObjectProxyBase::HandleCheckPermissionRequest(
    NCellMaster::TBootstrap* bootstrap,
    const TCtxCheckPermissionPtr& context,
    T doCheckPermission)
{
    auto* request = &context->Request();
    auto* response = &context->Response();

    const auto& userName = request->user();
    auto permission = CheckedEnumCast<NSecurityServer::EPermission>(request->permission());
    bool ignoreSafeMode = request->ignore_safe_mode();

    NSecurityServer::TPermissionCheckOptions checkOptions;
    if (request->has_columns()) {
        checkOptions.Columns = FromProto<std::vector<TString>>(request->columns().items());
    }
    if (request->has_vital()) {
        checkOptions.Vital = request->vital();
    }

    context->SetRequestInfo("User: %v, Permission: %v, Columns: %v, Vital: %v, IgnoreSafeMode: %v",
        userName,
        permission,
        checkOptions.Columns,
        checkOptions.Vital,
        ignoreSafeMode);

    const auto& securityManager = bootstrap->GetSecurityManager();
    if (!ignoreSafeMode && securityManager->IsSafeMode()) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::SafeModeEnabled,
            "Permission check is not possible: cluster is in safe mode; "
            "check for announces at https://infra.yandex-team.ru before reporting any issues");
    }

    auto* user = securityManager->GetUserByNameOrThrow(userName, true /*activeLifeStageOnly*/);

    // NB: this may throw, and it's OK.
    auto checkResponse = doCheckPermission(user, permission, std::move(checkOptions));

    const auto& objectManager = bootstrap->GetObjectManager();

    auto fillResult = [&] (auto* protoResult, const auto& result) {
        protoResult->set_action(static_cast<int>(result.Action));
        if (result.Object) {
            ToProto(protoResult->mutable_object_id(), result.Object->GetId());
            const auto& handler = objectManager->GetHandler(result.Object);
            protoResult->set_object_name(handler->GetName(result.Object));
        }
        if (result.Subject) {
            ToProto(protoResult->mutable_subject_id(), result.Subject->GetId());
            protoResult->set_subject_name(result.Subject->GetName());
        }
    };

    fillResult(response, checkResponse);
    if (checkResponse.Columns) {
        for (const auto& result : *checkResponse.Columns) {
            fillResult(response->mutable_columns()->add_items(), result);
        }
    }

    context->SetResponseInfo("Action: %v", checkResponse.Action);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
