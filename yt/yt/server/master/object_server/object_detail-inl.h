#ifndef OBJECT_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_detail.h"
// For the sake of sane code completion.
#include "object_detail.h"
#endif

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/ytlib/security_client/acl.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
template <class T>
TFuture<std::vector<T>> TNonversionedObjectProxyBase<TObject>::FetchFromSwarm(NYTree::TInternedAttributeKey key)
{
    auto ysonResults = FetchYsonFromSwarm(key);
    std::vector<TFuture<T>> asyncResults;
    asyncResults.reserve(ysonResults.size());
    for (const auto& ysonResult : ysonResults) {
        asyncResults.push_back(ysonResult.Apply(
            BIND([] (const NYson::TYsonString& value) {
                return NYTree::ConvertTo<T>(value);
            })));
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
    auto permission = FromProto<NSecurityServer::EPermission>(request->permission());
    bool ignoreSafeMode = request->ignore_safe_mode();

    NSecurityServer::TPermissionCheckOptions checkOptions;
    if (request->has_columns()) {
        checkOptions.Columns = FromProto<std::vector<std::string>>(request->columns().items());
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

    // NB: This may throw, and it's OK.
    auto checkResponse = doCheckPermission(user, permission, std::move(checkOptions));

    const auto& objectManager = bootstrap->GetObjectManager();

    auto fillResult = [&] (auto* protoResult, const auto& result) {
        protoResult->set_action(ToProto(result.Action));
        if (result.ObjectId) {
            ToProto(protoResult->mutable_object_id(), result.ObjectId);
            auto* object = objectManager->GetObject(result.ObjectId);
            const auto& handler = objectManager->GetHandler(object);
            protoResult->set_object_name(ToProto(handler->GetName(object)));
        }
        if (result.SubjectId) {
            ToProto(protoResult->mutable_subject_id(), result.SubjectId);
            auto* subject = securityManager->GetSubjectOrThrow(result.SubjectId);
            protoResult->set_subject_name(ToProto(subject->GetName()));
        }
    };

    fillResult(response, checkResponse);
    if (checkResponse.Columns) {
        for (const auto& result : *checkResponse.Columns) {
            fillResult(response->mutable_columns()->add_items(), result);
        }
    }
    if (checkResponse.RowLevelAcl) {
        ToProto(response->mutable_row_level_acl()->mutable_items(), *checkResponse.RowLevelAcl);
    }

    context->SetResponseInfo("Action: %v", checkResponse.Action);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
