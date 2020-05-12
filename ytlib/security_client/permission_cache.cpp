#include "permission_cache.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/rpc_helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

namespace NYT::NSecurityClient {

using namespace NApi::NNative;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPermissionKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, Object);
    HashCombine(result, User);
    HashCombine(result, Permission);
    if (Columns) {
        for (const auto& column : *Columns) {
            HashCombine(result, column);
        }
    }
    return result;
}

bool TPermissionKey::operator == (const TPermissionKey& other) const
{
    return
        Object == other.Object &&
        User == other.User &&
        Permission == other.Permission &&
        Columns == other.Columns;
}

TString ToString(const TPermissionKey& key)
{
    return Format("%v:%v:%v:%v",
        key.Object,
        key.User,
        key.Permission,
        key.Columns);
}

////////////////////////////////////////////////////////////////////////////////

TPermissionCache::TPermissionCache(
    TPermissionCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache(config, std::move(profiler))
    , Config_(std::move(config))
    , Connection_(connection)
{ }

TFuture<void> TPermissionCache::DoGet(const TPermissionKey& key, bool isPeriodicUpdate) noexcept
{
    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<void>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
    }

    TObjectServiceProxy proxy(connection->GetMasterChannelOrThrow(Config_->ReadFrom));
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, connection->GetConfig(), GetMasterReadOptions());
    batchReq->SetUser(isPeriodicUpdate || Config_->AlwaysUseRefreshUser ? Config_->RefreshUser : key.User);
    batchReq->AddRequest(MakeCheckPermissionRequest(connection, key));

    return batchReq->Invoke()
        .Apply(BIND([=] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            auto rspOrError = batchRsp->GetResponse<TObjectYPathProxy::TRspCheckPermission>(0);
            ParseCheckPermissionResponse(key, rspOrError)
                .ThrowOnError();
        }));
}

TFuture<std::vector<TError>> TPermissionCache::DoGetMany(
    const std::vector<TPermissionKey>& keys,
    bool isPeriodicUpdate) noexcept
{
    if (keys.empty()) {
        return MakeFuture(std::vector<TError>());
    }

    if (!isPeriodicUpdate) {
        return TAsyncExpiringCache::DoGetMany(keys, false);
    }

    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<std::vector<TError>>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
    }

    TObjectServiceProxy proxy(connection->GetMasterChannelOrThrow(Config_->ReadFrom));
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetUser(Config_->RefreshUser);
    for (const auto& key : keys) {
        batchReq->AddRequest(MakeCheckPermissionRequest(connection, key));
    }

    return batchReq->Invoke()
        .Apply(BIND([=] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspCheckPermission>();
            std::vector<TError> results;
            results.reserve(keys.size());
            for (int index = 0; index < rspsOrError.size(); ++index) {
                const auto& rspOrError = rspsOrError[index];
                const auto& key = keys[index];
                results.push_back(ParseCheckPermissionResponse(key, rspOrError));
            }
            return results;
        }));
}

NApi::TMasterReadOptions TPermissionCache::GetMasterReadOptions()
{
    return NApi::TMasterReadOptions{
        Config_->ReadFrom,
        Config_->ExpireAfterSuccessfulUpdateTime,
        Config_->ExpireAfterFailedUpdateTime,
        1
    };
}

TObjectYPathProxy::TReqCheckPermissionPtr TPermissionCache::MakeCheckPermissionRequest(
    const IConnectionPtr& connection,
    const TPermissionKey& key)
{
    auto req = TObjectYPathProxy::CheckPermission(key.Object);
    req->set_user(key.User);
    req->set_permission(static_cast<int>(key.Permission));
    if (key.Columns) {
        ToProto(req->mutable_columns()->mutable_items(), *key.Columns);
    }
    req->set_ignore_safe_mode(true);
    SetCachingHeader(req, connection->GetConfig(), GetMasterReadOptions());
    NCypressClient::SetSuppressAccessTracking(req, true);
    return req;
}

TError TPermissionCache::ParseCheckPermissionResponse(
    const TPermissionKey& key,
    const TObjectYPathProxy::TErrorOrRspCheckPermissionPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
        // Skip resolve errors, these are better handled elsewhere.
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return {};
        }
        return TError("Error checking permissions for %v", key.Object)
            << rspOrError;
    }
    const auto& rsp = rspOrError.Value();

    // TODO(dakovalkov): Remove this copy-paste code from native client.
    auto parseResult = [] (const auto& protoResult) {
        NApi::TCheckPermissionResult result;
        result.Action = CheckedEnumCast<ESecurityAction>(protoResult.action());
        result.ObjectId = FromProto<TObjectId>(protoResult.object_id());
        result.ObjectName = protoResult.has_object_name() ? std::make_optional(protoResult.object_name()) : std::nullopt;
        result.SubjectId = FromProto<TSubjectId>(protoResult.subject_id());
        result.SubjectName = protoResult.has_subject_name() ? std::make_optional(protoResult.subject_name()) : std::nullopt;
        return result;
    };

    TError error;
    auto checkError = [&] (const NApi::TCheckPermissionResult& result, const std::optional<TString>& column) {
        if (!error.IsOK()) {
            return;
        }
        error = result.ToError(key.User, key.Permission, column);
    };

    checkError(parseResult(*rsp), std::nullopt);
    if (key.Columns && rsp->has_columns()) {
        for (int j = 0; j < rsp->columns().items_size(); ++j) {
            checkError(parseResult(rsp->columns().items(j)), (*key.Columns)[j]);
        }
    }

    return error;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
