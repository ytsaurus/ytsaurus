#include "permission_cache.h"

#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

namespace NYT::NSecurityClient {

using namespace NApi::NNative;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPermissionKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, Object);
    HashCombine(result, Acl);
    HashCombine(result, User);
    HashCombine(result, Permission);
    if (Columns) {
        // In order to distinguish between nullopt and [].
        HashCombine(result, 1);
        for (const auto& column : *Columns) {
            HashCombine(result, column);
        }
    }
    if (Vital) {
        // In order to distinguish between nullopt and false.
        HashCombine(result, 1);
        HashCombine(result, Vital);
    }
    return result;
}

void TPermissionKey::AssertValidity() const
{
    // Perform sanity check that key is correct only in Debug mode.
    YT_ASSERT(Object.has_value() || Acl.has_value());
    YT_ASSERT(!Object.has_value() || !Acl.has_value());
    YT_ASSERT(Object.has_value() || !Columns.has_value());
    YT_ASSERT(Object.has_value() || !Vital.has_value());
}

bool TPermissionKey::operator == (const TPermissionKey& other) const
{
    return
        Object == other.Object &&
        Acl == other.Acl &&
        User == other.User &&
        Permission == other.Permission &&
        Columns == other.Columns &&
        Vital == other.Vital;
}

TString ToString(const TPermissionKey& key)
{
    TStringBuilder builder;
    builder.AppendFormat(
        "%v:%v:%v",
        key.Object ? TStringBuf(*key.Object) : key.Acl->AsStringBuf(),
        key.User,
        key.Permission);

    // Format optional part.
    bool isFirst = true;

    auto append = [&] (auto format, auto value) {
        if (!value) {
            return;
        }
        builder.AppendString(isFirst ? ":{" : ", ");
        isFirst = false;
        builder.AppendFormat(format, value);
    };

    append("Columns: %v", key.Columns);
    append("Vital: %v", key.Vital);

    if (!isFirst) {
        builder.AppendString("}");
    }

    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TPermissionCache::TPermissionCache(
    TPermissionCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache(
        config,
        SecurityClientLogger.WithTag("Cache: Permission"),
        std::move(profiler))
    , Config_(std::move(config))
    , Connection_(std::move(connection))
{ }

TFuture<void> TPermissionCache::DoGet(const TPermissionKey& key, bool isPeriodicUpdate) noexcept
{
    key.AssertValidity();

    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<void>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
    }

    TObjectServiceProxy proxy(
        connection,
        Config_->MasterReadOptions.ReadFrom,
        PrimaryMasterCellTagSentinel,
        connection->GetStickyGroupSizeCache());
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, connection, Config_->MasterReadOptions);
    batchReq->SetUser(isPeriodicUpdate || Config_->AlwaysUseRefreshUser ? Config_->RefreshUser : key.User);
    batchReq->AddRequest(MakeRequest(connection, key));

    return batchReq->Invoke()
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            if (key.Object) {
                auto rspOrError = batchRsp->GetResponse<TObjectYPathProxy::TRspCheckPermission>(0);
                ParseCheckPermissionResponse(key, rspOrError)
                    .ThrowOnError();
            } else {
                auto rspOrError = batchRsp->GetResponse<TMasterYPathProxy::TRspCheckPermissionByAcl>(0);
                ParseCheckPermissionByAclResponse(key, rspOrError)
                    .ThrowOnError();
            }
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

    TObjectServiceProxy proxy(
        connection,
        Config_->MasterReadOptions.ReadFrom,
        PrimaryMasterCellTagSentinel,
        /*stickyGroupSizeCache*/ nullptr);
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetUser(Config_->RefreshUser);
    for (const auto& key : keys) {
        key.AssertValidity();
        batchReq->AddRequest(MakeRequest(connection, key));
    }

    return batchReq->Invoke()
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            std::vector<TError> results;
            results.reserve(keys.size());
            YT_ASSERT(std::ssize(keys) == batchRsp->GetResponseCount());
            for (int index = 0; index < std::ssize(keys); ++index) {
                const auto& key = keys[index];
                if (key.Object) {
                    const auto& rspOrError = batchRsp->GetResponse<TObjectYPathProxy::TRspCheckPermission>(index);
                    results.push_back(ParseCheckPermissionResponse(key, rspOrError));
                } else {
                    const auto& rspOrError = batchRsp->GetResponse<TMasterYPathProxy::TRspCheckPermissionByAcl>(index);
                    results.push_back(ParseCheckPermissionByAclResponse(key, rspOrError));
                }
            }
            return results;
        }));
}

bool TPermissionCache::CanCacheError(const TError& error) noexcept
{
    return error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError).has_value();
}

NYTree::TYPathRequestPtr TPermissionCache::MakeRequest(
    const IConnectionPtr& connection,
    const TPermissionKey& key)
{
    TYPathRequestPtr req;
    if (key.Object) {
        auto typedReq = TObjectYPathProxy::CheckPermission(*key.Object);
        typedReq->set_user(key.User);
        typedReq->set_permission(static_cast<int>(key.Permission));
        if (key.Columns) {
            ToProto(typedReq->mutable_columns()->mutable_items(), *key.Columns);
        }
        if (key.Vital) {
            typedReq->set_vital(*key.Vital);
        }
        typedReq->set_ignore_safe_mode(true);
        req = std::move(typedReq);
    } else {
        auto typedReq = TMasterYPathProxy::CheckPermissionByAcl();
        typedReq->set_user(key.User);
        typedReq->set_permission(static_cast<int>(key.Permission));
        typedReq->set_acl(key.Acl->ToString());
        typedReq->set_ignore_missing_subjects(true);
        req = std::move(typedReq);
    }
    SetCachingHeader(req, connection, Config_->MasterReadOptions);
    NCypressClient::SetSuppressAccessTracking(req, true);
    return req;
}

TError TPermissionCache::ParseCheckPermissionResponse(
    const TPermissionKey& key,
    const TObjectYPathProxy::TErrorOrRspCheckPermissionPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
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

TError TPermissionCache::ParseCheckPermissionByAclResponse(
    const TPermissionKey& key,
    const TMasterYPathProxy::TErrorOrRspCheckPermissionByAclPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
        return rspOrError;
    }
    const auto& rsp = rspOrError.Value();

    NApi::TCheckPermissionByAclResult result;
    result.Action = CheckedEnumCast<ESecurityAction>(rsp->action());
    result.SubjectId = FromProto<TSubjectId>(rsp->subject_id());
    result.SubjectName = rsp->has_subject_name() ? std::make_optional(rsp->subject_name()) : std::nullopt;
    result.MissingSubjects = FromProto<std::vector<TString>>(rsp->missing_subjects());
    return result.ToError(key.User, key.Permission);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
