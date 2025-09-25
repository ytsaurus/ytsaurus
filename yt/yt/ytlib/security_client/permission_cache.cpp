#include "permission_cache.h"

#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/security_client/acl.h>

namespace NYT::NSecurityClient {

using namespace NApi::NNative;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPermissionKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, Path);
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
    HashCombine(result, CallerIsRlsAware);
    return result;
}

bool TPermissionKey::operator == (const TPermissionKey& other) const
{
    return
        Path == other.Path &&
        User == other.User &&
        Permission == other.Permission &&
        Columns == other.Columns &&
        Vital == other.Vital &&
        CallerIsRlsAware == other.CallerIsRlsAware;
}

void FormatValue(TStringBuilderBase* builder, const TPermissionKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "%v:%v:%v",
        key.Path,
        key.User,
        key.Permission);

    // Format optional part.
    bool isFirst = true;

    auto append = [&] (auto format, auto value) {
        if (!value) {
            return;
        }
        builder->AppendString(isFirst ? ":{" : ", ");
        isFirst = false;
        builder->AppendFormat(format, value);
    };

    append("Columns: %v", key.Columns);
    append("Vital: %v", key.Vital);
    append("CallerIsRlsAware: %v", key.CallerIsRlsAware);

    if (!isFirst) {
        builder->AppendString("}");
    }
}

////////////////////////////////////////////////////////////////////////////////

TPermissionCache::TPermissionCache(
    TPermissionCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache(
        config,
        SecurityClientLogger().WithTag("Cache: Permission"),
        std::move(profiler))
    , Config_(std::move(config))
    , Connection_(std::move(connection))
{ }

TFuture<TPermissionValue> TPermissionCache::DoGet(const TPermissionKey& key, bool isPeriodicUpdate) noexcept
{
    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<TPermissionValue>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
    }

    TObjectServiceProxy proxy(
        connection,
        Config_->MasterReadOptions->ReadFrom,
        PrimaryMasterCellTagSentinel,
        connection->GetStickyGroupSizeCache());
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, connection, *Config_->MasterReadOptions);
    batchReq->SetUser(isPeriodicUpdate || Config_->AlwaysUseRefreshUser ? Config_->RefreshUser : key.User);
    batchReq->AddRequest(MakeRequest(connection, key));

    return batchReq->Invoke()
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
                auto rspOrError = batchRsp->GetResponse<TObjectYPathProxy::TRspCheckPermission>(0);
                return ParseCheckPermissionResponse(key, rspOrError)
                    .ValueOrThrow();
        }));
}

TFuture<std::vector<TErrorOr<TPermissionValue>>> TPermissionCache::DoGetMany(
    const std::vector<TPermissionKey>& keys,
    bool isPeriodicUpdate) noexcept
{
    if (keys.empty()) {
        return MakeFuture(std::vector<TErrorOr<TPermissionValue>>());
    }

    if (!isPeriodicUpdate) {
        return TAsyncExpiringCache::DoGetMany(keys, false);
    }

    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<std::vector<TErrorOr<TPermissionValue>>>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
    }

    TObjectServiceProxy proxy(
        connection,
        Config_->MasterReadOptions->ReadFrom,
        PrimaryMasterCellTagSentinel,
        /*stickyGroupSizeCache*/ nullptr);
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, connection, *Config_->MasterReadOptions);
    batchReq->SetUser(Config_->RefreshUser);
    for (const auto& key : keys) {
        batchReq->AddRequest(MakeRequest(connection, key));
    }

    return batchReq->Invoke()
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            std::vector<TErrorOr<TPermissionValue>> results;
            results.reserve(keys.size());
            YT_ASSERT(std::ssize(keys) == batchRsp->GetResponseCount());
            for (int index = 0; index < std::ssize(keys); ++index) {
                const auto& key = keys[index];
                const auto& rspOrError = batchRsp->GetResponse<TObjectYPathProxy::TRspCheckPermission>(index);
                results.push_back(ParseCheckPermissionResponse(key, rspOrError));
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
    using NYT::ToProto;

    TYPathRequestPtr req;
    auto typedReq = TObjectYPathProxy::CheckPermission(key.Path);
    typedReq->set_user(ToProto(key.User));
    typedReq->set_permission(ToProto(key.Permission));
    if (key.Columns) {
        ToProto(typedReq->mutable_columns()->mutable_items(), *key.Columns);
    }
    if (key.Vital) {
        typedReq->set_vital(*key.Vital);
    }
    typedReq->set_ignore_safe_mode(true);
    req = std::move(typedReq);
    SetCachingHeader(req, connection, *Config_->MasterReadOptions);
    NCypressClient::SetSuppressAccessTracking(req, true);
    return req;
}

TErrorOr<TPermissionValue> TPermissionCache::ParseCheckPermissionResponse(
    const TPermissionKey& key,
    const TObjectYPathProxy::TErrorOrRspCheckPermissionPtr& rspOrError)
{
    using NYT::FromProto;

    if (!rspOrError.IsOK()) {
        return TError("Error checking permissions for %v", key.Path)
            << rspOrError;
    }
    const auto& rsp = rspOrError.Value();

    // TODO(dakovalkov): Remove this copy-paste code from native client.
    auto parseResult = [] (const auto& protoResult) {
        NApi::TCheckPermissionResult result;
        result.Action = FromProto<ESecurityAction>(protoResult.action());
        result.ObjectId = FromProto<TObjectId>(protoResult.object_id());
        result.ObjectName = protoResult.has_object_name() ? std::make_optional(protoResult.object_name()) : std::nullopt;
        result.SubjectId = FromProto<TSubjectId>(protoResult.subject_id());
        result.SubjectName = protoResult.has_subject_name() ? std::make_optional(protoResult.subject_name()) : std::nullopt;
        return result;
    };

    TError error;
    auto checkError = [&] (const NApi::TCheckPermissionResult& result, const std::optional<std::string>& column) {
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

    if (!error.IsOK()) {
        return error;
    }

    if (rsp->has_row_level_acl()) {
        if (key.CallerIsRlsAware) {
            return TPermissionValue{
                .RowLevelAcl = FromProto<std::vector<TRowLevelAccessControlEntry>>(rsp->row_level_acl().items()),
            };
        } else {
            auto error = TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Access denied for user %Qv: row-level ACL is present, but is not supported by this method yet",
                key.User);
            error <<= TErrorAttribute("user", key.User);
            error <<= TErrorAttribute("permission", key.Permission);
            return error;
        }
    }

    return TErrorOr<TPermissionValue>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
