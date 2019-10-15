#include "permission_cache.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/object_ypath_proxy.h>

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
    return result;
}

bool TPermissionKey::operator == (const TPermissionKey& other) const
{
    return
        Object == other.Object &&
        User == other.User &&
        Permission == other.Permission;
}

TString ToString(const TPermissionKey& key)
{
    return Format("%v:%v:%v",
        key.Object,
        key.User,
        key.Permission);
}

////////////////////////////////////////////////////////////////////////////////

TPermissionCache::TPermissionCache(
    TPermissionCacheConfigPtr config,
    IClientPtr client,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache(config, std::move(profiler))
    , Client_(std::move(client))
    , Config_(std::move(config))
{ }

TFuture<std::vector<TError>> TPermissionCache::CheckPermissions(const std::vector<TYPath>& paths, const TString& user, EPermission permission)
{
    std::vector<TPermissionKey> keys;
    keys.reserve(paths.size());
    for (const auto& path : paths) {
        keys.push_back({path, user, permission});
    }
    return Get(keys);
}

TFuture<void> TPermissionCache::DoGet(const TPermissionKey& key)
{
    return DoGetMany({key}).Apply(BIND([] (const std::vector<TError>& responses) {
            responses[0].ThrowOnError();
        }));
}

TFuture<std::vector<TError>> TPermissionCache::DoGetMany(const std::vector<TPermissionKey>& keys)
{
    if (keys.empty()) {
        return {};
    }

    TObjectServiceProxy proxy(Client_->GetMasterChannelOrThrow(Config_->ReadFrom));
    auto batchReq = proxy.ExecuteBatch();
    for (const auto& key : keys) {
        auto req = TObjectYPathProxy::CheckPermission(key.Object);
        req->set_user(key.User);
        req->set_permission(static_cast<int>(key.Permission));
        NCypressClient::SetSuppressAccessTracking(req, true);
        batchReq->AddRequest(req);
    }

    return batchReq->Invoke()
        .Apply(BIND([=] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            auto responses = batchRsp->GetResponses<TObjectYPathProxy::TRspCheckPermission>();
            std::vector<TError> result;
            result.reserve(keys.size());
            for (int i = 0; i < keys.size(); ++i) {
                if (!responses[i].IsOK()) {
                    result.push_back(responses[i]);
                    continue;
                }
                // TODO(dakovalkov): Remove this copy-paste code from native client.
                auto fillResult = [] (auto* result, const auto& protoResult) {
                    result->Action = CheckedEnumCast<ESecurityAction>(protoResult.action());
                    result->ObjectId = FromProto<TObjectId>(protoResult.object_id());
                    result->ObjectName = protoResult.has_object_name() ? std::make_optional(protoResult.object_name()) : std::nullopt;
                    result->SubjectId = FromProto<TSubjectId>(protoResult.subject_id());
                    result->SubjectName = protoResult.has_subject_name() ? std::make_optional(protoResult.subject_name()) : std::nullopt;
                };

                NApi::TCheckPermissionResponse response;
                const auto& rsp = responses[i].Value();
                fillResult(&response, *rsp);
                // TODO(dakovalkov): YT-10367, columnar ACL
                if (rsp->has_columns()) {
                    response.Columns.emplace();
                    response.Columns->reserve(static_cast<size_t>(rsp->columns().items_size()));
                    for (const auto& protoResult : rsp->columns().items()) {
                        fillResult(&response.Columns->emplace_back(), protoResult);
                    }
                }
                result.push_back(response.ToError(keys[i].User, keys[i].Permission));
            }
            return result;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
