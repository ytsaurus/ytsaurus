#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TPermissionKey
{
    //! Permission will be validated via `CheckPermission` YPath request for this path.
    NYPath::TYPath Path;

    std::string User;
    NYTree::EPermission Permission;

    std::optional<std::vector<std::string>> Columns;
    std::optional<bool> Vital;

    // COMPAT(coteeq)
    bool CallerIsRlsAware = false;

    // Hasher.
    operator size_t() const;

    // Comparer.
    bool operator==(const TPermissionKey& other) const;

    // Formatter.
    friend void FormatValue(TStringBuilderBase* builder, const TPermissionKey& key, TStringBuf /*spec*/);
};

struct TPermissionValue
{
    std::optional<std::vector<TRowLevelAccessControlEntry>> RowLevelAcl;
};

////////////////////////////////////////////////////////////////////////////////

//! This cache is able to store cached results both for `CheckPermission` and `CheckPermissionByAcl`
//! YPath requests.
class TPermissionCache
    : public TAsyncExpiringCache<TPermissionKey, TPermissionValue>
{
public:
    TPermissionCache(
        TPermissionCacheConfigPtr config,
        NApi::NNative::IConnectionPtr connection,
        NProfiling::TProfiler profiler = {});

private:
    const TPermissionCacheConfigPtr Config_;
    const TWeakPtr<NApi::NNative::IConnection> Connection_;

    TFuture<TPermissionValue> DoGet(
        const TPermissionKey& key,
        bool isPeriodicUpdate) noexcept override;
    TFuture<std::vector<TErrorOr<TPermissionValue>>> DoGetMany(
        const std::vector<TPermissionKey>& keys,
        bool isPeriodicUpdate) noexcept override;

    //! This method allows overloading logic of which errors should be cached during background entry update.
    //! By default all errors are cached, but when background update is performed using cache user,
    //! you may want to discard unrelated errors (like request queue size exceeded) from being cached.
    bool CanCacheError(const TError& error) noexcept override;

    //! Make proper request for given key: `TReqCheckPermission` for keys with `Object` set,
    //! `TReqCheckPermissionByAcl` for keys with `Acl` set.
    NYTree::TYPathRequestPtr MakeRequest(
        const NApi::NNative::IConnectionPtr& connection,
        const TPermissionKey& key);

    TErrorOr<TPermissionValue> ParseCheckPermissionResponse(
        const TPermissionKey& key,
        const NObjectClient::TObjectYPathProxy::TErrorOrRspCheckPermissionPtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TPermissionCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
