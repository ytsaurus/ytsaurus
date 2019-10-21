#pragma once

#include "public.h"

#include "config.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TPermissionKey
{
    TString Object;
    TString User;
    NYTree::EPermission Permission;

    // Hasher.
    operator size_t() const;

    // Comparer.
    bool operator == (const TPermissionKey& other) const;

    // Formatter.
    friend TString ToString(const TPermissionKey& key);
};

////////////////////////////////////////////////////////////////////////////////

class TPermissionCache
    : public TAsyncExpiringCache<TPermissionKey, void>
{
public:
    TPermissionCache(
        TPermissionCacheConfigPtr config,
        NApi::NNative::IClientPtr client,
        NProfiling::TProfiler profiler = {});

    TFuture<std::vector<TError>> CheckPermissions(const std::vector<NYTree::TYPath>& paths, const TString& user, NYTree::EPermission permission);

private:
    NApi::NNative::IClientPtr Client_;
    TPermissionCacheConfigPtr Config_;

    virtual TFuture<void> DoGet(const TPermissionKey& key) override;

    virtual TFuture<std::vector<TError>> DoGetMany(const std::vector<TPermissionKey>& keys) override;
};

DEFINE_REFCOUNTED_TYPE(TPermissionCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
