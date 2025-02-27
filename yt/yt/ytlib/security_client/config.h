#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TPermissionCacheConfig
    : public TAsyncExpiringCacheConfig
{
    NApi::TSerializableMasterReadOptionsPtr MasterReadOptions;
    std::string RefreshUser;
    bool AlwaysUseRefreshUser;

    REGISTER_YSON_STRUCT(TPermissionCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
