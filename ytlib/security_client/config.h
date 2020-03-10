#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/core/misc/config.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TPermissionCacheConfig
    : TAsyncExpiringCacheConfig
{
    NApi::EMasterChannelKind ReadFrom;
    TString RefreshUser;
    bool AlwaysUseRefreshUser;

    TPermissionCacheConfig()
    {
        RegisterParameter("read_from", ReadFrom)
            .Default(NApi::EMasterChannelKind::Cache);
        RegisterParameter("refresh_user", RefreshUser)
            // COMPAT(babenko): separate user
            .Default(RootUserName);
        RegisterParameter("always_use_refresh_user", AlwaysUseRefreshUser)
            // COMPAT(babenko): turn this off and remove the feature flag
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // NYT::NSecurityClient
