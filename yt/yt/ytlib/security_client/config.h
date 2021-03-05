#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

class TPermissionCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
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

        RegisterPreprocessor([&] {
            ExpireAfterAccessTime = TDuration::Minutes(5);
            ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(3);
            RefreshTime = TDuration::Minutes(1);
            BatchUpdate = true;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // NYT::NSecurityClient
