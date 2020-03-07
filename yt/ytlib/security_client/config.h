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

    TPermissionCacheConfig()
    {
        RegisterParameter("read_from", ReadFrom)
            .Default(NApi::EMasterChannelKind::Cache);
        RegisterParameter("refresh_user", RefreshUser)
            // TODO(babenko): separate user
            .Default(RootUserName);
    }
};

DEFINE_REFCOUNTED_TYPE(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // NYT::NSecurityClient
