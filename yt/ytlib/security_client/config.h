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

    TPermissionCacheConfig()
    {
        RegisterParameter("read_from", ReadFrom)
            .Default(NApi::EMasterChannelKind::Cache);
    }
};

DEFINE_REFCOUNTED_TYPE(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // NYT::NSecurityClient
