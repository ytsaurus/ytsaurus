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

    REGISTER_YSON_STRUCT(TPermissionCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // NYT::NSecurityClient
