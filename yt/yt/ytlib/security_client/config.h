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
    NApi::TSerializableMasterReadOptionsPtr MasterReadOptions;
    TString RefreshUser;
    bool AlwaysUseRefreshUser;

    REGISTER_YSON_STRUCT(TPermissionCacheConfig);

    static void Register(TRegistrar registrar);

private:
    // COMPAT(dakovalkov): This option is deprecated.
    // Use MasterReadOptions instead.
    // TODO(dakovalkov): delete it after elimination from all configs.
    std::optional<NApi::EMasterChannelKind> ReadFrom_;
};

DEFINE_REFCOUNTED_TYPE(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
