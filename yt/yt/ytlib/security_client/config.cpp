#include "config.h"

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

void TPermissionCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("read_from", &TThis::ReadFrom)
        .Default(NApi::EMasterChannelKind::Cache);
    registrar.Parameter("refresh_user", &TThis::RefreshUser)
        // COMPAT(babenko): separate user
        .Default(RootUserName);
    registrar.Parameter("always_use_refresh_user", &TThis::AlwaysUseRefreshUser)
        // COMPAT(babenko): turn this off and remove the feature flag
        .Default(true);

    registrar.Preprocessor([] (TThis* config) {
        config->ExpireAfterAccessTime = TDuration::Minutes(5);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(3);
        config->RefreshTime = TDuration::Minutes(1);
        config->BatchUpdate = true;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NSecurityClient
