#include "config.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

void TUserAccessValidatorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("ban_cache", &TThis::BanCache)
        .Alias("user_cache")
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->BanCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(60);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
