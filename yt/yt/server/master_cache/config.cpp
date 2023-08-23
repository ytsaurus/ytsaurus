#include "config.h"

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

void TMasterCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();

    registrar.Parameter("caching_object_service", &TThis::CachingObjectService)
        .DefaultNew();

    registrar.Parameter("chaos_cache", &TThis::ChaosCache)
        .DefaultNew();

    registrar.Parameter("cypress_registrar", &TThis::CypressRegistrar)
        .DefaultNew();

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TMasterCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("caching_object_service", &TThis::CachingObjectService)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
