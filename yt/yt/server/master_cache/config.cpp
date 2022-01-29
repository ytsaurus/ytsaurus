#include "config.h"

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

void TMasterCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();

    registrar.Parameter("cluster_connection", &TThis::ClusterConnection)
        .DefaultNew();

    registrar.Parameter("caching_object_service", &TThis::CachingObjectService)
        .DefaultNew();

    registrar.Parameter("chaos_cache", &TThis::ChaosCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
