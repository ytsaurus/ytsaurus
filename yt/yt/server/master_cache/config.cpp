#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

TMasterCacheConfig::TMasterCacheConfig()
{
    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);

    RegisterParameter("bus_client", BusClient)
        .DefaultNew();

    RegisterParameter("cluster_connection", ClusterConnection)
        .DefaultNew();

    RegisterParameter("caching_object_service", CachingObjectService)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
