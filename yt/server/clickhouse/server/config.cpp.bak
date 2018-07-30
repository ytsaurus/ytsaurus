#include "config.h"

#include "private.h"

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

TConfig::TConfig()
{
    RegisterParameter("cluster_connection", ClusterConnection);

    RegisterParameter("client_cache", ClientCache)
        .DefaultNew();

    RegisterParameter("scan_throttler", ScanThrottler)
        .Default();
}

}   // namespace NClickHouse
}   // namespace NYT
