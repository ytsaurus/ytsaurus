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

    RegisterParameter("validate_operation_permission", ValidateOperationPermission)
        .Default(true);
}

}   // namespace NClickHouse
}   // namespace NYT
