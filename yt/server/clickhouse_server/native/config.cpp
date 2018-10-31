#include "config.h"

#include "private.h"

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
