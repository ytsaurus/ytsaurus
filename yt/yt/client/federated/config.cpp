#include "config.h"

namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

void TFederatedClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle_name", &TThis::BundleName)
        .Default();

    registrar.Parameter("check_clusters_health_period", &TThis::CheckClustersHealthPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(60));

    registrar.Parameter("cluster_retry_attempts", &TThis::ClusterRetryAttempts)
        .GreaterThanOrEqual(0)
        .Default(3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
