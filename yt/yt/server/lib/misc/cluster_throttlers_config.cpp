#include "cluster_throttlers_config.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

namespace NYT::NServer {

using namespace NApi;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TClusterThrottlersConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("distributed_throttler", &TThis::DistributedThrottler)
        .DefaultNew();
    registrar.Parameter("cluster_limits", &TThis::ClusterLimits)
        .Default();
    registrar.Parameter("max_estimated_time_to_read_pending_bytes_threshold", &TThis::MaxEstimatedTimeToReadPendingBytesThreshold)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("min_estimated_time_to_read_pending_bytes_threshold", &TThis::MinEstimatedTimeToReadPendingBytesThreshold)
        .Default(TDuration::Seconds(0));
    registrar.Parameter("rate_limit_ratio_hard_threshold", &TThis::RateLimitRatioHardThreshold)
        .Default(1.0);
    registrar.Parameter("rate_limit_ratio_soft_threshold", &TThis::RateLimitRatioSoftThreshold)
        .Default(0.98)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(1);
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TClusterLimitsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bandwidth", &TThis::Bandwidth)
        .DefaultNew();
    registrar.Parameter("rps", &TThis::Rps)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TYsonString> GetClusterThrottlersYson(const NNative::IClientPtr& client)
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    return client->GetNode(ClusterThrottlersConfigPath, std::move(options));
}

bool AreClusterThrottlersConfigsEqual(TClusterThrottlersConfigPtr lhs, TClusterThrottlersConfigPtr rhs)
{
    if (!lhs && !rhs) {
        return true;
    }

    if (!lhs != !rhs) {
        return false;
    }

    return NYson::ConvertToYsonString(lhs) == NYson::ConvertToYsonString(rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
