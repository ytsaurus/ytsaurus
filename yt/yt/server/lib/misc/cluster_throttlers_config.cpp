#include "cluster_throttlers_config.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TClusterThrottlersConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("distributed_throttler", &TThis::DistributedThrottler)
        .DefaultNew();
    registrar.Parameter("cluster_limits", &TThis::ClusterLimits)
        .Default();
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Postprocessor([] (TClusterThrottlersConfig* config) {
        for (const auto& [clusterName, _] : config->ClusterLimits) {
            if (clusterName.empty()) {
                THROW_ERROR_EXCEPTION("Invalid cluster name %Qv in \"cluster_limist\"",
                    clusterName);
            }
        }
    });
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

void TLimitConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("limit", &TThis::Limit)
        .Default()
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NYT::NYson::TYsonString> GetClusterThrottlersYson(NApi::NNative::IClientPtr client)
{
    NApi::TGetNodeOptions options;
    options.ReadFrom = NApi::EMasterChannelKind::Cache;
    return client->GetNode(ClusterThrottlersConfigPath, std::move(options));
}

TClusterThrottlersConfigPtr GetClusterThrottlersConfig(NApi::NNative::IClientPtr client)
{
    auto future = GetClusterThrottlersYson(client);
    auto errorOrYson = NConcurrency::WaitFor(future);
    if (!errorOrYson.IsOK()) {
        return nullptr;
    }

    return ConvertTo<TClusterThrottlersConfigPtr>(errorOrYson.Value());
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

} // namespace NYT
