#include "cluster_throttlers_config.h"

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

std::optional<NYT::NYson::TYsonString> GetClusterThrottlersYson(NApi::NNative::IClientPtr client)
{
    static constexpr auto ClusterThrottlersConfigPath = "//sys/cluster_throttlers";

    NApi::TGetNodeOptions options;
    options.ReadFrom = NApi::EMasterChannelKind::MasterCache;
    auto errorOrYson = NConcurrency::WaitFor(client->GetNode(ClusterThrottlersConfigPath, std::move(options)));
    if (!errorOrYson.IsOK()) {
        return std::nullopt;
    }

    return errorOrYson.Value();
}

TClusterThrottlersConfigPtr MakeClusterThrottlersConfig(const NYT::NYson::TYsonString& yson)
{
    try {
        auto config = New<TClusterThrottlersConfig>();
        config->Load(NYTree::ConvertTo<NYTree::INodePtr>(yson));
        return config;
    } catch (const std::exception& ex) {
        return nullptr;
    }
}

TClusterThrottlersConfigPtr GetClusterThrottlersConfig(NApi::NNative::IClientPtr client)
{
    auto yson = GetClusterThrottlersYson(client);
    if (!yson) {
        return nullptr;
    }

    return MakeClusterThrottlersConfig(*yson);
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
