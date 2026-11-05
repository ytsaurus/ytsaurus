#include "yql_ytflow_config_clusters.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/utils/yql_panic.h>


namespace NYql::NYtflow {

TConfigClusters::TConfigClusters(const TYtflowGatewayConfig& config)
{
    for (const auto& clusterMapping : config.GetClusterMapping()) {
        Clusters.emplace(std::pair(clusterMapping.GetName(), TClusterInfo{
            .RealName = clusterMapping.GetRealName(),
            .ProxyUrl = clusterMapping.GetProxyUrl(),
            .Token = clusterMapping.GetToken(),
        }));
    }
}

TString TConfigClusters::GetAuth(const TString& clusterAlias) const
{
    const auto& iterator = Clusters.find(clusterAlias);
    YQL_ENSURE(iterator);
    return iterator->second.Token;
}

TString TConfigClusters::GetRealName(const TString& clusterAlias) const
{
    const auto& iterator = Clusters.find(clusterAlias);
    YQL_ENSURE(iterator);
    return iterator->second.RealName;
}

TString TConfigClusters::GetAlias(const TString& clusterRealName) const
{
    for (const auto& [alias, clusterInfo] : Clusters) {
        if (clusterInfo.RealName == clusterRealName) {
            return alias;
        }
    }

    YQL_ENSURE(false, "No Ytflow cluster mapping for YT cluster " << clusterRealName);
}

TString TConfigClusters::GetProxyUrl(const TString& clusterAlias) const
{
    const auto& iterator = Clusters.find(clusterAlias);
    YQL_ENSURE(iterator);
    return iterator->second.ProxyUrl;
}

THashMap<std::string, std::string> TConfigClusters::GetProxyUrlAliasingRules() const
{
    // TODO(ngc224): cache result
    THashMap<std::string, std::string> proxyUrlAliasingRules;
    for (const auto& [alias, clusterInfo] : Clusters) {
        proxyUrlAliasingRules.emplace(std::pair(clusterInfo.RealName, clusterInfo.ProxyUrl));
    }

    return proxyUrlAliasingRules;
}

} // namespace NYql::NYtflow
