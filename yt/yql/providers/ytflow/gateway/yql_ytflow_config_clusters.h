#pragma once

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <string>


namespace NYql {

class TYtflowGatewayConfig;

} // namespace NYql


namespace NYql::NYtflow {

class TConfigClusters
    : public TThrRefBase
{
public:
    using TPtr = TIntrusivePtr<TConfigClusters>;

private:
    struct TClusterInfo
    {
        TString RealName;
        TString ProxyUrl;
        TString Token;
    };

public:
    TConfigClusters(const TYtflowGatewayConfig& config);

public:
    TString GetAuth(const TString& clusterAlias) const;
    TString GetRealName(const TString& clusterAlias) const;
    TString GetProxyUrl(const TString& clusterAlias) const;

    THashMap<std::string, std::string> GetProxyUrlAliasingRules() const;

private:
    THashMap<TString, TClusterInfo> Clusters;
};

} // namespace NYql::NYtflow
