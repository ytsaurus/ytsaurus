#include "deploy_sd.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/fetch/fetch.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>


namespace NYql {

static const TStringBuf SdClusters[] = {"sas"_sb, "vla"_sb, "klg"_sb, "nrg"_sb};

void DeployServiceToHosts(TStringBuf service, ui32 expectedHosts, TVector<TString>* hosts)
{
    const auto url = ParseURL("http://sd.yandex.net:8080/resolve_endpoints/json");

    ui32 discoveredHosts = 0;
    for (auto cluster : SdClusters) {
        auto body = NJson::TJsonValue(NJson::JSON_MAP);
        body["cluster_name"] = cluster;
        body["endpoint_set_id"] = service;
        body["client_name"] = "yqlapi";
        auto res = FetchEx(url, "POST"_sb, body.GetStringRobust(), {}, TDuration::Minutes(1), 10);
        if (res->GetRetCode() == 200) {
            NJson::TJsonValue content;
            if (NJson::ReadJsonTree(&res->GetStream(), &content)) {
                if (auto endpoints = content.GetValueByPath("endpoint_set.endpoints")) {
                    for (const auto& endpoint : endpoints->GetArray()) {
                        if (auto fqdn = endpoint.GetValueByPath("fqdn")) {
                            hosts->push_back(fqdn->GetString());
                            ++discoveredHosts;
                        }
                    }
                }
            } else {
                YQL_LOG(ERROR) << "Can't parse json from SD response";
                throw yexception() << "Can't parse json from SD response";
            }
        } else {
            YQL_LOG(ERROR) << "Can't resolve hosts from service discovery for service " << service << ", status=" << res->GetRetCode() << ", body:" << res->GetStream().ReadAll();
            throw yexception() << "Can't resolve hosts from service discovery for service " << service;
        }
    }
    if (discoveredHosts != expectedHosts) {
        YQL_LOG(ERROR) << "Unexpected number of hosts from service discovery for service " << service << ", expected " << expectedHosts << " hosts, but got " << discoveredHosts;
        throw yexception() << "Unexpected number of hosts from service discovery for service " << service << ", expected " << expectedHosts << " hosts, but got " << discoveredHosts;
    }
}

} // namespace NYql
