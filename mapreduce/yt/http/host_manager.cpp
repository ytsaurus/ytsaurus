#include "host_manager.h"

#include "helpers.h"
#include "http.h"
#include "http_client.h"
#include "requests.h"
#include "util/generic/guid.h"

#include <mapreduce/yt/interface/logging/yt_log.h>

#include <mapreduce/yt/interface/config.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/vector.h>
#include <util/generic/singleton.h>
#include <util/generic/ymath.h>

#include <util/random/random.h>

#include <util/string/vector.h>

namespace NYT::NPrivate {

////////////////////////////////////////////////////////////////////////////////

static TVector<TString> ParseJsonStringArray(const TString& response)
{
    NJson::TJsonValue value;
    TStringInput input(response);
    NJson::ReadJsonTree(&input, &value);

    const NJson::TJsonValue::TArray& array = value.GetArray();
    TVector<TString> result;
    result.reserve(array.size());
    for (size_t i = 0; i < array.size(); ++i) {
        result.push_back(array[i].GetString());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class THostManager::TClusterHostList
{
public:
    explicit TClusterHostList(TVector<TString> hosts)
        : Hosts_(std::move(hosts))
        , Timestamp_(TInstant::Now())
    { }

    explicit TClusterHostList(std::exception_ptr error)
        : Error_(std::move(error))
        , Timestamp_(TInstant::Now())
    { }

    TString ChooseHostOrThrow() const
    {
        if (Error_) {
            std::rethrow_exception(Error_);
        }

        if (Hosts_.empty()) {
            ythrow yexception() << "fetched list of proxies is empty";
        }

        return Hosts_[RandomNumber<size_t>(Hosts_.size())];
    }

    TDuration GetAge() const
    {
        return TInstant::Now() - Timestamp_;
    }

private:
    TVector<TString> Hosts_;
    std::exception_ptr Error_;
    TInstant Timestamp_;
};

////////////////////////////////////////////////////////////////////////////////

THostManager& THostManager::Get()
{
    return *Singleton<THostManager>();
}

void THostManager::Reset()
{
    auto guard = Guard(Lock_);
    ClusterHosts_.clear();
}

TString THostManager::GetProxyForHeavyRequest(const TAuth& auth)
{
    auto cluster = auth.ServerName;
    {
        auto guard = Guard(Lock_);
        auto it = ClusterHosts_.find(cluster);
        if (it != ClusterHosts_.end() && it->second.GetAge() < TConfig::Get()->HostListUpdateInterval) {
            return it->second.ChooseHostOrThrow();
        }
    }

    auto hostList = GetHosts(auth);
    auto result = hostList.ChooseHostOrThrow();
    {
        auto guard = Guard(Lock_);
        ClusterHosts_.emplace(cluster, std::move(hostList));
    }
    return result;
}

THostManager::TClusterHostList THostManager::GetHosts(const TAuth& auth)
{
    TString hostsEndpoint = TConfig::Get()->Hosts;
    while (hostsEndpoint.StartsWith("/")) {
        hostsEndpoint = hostsEndpoint.substr(1);
    }
    THttpHeader header("GET", hostsEndpoint, false);

    try {
        auto hostName = auth.ServerName;
        auto requestId = CreateGuidAsString();
        // TODO: we need to set socket timeout here
        auto response = auth.HttpClient->Request(GetFullUrl(hostName, auth, header), requestId, header);
        auto hosts = ParseJsonStringArray(response->GetResponse());
        for (auto& host : hosts) {
            host = CreateHostNameWithPort(host, auth);
        }
        return TClusterHostList(std::move(hosts));
    } catch (const std::exception& e) {
        return TClusterHostList(std::current_exception());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPrivate
