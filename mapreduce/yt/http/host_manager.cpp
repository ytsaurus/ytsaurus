#include "host_manager.h"

#include "http.h"

#include <mapreduce/yt/interface/logging/yt_log.h>

#include <mapreduce/yt/common/config.h>

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
    { }

    explicit TClusterHostList(std::exception_ptr error)
        : Error_(std::move(error))
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

private:
    TVector<TString> Hosts_;
    std::exception_ptr Error_;
};

////////////////////////////////////////////////////////////////////////////////

THostManager::THostManager()
{
    Restart();
}

void THostManager::Restart()
{
    UpdateHandle_ = NCron::StartPeriodicJob(
        // NB. It's safe to use raw pointer as the thread is joined
        // in the destructor of UpdateHandle_.
        [this] {
            UpdateHosts();
        },
        TConfig::Get()->HostListUpdateInterval);
}

THostManager& THostManager::Get()
{
    return *Singleton<THostManager>();
}

TString THostManager::GetProxyForHeavyRequest(TStringBuf cluster)
{
    {
        auto guard = Guard(Lock_);
        auto it = ClusterHosts_.find(cluster);
        if (it != ClusterHosts_.end()) {
            return it->second.ChooseHostOrThrow();
        }
    }

    auto hostList = GetHosts(cluster);
    auto result = hostList.ChooseHostOrThrow();
    {
        auto guard = Guard(Lock_);
        ClusterHosts_.emplace(cluster, std::move(hostList));
    }
    return result;
}

THostManager::TClusterHostList THostManager::GetHosts(TStringBuf cluster)
{
    TString hostsEndpoint = TConfig::Get()->Hosts;
    while (hostsEndpoint.StartsWith("/")) {
        hostsEndpoint = hostsEndpoint.substr(1);
    }
    THttpHeader header("GET", hostsEndpoint, false);

    try {
        THttpRequest request;
        // TODO: we need to set socket timeout here
        request.Connect(TString(cluster));
        request.SmallRequest(header, {});
        auto hosts = ParseJsonStringArray(request.GetResponse());
        return TClusterHostList(std::move(hosts));
    } catch (const std::exception& e) {
        return TClusterHostList(std::current_exception());
    }
}

void THostManager::UpdateHosts()
{
    TVector<TString> clusters;
    {
        auto guard = Guard(Lock_);
        for (const auto& [cluster, hosts]: ClusterHosts_) {
            clusters.push_back(cluster);
        }
    }

    YT_LOG_DEBUG("Fetching host lists (Clusters: [%v])", JoinStrings(clusters, ", "));
    THashMap<TString, TClusterHostList> newClusterHosts;
    for (const auto& cluster : clusters) {
        newClusterHosts.emplace(cluster, GetHosts(cluster));
    }
    YT_LOG_DEBUG("Fetched host lists (Clusters: [%v])", JoinStrings(clusters, ", "));

    {
        auto guard = Guard(Lock_);
        std::swap(newClusterHosts, ClusterHosts_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPrivate
