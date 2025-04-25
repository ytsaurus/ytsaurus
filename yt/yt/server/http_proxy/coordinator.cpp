#include "coordinator.h"

#include "api.h"
#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>
#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/client.h>

// TODO(nadya73): move it outside of 'rpc_proxy' or rename it somehow.
#include <yt/yt/client/api/rpc_proxy/address_helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/https/config.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/library/profiling/resource_tracker/resource_tracker.h>

#include <yt/yt/build/build.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/vector.h>

#include <util/random/shuffle.h>

#include <util/string/split.h>

#include <util/system/info.h>

namespace NYT::NHttpProxy {

using namespace NApi;
using namespace NApi::NRpcProxy;
using namespace NConcurrency;
using namespace NTracing;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NHttp;
using namespace NCypressClient;
using namespace NNative;
using namespace NProfiling;
using namespace NObjectClient;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

void TLiveness::Register(TRegistrar registrar)
{
    registrar.Parameter("updated_at", &TThis::UpdatedAt)
        .Default();
    registrar.Parameter("load_average", &TThis::LoadAverage)
        .Default();
    registrar.Parameter("network_coef", &TThis::NetworkCoef)
        .Default();
    registrar.Parameter("user_cpu", &TThis::UserCpu)
        .Default();
    registrar.Parameter("system_cpu", &TThis::SystemCpu)
        .Default();
    registrar.Parameter("cpu_wait", &TThis::CpuWait)
        .Default();
    registrar.Parameter("concurrent_requests", &TThis::ConcurrentRequests)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TProxyEntry::Register(TRegistrar registrar)
{
    registrar.Parameter("role", &TThis::Role);
    registrar.Parameter("endpoint", &TThis::Endpoint)
        .Optional();
    registrar.Parameter("banned", &TThis::IsBanned)
        .Default(false);
    registrar.Parameter("liveness", &TThis::Liveness)
        .DefaultNew();
    registrar.Parameter(BanMessageAttributeName, &TThis::BanMessage)
        .Default();
}

std::string TProxyEntry::GetHost() const
{
    return std::string(NNet::GetServiceHostName(Endpoint));
}

////////////////////////////////////////////////////////////////////////////////

TCoordinatorProxy::TCoordinatorProxy(const TProxyEntryPtr& proxyEntry)
    : Entry(proxyEntry)
{ }

////////////////////////////////////////////////////////////////////////////////

TCoordinator::TCoordinator(
    TProxyBootstrapConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config->Coordinator)
    , Sampler_(New<TSampler>())
    , Bootstrap_(bootstrap)
    , Client_(bootstrap->GetRootClient())
    , UpdateStateExecutor_(New<TPeriodicExecutor>(
        bootstrap->GetControlInvoker(),
        BIND(&TCoordinator::UpdateState, MakeWeak(this)),
        TPeriodicExecutorOptions::WithJitter(Config_->HeartbeatInterval)))
{
    auto selfEntry = New<TProxyEntry>();
    selfEntry->Endpoint = Config_->PublicFqdn
        ? *Config_->PublicFqdn
        : Format("%v:%v", NNet::GetLocalHostName(), config->Port);
    selfEntry->Role = config->Role;
    Self_ = New<TCoordinatorProxy>(std::move(selfEntry));

    {
        TCypressRegistrarOptions options;
        options.RootPath = NApi::HttpProxiesPath + "/" + ToYPathLiteral(Self_->Entry->Endpoint);
        options.OrchidRemoteAddresses = NServer::GetLocalAddresses(
            {{"default", Self_->Entry->GetHost()}},
            Bootstrap_->GetConfig()->RpcPort);
        options.CreateAliveChild = true;
        options.AttributesOnCreation = BuildAttributeDictionaryFluently()
            .Item("role").Value(Self_->Entry->Role)
            .Item("banned").Value(false)
            .Item("liveness").Value(Self_->Entry->Liveness)
            .Finish();

        auto proxyConfig = Bootstrap_->GetConfig();

        auto proxyAddressMap = TProxyAddressMap{
            {EAddressType::Http, NRpcProxy::GetLocalAddresses(proxyConfig->Addresses, proxyConfig->Port)},
            {EAddressType::InternalRpc, NRpcProxy::GetLocalAddresses(proxyConfig->Addresses, proxyConfig->RpcPort)},
            {EAddressType::MonitoringHttp, NRpcProxy::GetLocalAddresses(proxyConfig->Addresses, proxyConfig->MonitoringPort)},
        };

        if (proxyConfig->HttpsServer) {
            auto addresses = NRpcProxy::GetLocalAddresses(proxyConfig->Addresses, proxyConfig->HttpsServer->Port);
            proxyAddressMap.emplace(EAddressType::Https, std::move(addresses));
        }

        if (proxyConfig->TvmOnlyAuth) {
            if (proxyConfig->TvmOnlyHttpServer) {
                auto addresses = NRpcProxy::GetLocalAddresses(proxyConfig->Addresses, proxyConfig->TvmOnlyHttpServer->Port);
                proxyAddressMap.emplace(EAddressType::TvmOnlyHttp, std::move(addresses));
            }

            if (proxyConfig->TvmOnlyHttpsServer) {
                auto addresses = NRpcProxy::GetLocalAddresses(proxyConfig->Addresses, proxyConfig->TvmOnlyHttpsServer->Port);
                proxyAddressMap.emplace(EAddressType::TvmOnlyHttps, std::move(addresses));
            }
        }

        options.AttributesOnStart = BuildAttributeDictionaryFluently()
            .Item("version").Value(NYT::GetVersion())
            .Item("start_time").Value(TInstant::Now().ToString())
            .Item("annotations").Value(Bootstrap_->GetConfig()->CypressAnnotations)
            .Item("addresses").Value(std::move(proxyAddressMap))
            .Finish();
        options.NodeType = EObjectType::ClusterProxyNode;
        CypressRegistrar_ = CreateCypressRegistrar(
            std::move(options),
            Config_->CypressRegistrar,
            Client_,
            bootstrap->GetControlInvoker());
    }
}

void TCoordinator::Start()
{
    AvailableAt_.Store(TInstant::Now());
    UpdateStateExecutor_->Start();
    UpdateStateExecutor_->ScheduleOutOfBand();

    auto result = WaitFor(FirstUpdateIterationFinished_.ToFuture());
    YT_LOG_INFO(result, "Initial coordination iteration finished");
}

bool TCoordinator::IsBanned() const
{
    auto guard = Guard(SelfLock_);
    return Self_->Entry->IsBanned;
}

bool TCoordinator::CanHandleHeavyRequests() const
{
    auto guard = Guard(SelfLock_);
    return Self_->Entry->Role != "control";
}

std::vector<TCoordinatorProxyPtr> TCoordinator::ListProxies(std::optional<std::string> roleFilter, bool includeDeadAndBanned)
{
    std::vector<TCoordinatorProxyPtr> proxies;
    {
        auto guard = Guard(ProxiesLock_);
        proxies = Proxies_;
    }

    auto now = TInstant::Now();
    std::vector<TCoordinatorProxyPtr> filtered;
    for (const auto& proxy : proxies) {
        if (roleFilter && proxy->Entry->Role != roleFilter) {
            continue;
        }

        if (includeDeadAndBanned) {
            filtered.push_back(proxy);
            continue;
        }

        if (!proxy->Entry->IsBanned && !IsDead(proxy->Entry, now)) {
            filtered.push_back(proxy);
        }
    }

    auto dynamicConfig = Bootstrap_->GetDynamicConfig();
    TString fitnessFunction = dynamicConfig->FitnessFunction;

    std::vector<std::pair<double, TCoordinatorProxyPtr>> ordered;
    for (const auto& proxy : filtered) {
        auto liveness = proxy->Entry->Liveness;

        double fitness = 0.0;
        if (fitnessFunction == "cpu") {
            fitness = (liveness->UserCpu + liveness->SystemCpu) * dynamicConfig->CpuWeight
                + liveness->CpuWait * dynamicConfig->CpuWaitWeight
                + liveness->ConcurrentRequests * dynamicConfig->ConcurrentRequestsWeight;
        } else {
            auto adjustedNetworkLoad = std::pow(1.5, liveness->NetworkCoef);
            fitness = liveness->LoadAverage * Config_->LoadAverageWeight
                + adjustedNetworkLoad * Config_->NetworkLoadWeight
                + proxy->Dampening * Config_->DampeningWeight
                + RandomNumber<double>() * Config_->RandomnessWeight;
        }

        ordered.emplace_back(fitness, proxy);
    }

    std::sort(ordered.begin(), ordered.end());
    Shuffle(ordered.begin(), ordered.begin() + ordered.size() / 2);

    filtered.clear();
    for (const auto& proxy : ordered) {
        filtered.push_back(proxy.second);
    }

    return filtered;
}

std::vector<TProxyEntryPtr> TCoordinator::LisTProxyEntries(std::optional<std::string> roleFilter, bool includeDeadAndBanned)
{
    std::vector<TProxyEntryPtr> result;
    auto proxies = ListProxies(roleFilter, includeDeadAndBanned);
    for (const auto& proxy : proxies) {
        result.push_back(proxy->Entry);
    }
    return result;
}

TProxyEntryPtr TCoordinator::AllocateProxy(const std::string& role)
{
    auto proxies = ListProxies(role);

    if (proxies.empty()) {
        return nullptr;
    }

    proxies[0]->Dampening++;
    return proxies[0]->Entry;
}

TProxyEntryPtr TCoordinator::GetSelf()
{
    auto guard = Guard(SelfLock_);
    return Self_->Entry;
}

const TCoordinatorConfigPtr& TCoordinator::GetConfig() const
{
    return Config_;
}

TSamplerPtr TCoordinator::GetTraceSampler()
{
    return Sampler_;
}

std::vector<TCoordinatorProxyPtr> TCoordinator::ListCypressProxies()
{
    TListNodeOptions options;
    options.Timeout = Config_->CypressTimeout;
    options.SuppressTransactionCoordinatorSync = true;
    options.SuppressUpstreamSync = true;
    options.ReadFrom = EMasterChannelKind::Cache;
    options.Attributes = {"role", "banned", "liveness", BanMessageAttributeName};

    auto proxiesYson = WaitFor(Client_->ListNode(NApi::HttpProxiesPath, options))
        .ValueOrThrow();
    auto proxiesList = ConvertTo<IListNodePtr>(proxiesYson);
    std::vector<TCoordinatorProxyPtr> proxies;
    for (const auto& proxyNode : proxiesList->GetChildren()) {
        try {
            auto proxy = ConvertTo<TProxyEntryPtr>(proxyNode->Attributes());
            proxy->Endpoint = proxyNode->GetValue<TString>();
            proxies.push_back(New<TCoordinatorProxy>(std::move(proxy)));
        } catch (std::exception& ex) {
            YT_LOG_WARNING(ex, "Broken proxy node found in Cypress (ProxyNode: %v)",
                ConvertToYsonString(proxyNode));
        }
    }

    return proxies;
}

void TCoordinator::UpdateReadOnly()
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    options.Timeout = Config_->OrchidTimeout;

    auto instances = TComponentDiscoverer::GetCypressPaths(Client_, options, EClusterComponentType::PrimaryMaster);
    std::vector<TFuture<TYsonString>> futures;
    for (const auto& instance : instances) {
        futures.push_back(Client_->GetNode(instance + "/orchid/monitoring/hydra", options));
    }

    std::optional<bool> readOnly;
    for (const auto& future : futures) {
        auto ysonOrError = WaitFor(future);
        if (!ysonOrError.IsOK()) {
            continue;
        }

        auto rspMap = ConvertToNode(ysonOrError.Value())->AsMap();

        if (auto errorNode = rspMap->FindChild("error")) {
            continue;
        }

        if (!rspMap->GetChildValueOrThrow<bool>("active")) {
            continue;
        }

        auto peerReadOnly = rspMap->GetChildValueOrThrow<bool>("read_only");
        readOnly = readOnly ? (*readOnly || peerReadOnly) : peerReadOnly;
    }

    MastersInReadOnly_ = readOnly.value_or(true);
}

void TCoordinator::UpdateState()
{
    YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    try {
        UpdateReadOnly();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error updating read-only");
    }

    auto selfPath = NApi::HttpProxiesPath + "/" + ToYPathLiteral(Self_->Entry->Endpoint);

    auto proxyEntry = CloneYsonStruct(Self_->Entry);
    proxyEntry->Liveness = GetSelfLiveness();
    {
        auto guard = Guard(SelfLock_);
        Self_ = New<TCoordinatorProxy>(std::move(proxyEntry));
    }

    auto onUpdateSuccess = [&] {
        YT_LOG_DEBUG("Coordinator update succeeded");
        BannedGauge_.Update(Self_->Entry->IsBanned ? 1 : 0);
        AvailableAt_.Store(TInstant::Now());
        FirstUpdateIterationFinished_.TrySet();
    };

    auto onUpdateFailure = [&] (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Coordinator update failed");
        FirstUpdateIterationFinished_.TrySet(TError(ex));
    };

    try {
        if (!Config_->Enable) {
            YT_LOG_INFO("Coordinator is disabled");
            FirstUpdateIterationFinished_.TrySet();
            return;
        }

        if (Config_->Announce) {
            auto attributes = BuildAttributeDictionaryFluently()
                .Item("liveness").Value(Self_->Entry->Liveness)
                .Finish();
            auto error = WaitFor(CypressRegistrar_->UpdateNodes(attributes));
            if (!error.IsOK()) {
                YT_LOG_INFO(error, "Error updating proxy liveness");
            }
        }

        auto proxies = ListCypressProxies();
        {
            auto guard = Guard(ProxiesLock_);
            Proxies_ = proxies;
        }

        for (auto& proxy : proxies) {
            if (proxy->Entry->Endpoint != Self_->Entry->Endpoint) {
                continue;
            }

            if (proxy->Entry->IsBanned != Self_->Entry->IsBanned) {
                YT_LOG_INFO("Updating self banned attribute (Old: %v, New: %v)",
                    Self_->Entry->IsBanned,
                    proxy->Entry->IsBanned);
            }

            if (proxy->Entry->Role != Self_->Entry->Role) {
                YT_LOG_INFO("Updating self role attribute (Old: %v, New: %v)",
                    Self_->Entry->Role,
                    proxy->Entry->Role);
                OnSelfRoleChanged_.Fire(proxy->Entry->Role);
            }

            {
                auto guard = Guard(SelfLock_);
                Self_ = proxy;
            }
        }

        onUpdateSuccess();
    } catch (const TErrorException& ex) {
        if (ex.Error().FindMatching(NHydra::EErrorCode::ReadOnly)) {
            YT_LOG_INFO("Master is in read-only mode");
            onUpdateSuccess();
        } else {
            onUpdateFailure(ex);
        }
    } catch (const std::exception& ex) {
        onUpdateFailure(ex);
    }
}

bool TCoordinator::IsDead(const TProxyEntryPtr& proxy, TInstant at) const
{
    if (!proxy->Liveness) {
        return true;
    }

    return proxy->Liveness->UpdatedAt + GetDeathAge() < at;
}

bool TCoordinator::IsUnavailable(TInstant at) const
{
    {
        auto guard = Guard(SelfLock_);
        if (!Self_->Entry->Liveness) {
            return true;
        }
    }

    return IsBanned() || AvailableAt_.Load() + GetDeathAge() < at;
}

TLivenessPtr TCoordinator::GetSelfLiveness()
{
    auto liveness = New<TLiveness>();

    liveness->UpdatedAt = TInstant::Now();

    double loadAverage;
    NSystemInfo::LoadAverage(&loadAverage, 1);
    liveness->LoadAverage = loadAverage;

    liveness->UserCpu = NProfiling::TResourceTracker::GetUserCpu();
    liveness->SystemCpu = NProfiling::TResourceTracker::GetSystemCpu();
    liveness->CpuWait = NProfiling::TResourceTracker::GetCpuWait();

    liveness->ConcurrentRequests = Bootstrap_->GetApi()->GetNumberOfConcurrentRequests();

    auto networkStatistics = GetNetworkStatistics();

    if (!StatisticsUpdatedAt_ || !LastStatistics_ || !networkStatistics) {
        // Take conservative approach.
        liveness->NetworkCoef = 1.0;
    } else {
        auto deltaTx = networkStatistics->TotalTxBytes - LastStatistics_->TotalTxBytes;
        auto deltaRx = networkStatistics->TotalRxBytes - LastStatistics_->TotalRxBytes;
        auto deltaTime = std::max(1e-6, (liveness->UpdatedAt - StatisticsUpdatedAt_).MicroSeconds() / 1e-6);

        auto tenGigabits = 10_GB / 8;

        auto txLoad = deltaTx / tenGigabits / deltaTime;
        auto rxLoad = deltaRx / tenGigabits / deltaTime;
        liveness->NetworkCoef = ClampVal(std::max(txLoad, rxLoad), 0.0, 1.0);
    }

    StatisticsUpdatedAt_ = liveness->UpdatedAt;
    LastStatistics_ = networkStatistics;

    return liveness;
}

TDuration TCoordinator::GetDeathAge() const
{
    if (MastersInReadOnly_) {
        return Config_->ReadOnlyDeathAge;
    }

    return Config_->DeathAge;
}

////////////////////////////////////////////////////////////////////////////////

THostsHandler::THostsHandler(TCoordinatorPtr coordinator)
    : Coordinator_(std::move(coordinator))
{ }

void THostsHandler::HandleRequest(
    const NHttp::IRequestPtr& req,
    const NHttp::IResponseWriterPtr& rsp)
{
    auto role = Coordinator_->GetConfig()->DefaultRoleFilter;
    std::optional<std::string> suffix;
    bool returnJson = true;

    {
        auto path = req->GetUrl().Path;
        TCgiParameters query(req->GetUrl().RawQuery);

        auto roleIt = query.find("role");
        if (roleIt != query.end()) {
            role = roleIt->second;
        }

        if (path != "/hosts" && path != "/hosts/") {
            YT_VERIFY(path.StartsWith("/hosts/"));
            suffix = TString(path.substr(7));
        }

        if (auto header = req->GetHeaders()->Find("Accept")) {
            returnJson = !(*header == "text/plain");
        }
    }

    rsp->SetStatus(EStatusCode::OK);
    if (suffix && *suffix == "all") {
        auto proxies = Coordinator_->LisTProxyEntries({}, true);
        ReplyJson(rsp, [&] (NYson::IYsonConsumer* json) {
            BuildYsonFluently(json)
                .DoListFor(proxies, [&] (auto item, const TProxyEntryPtr& proxy) {
                    item.Item()
                        .BeginMap()
                            .Item("host").Value(proxy->GetHost())
                            .Item("name").Value(proxy->Endpoint)
                            .Item("role").Value(proxy->Role)
                            .Item("banned").Value(proxy->IsBanned)
                            .Item(BanMessageAttributeName).Value(proxy->BanMessage)
                            .Item("dead").Value(Coordinator_->IsDead(proxy, TInstant::Now()))
                            .Item("liveness").Value(proxy->Liveness)
                        .EndMap();
            });
        });
    } else {
        auto formatHostname = [&] (const TProxyEntryPtr& proxy) {
            if (Coordinator_->GetConfig()->ShowPorts) {
                return proxy->Endpoint;
            } else if (suffix && suffix->starts_with("fb")) {
                return "fb-" + proxy->GetHost();
            } else {
                return proxy->GetHost();
            }
        };

        auto proxies = Coordinator_->LisTProxyEntries(role);
        if (returnJson) {
            ReplyJson(rsp, [&] (NYson::IYsonConsumer* json) {
                BuildYsonFluently(json)
                    .DoListFor(proxies, [&] (auto item, const TProxyEntryPtr& proxy) {
                        item.Item().Value(formatHostname(proxy));
                    });
            });
        } else {
            auto output = CreateBufferedSyncAdapter(rsp);
            for (const auto& proxy : proxies) {
                output->Write(formatHostname(proxy));
                output->Write('\n');
            }
            output->Finish();
            WaitFor(rsp->Close())
                .ThrowOnError();
        }
    }
}


////////////////////////////////////////////////////////////////////////////////

TClusterConnectionHandler::TClusterConnectionHandler(NApi::IClientPtr client)
    : Client_(std::move(client))
{ }

void TClusterConnectionHandler::HandleRequest(
    const NHttp::IRequestPtr& req,
    const NHttp::IResponseWriterPtr& rsp)
{
    const auto& path = req->GetUrl().Path;
    if (path != "/cluster_connection" && path != "/cluster_connection/") {
        rsp->SetStatus(EStatusCode::NotFound);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    auto nodeYson = WaitFor(Client_->GetNode("//sys/@cluster_connection", options))
        .ValueOrThrow();

    rsp->SetStatus(EStatusCode::OK);
    ReplyJson(rsp, [&] (IYsonConsumer* consumer) {
        consumer->OnRaw(nodeYson);
    });
}

////////////////////////////////////////////////////////////////////////////////

TPingHandler::TPingHandler(TCoordinatorPtr coordinator)
    : Coordinator_(std::move(coordinator))
{ }

void TPingHandler::HandleRequest(
    const NHttp::IRequestPtr& /*req*/,
    const NHttp::IResponseWriterPtr& rsp)
{
    rsp->SetStatus(Coordinator_->IsUnavailable(TInstant::Now())
        ? EStatusCode::ServiceUnavailable
        : EStatusCode::OK);

    WaitFor(rsp->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

struct TVersionCounter {
    int Total = 0;
    int Banned = 0;
    int Offline = 0;
};

void Serialize(const TVersionCounter& counter, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Value(counter.Total)
            .Item("banned").Value(counter.Banned)
            .Item("offline").Value(counter.Offline)
        .EndMap();
}

TDiscoverVersionsHandler::TDiscoverVersionsHandler(NApi::IClientPtr client, TComponentDiscoveryOptions componentDiscoveryOptions)
    : TComponentDiscoverer(
        std::move(client),
        TMasterReadOptions{.ReadFrom = EMasterChannelKind::Follower},
        std::move(componentDiscoveryOptions))
{ }

void TDiscoverVersionsHandler::HandleRequest(
    const NHttp::IRequestPtr& /*req*/,
    const NHttp::IResponseWriterPtr& rsp)
{
    auto instances = GetAllInstances();

    THashMap<std::string, THashMap<EClusterComponentType, TVersionCounter>> summary;
    for (const auto& instance : instances) {
        auto count = [&] (const std::string& key) {
            summary[key][instance.Type].Total++;

            if (instance.Banned) {
                summary[key][instance.Type].Banned++;
            }

            if (!instance.Online) {
                summary[key][instance.Type].Offline++;
            }
        };

        count("total");
        if (instance.Error.IsOK()) {
            count(instance.Version);
        } else {
            count("error");
        }
    }

    rsp->SetStatus(EStatusCode::OK);
    ReplyJson(rsp, [&] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("summary").Value(summary)
                .Item("details").Value(instances)
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
