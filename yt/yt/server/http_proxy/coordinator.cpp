#include "coordinator.h"

#include "api.h"
#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/profiling/profile_manager.h>
#include <yt/yt/core/profiling/resource_tracker.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/build/build.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <util/string/split.h>

#include <util/generic/vector.h>

#include <util/random/shuffle.h>

#include <util/system/info.h>

namespace NYT::NHttpProxy {

static const auto& Logger = HttpProxyLogger;

static const TString SysProxies = "//sys/proxies";

using namespace NApi;
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

////////////////////////////////////////////////////////////////////////////////

TLiveness::TLiveness()
{
    RegisterParameter("updated_at", UpdatedAt)
        .Default();
    RegisterParameter("load_average", LoadAverage)
        .Default();
    RegisterParameter("network_coef", NetworkCoef)
        .Default();
    RegisterParameter("user_cpu", UserCpu)
        .Default();
    RegisterParameter("system_cpu", SystemCpu)
        .Default();
    RegisterParameter("cpu_wait", CpuWait)
        .Default();
    RegisterParameter("concurrent_requests", ConcurrentRequests)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TProxyEntry::TProxyEntry()
{
    RegisterParameter("role", Role);
    RegisterParameter("endpoint", Endpoint)
        .Optional();
    RegisterParameter("banned", IsBanned)
        .Default(false);
    RegisterParameter("liveness", Liveness)
        .DefaultNew();
    RegisterParameter(BanMessageAttributeName, BanMessage)
        .Default();
}

TString TProxyEntry::GetHost() const
{
    return TString{NNet::GetServiceHostName(Endpoint)};
}

////////////////////////////////////////////////////////////////////////////////

TCoordinatorProxy::TCoordinatorProxy(const TProxyEntryPtr& proxyEntry)
    : Entry(proxyEntry)
{ }

////////////////////////////////////////////////////////////////////////////////

TCoordinator::TCoordinator(
    const TProxyConfigPtr& config,
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
    selfEntry->Role = "data";
    Self_ = New<TCoordinatorProxy>(std::move(selfEntry));
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

std::vector<TCoordinatorProxyPtr> TCoordinator::ListProxies(std::optional<TString> roleFilter, bool includeDeadAndBanned)
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

        if(!proxy->Entry->IsBanned && !IsDead(proxy->Entry, now)) {
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

std::vector<TProxyEntryPtr> TCoordinator::ListProxyEntries(std::optional<TString> roleFilter, bool includeDeadAndBanned)
{
    std::vector<TProxyEntryPtr> result;
    auto proxies = ListProxies(roleFilter, includeDeadAndBanned);
    for (const auto& proxy : proxies) {
        result.push_back(proxy->Entry);
    }
    return std::move(result);
}

TProxyEntryPtr TCoordinator::AllocateProxy(const TString& role)
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
    options.ReadFrom = EMasterChannelKind::Cache;
    options.Attributes = {"role", "banned", "liveness", BanMessageAttributeName};

    auto proxiesYson = WaitFor(Client_->ListNode(SysProxies, options))
        .ValueOrThrow();
    auto proxiesList = ConvertTo<IListNodePtr>(proxiesYson);
    std::vector<TCoordinatorProxyPtr> proxies;
    for (const auto& proxyNode : proxiesList->GetChildren()) {
        try {
            auto proxy = ConvertTo<TProxyEntryPtr>(proxyNode->Attributes());
            proxy->Endpoint = proxyNode->GetValue<TString>();
            proxies.emplace_back(New<TCoordinatorProxy>(std::move(proxy)));
        } catch (std::exception& ex) {
            YT_LOG_WARNING(ex, "Broken proxy node found in Cypress (ProxyNode: %v)",
                ConvertToYsonString(proxyNode));
        }
    }

    return proxies;
}

void TCoordinator::UpdateState()
{
    auto selfPath = SysProxies + "/" + ToYPathLiteral(Self_->Entry->Endpoint);

    auto proxyEntry = CloneYsonSerializable(Self_->Entry);
    proxyEntry->Liveness = GetSelfLiveness();
    {
        auto guard = Guard(SelfLock_);
        Self_ = New<TCoordinatorProxy>(std::move(proxyEntry));
    }

    auto onUpdateSuccess = [&] {
        BannedGauge_.Update(Self_->Entry->IsBanned ? 1 : 0);
        AvailableAt_.Store(TInstant::Now());
        FirstUpdateIterationFinished_.TrySet();
    };

    auto onUpdateFailure = [&] (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Coordinator update failed");
        FirstUpdateIterationFinished_.TrySet(TError(ex));
    };

    try {
        if (Config_->Enable && !Initialized_ && Config_->Announce) {
            TCreateNodeOptions options;
            options.Timeout = Config_->CypressTimeout;
            options.SuppressTransactionCoordinatorSync = true;
            options.Recursive = true;
            options.Attributes = ConvertToAttributes(BuildYsonStringFluently()
                .BeginMap()
                    .Item("role").Value(Self_->Entry->Role)
                    .Item("banned").Value(false)
                    .Item("liveness").Value(Self_->Entry->Liveness)
                .EndMap());

            auto error = WaitFor(Client_->CreateNode(selfPath, EObjectType::MapNode, options));
            if (error.FindMatching(NYTree::EErrorCode::AlreadyExists)) {
                YT_LOG_INFO("Cypress node already exists (Path: %v)", selfPath);
            } else if (error.IsOK()) {
                YT_LOG_INFO("Created Cypress node (Path: %v)", selfPath);
            } else {
                error.ValueOrThrow();
            }

            {
                TCreateNodeOptions options;
                options.Timeout = Config_->CypressTimeout;
                options.SuppressTransactionCoordinatorSync = true;
                options.Recursive = true;
                options.IgnoreExisting = true;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("remote_addresses",
                    NYT::GetLocalAddresses({{"default", Self_->Entry->GetHost()}}, Bootstrap_->GetConfig()->RpcPort));
                options.Attributes = std::move(attributes);
                auto orchidPath = selfPath + "/orchid";
                WaitFor(Client_->CreateNode(orchidPath, EObjectType::Orchid, options))
                    .ThrowOnError();
                YT_LOG_INFO("Orchid node created (Path: %v)", orchidPath);
            }

            TMultisetAttributesNodeOptions multisetOptions;
            multisetOptions.Timeout = Config_->CypressTimeout;
            multisetOptions.SuppressTransactionCoordinatorSync = true;

            WaitFor(Client_->MultisetAttributesNode(
                selfPath + "/@",
                BuildYsonNodeFluently().BeginMap()
                    .Item("version").Value(NYT::GetVersion())
                    .Item("start_time").Value(TInstant::Now().ToString())
                    .Item("annotations").Value(Bootstrap_->GetConfig()->CypressAnnotations)
                .EndMap()->AsMap(),
                multisetOptions))
                .ThrowOnError();

            Initialized_ = true;
        }

        if (!Config_->Enable) {
            YT_LOG_INFO("Coordinator is disabled");
            FirstUpdateIterationFinished_.TrySet();
            return;
        }

        TSetNodeOptions setOptions;
        setOptions.Timeout = Config_->CypressTimeout;
        setOptions.SuppressTransactionCoordinatorSync = true;

        if (Config_->Announce) {
            WaitFor(Client_->SetNode(selfPath + "/@liveness", ConvertToYsonString(Self_->Entry->Liveness), setOptions))
                .ThrowOnError();
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

    return proxy->Liveness->UpdatedAt + Config_->DeathAge < at;
}

bool TCoordinator::IsUnavailable(TInstant at) const
{
    {
        auto guard = Guard(SelfLock_);
        if (!Self_->Entry->Liveness) {
            return true;
        }
    }

    return IsBanned() || AvailableAt_.Load() + Config_->DeathAge < at;
}

TLivenessPtr TCoordinator::GetSelfLiveness()
{
    auto liveness = New<TLiveness>();

    liveness->UpdatedAt = TInstant::Now();

    double loadAverage;
    NSystemInfo::LoadAverage(&loadAverage, 1);
    liveness->LoadAverage = loadAverage;

    auto resourceTracker = NProfiling::TProfileManager::Get()->GetResourceTracker();
    liveness->UserCpu = resourceTracker->GetUserCpu();
    liveness->SystemCpu = resourceTracker->GetSystemCpu();
    liveness->CpuWait = resourceTracker->GetCpuWait();

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

////////////////////////////////////////////////////////////////////////////////

THostsHandler::THostsHandler(TCoordinatorPtr coordinator)
    : Coordinator_(std::move(coordinator))
{ }

void THostsHandler::HandleRequest(
    const NHttp::IRequestPtr& req,
    const NHttp::IResponseWriterPtr& rsp)
{
    if (MaybeHandleCors(req, rsp)) {
        return;
    }

    auto role = Coordinator_->GetConfig()->DefaultRoleFilter;
    std::optional<TString> suffix;
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
        auto proxies = Coordinator_->ListProxyEntries({}, true);
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
            } else if (suffix && suffix->StartsWith("fb")) {
                return "fb-" + proxy->GetHost();
            } else {
                return proxy->GetHost();
            }
        };

        auto proxies = Coordinator_->ListProxyEntries(role);
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

TPingHandler::TPingHandler(TCoordinatorPtr coordinator)
    : Coordinator_(std::move(coordinator))
{ }

void TPingHandler::HandleRequest(
    const NHttp::IRequestPtr& req,
    const NHttp::IResponseWriterPtr& rsp)
{
    if (MaybeHandleCors(req, rsp)) {
        return;
    }

    rsp->SetStatus(Coordinator_->IsUnavailable(TInstant::Now())
        ? EStatusCode::ServiceUnavailable
        : EStatusCode::OK);
    WaitFor(rsp->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TDiscoverVersionsHandler::TDiscoverVersionsHandler(
    NApi::NNative::IConnectionPtr connection,
    NApi::IClientPtr client,
    const TCoordinatorConfigPtr config)
    : Connection_(std::move(connection))
    , Client_(std::move(client))
    , Config_(config)
{ }

std::vector<TInstance> TDiscoverVersionsHandler::ListComponent(
    const TString& component,
    const TString& type)
{
    TListNodeOptions options;
    if (type == "node") {
        options.Attributes = {
            "register_time",
            "version",
            "banned",
            "state",
        };
    } else {
        options.Attributes = {
            "start_time",
            "version",
            "banned",
        };
    }

    auto rsp = WaitFor(Client_->ListNode("//sys/" + component, options))
        .ValueOrThrow();
    auto rspList = ConvertToNode(rsp)->AsList();

    std::vector<TInstance> instances;
    for (const auto& node : rspList->GetChildren()) {
        auto version = node->Attributes().Find<TString>("version");
        auto banned = node->Attributes().Find<bool>("banned");
        auto nodeState = node->Attributes().Find<TString>("state");
        auto startTime = node->Attributes().Find<TString>(type == "node" ? "register_time" : "start_time");

        TInstance instance;
        instance.Type = type;
        instance.Address = node->GetValue<TString>();

        if (type == "node") {
            if (nodeState) {
                instance.State = *nodeState;
            }
            instance.Online = (nodeState == TString("online"));
        }

        if (version) {
            instance.Version = *version;
        }
        if (startTime) {
            instance.StartTime = *startTime;
        }
        instance.Banned = banned ? *banned : false;

        if (instance.Online && (!version || !startTime)) {
            instance.Error = TError("Component is missing some of the required attributes in response")
                << TErrorAttribute("version", version)
                << TErrorAttribute("start_time", startTime);
        }

        instances.push_back(instance);
    }

    return instances;
}

std::vector<TInstance> TDiscoverVersionsHandler::ListProxies(
    const TString& component,
    const TString& type)
{
    TGetNodeOptions options;
    if (type == "rpc_proxy") {
        options.Attributes = {
            "start_time",
            "version",
            "banned",
        };
    } else {
        options.Attributes = {
            "liveness",
            "start_time",
            "version",
            "banned",
        };
    }
    auto nodeYson = WaitFor(Client_->GetNode("//sys/" + component, options))
        .ValueOrThrow();

    std::vector<TInstance> instances;
    auto timeNow = TInstant::Now();
    for (const auto& [address, node] : ConvertTo<THashMap<TString, IMapNodePtr>>(nodeYson)) {
        auto version = node->Attributes().Find<TString>("version");
        auto banned = node->Attributes().Find<bool>("banned");
        auto startTime = node->Attributes().Find<TString>("start_time");

        TInstance instance;
        instance.Type = type;
        instance.Address = address;

        if (version && startTime) {
            instance.Version = *version;
            instance.StartTime = *startTime;
            instance.Banned = banned.value_or(false);
        } else {
            instance.Error = TError("Cannot find required attributes in response")
                << TErrorAttribute("version", version)
                << TErrorAttribute("start_time", startTime);
        }

        if (type == "rpc_proxy") {
            auto alive = node->AsMap()->FindChild("alive");
            instance.Online = static_cast<bool>(alive);
        } else {
            auto livenessPtr = node->Attributes().Find<TLivenessPtr>("liveness");
            if (!livenessPtr) {
                instance.Error = TError("Liveness attribute is missing");
            } else {
                instance.Online = (livenessPtr->UpdatedAt + Config_->DeathAge >= timeNow);
            }
        }
        if (instance.Online) {
            instance.State = "online";
        } else {
            instance.State = "offline";
        }
        instances.push_back(instance);
    }

    return instances;
}

std::vector<TString> TDiscoverVersionsHandler::GetInstances(const TYPath& path, bool fromSubdirectories)
{
    std::vector<TString> instances;
    if (fromSubdirectories) {
        auto directory = WaitFor(Client_->GetNode(path))
            .ValueOrThrow();
        for (const auto& subdirectory : ConvertToNode(directory)->AsMap()->GetChildren()) {
            for (const auto& instance : subdirectory.second->AsMap()->GetChildren()) {
                instances.push_back(subdirectory.first + "/" + instance.first);
            }
        }
    } else {
        auto rsp = WaitFor(Client_->ListNode(path)).ValueOrThrow();
        auto rspList = ConvertToNode(rsp)->AsList();
        for (const auto& node : rspList->GetChildren()) {
            instances.push_back(node->GetValue<TString>());
        }
    }
    return instances;
}

std::vector<TInstance> TDiscoverVersionsHandler::GetAttributes(
    const TYPath& path,
    const std::vector<TString>& instances,
    const TString& type,
    const TYPath& suffix)
{
    const auto OrchidTimeout = TDuration::Seconds(1);

    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.Timeout = OrchidTimeout;

    std::vector<TFuture<TYsonString>> responses;
    for (const auto& instance : instances) {
        responses.push_back(Client_->GetNode(path + "/" + instance + suffix));
    }

    std::vector<TInstance> results;
    for (size_t i = 0; i < instances.size(); ++i) {
        auto ysonOrError = WaitFor(responses[i]);

        auto& result = results.emplace_back();
        result.Type = type;

        TVector<TString> parts;
        Split(instances[i], "/", parts);
        result.Address = parts.back();
        if (!ysonOrError.IsOK()) {
            result.Error = ysonOrError;
            continue;
        }

        auto rspMap = ConvertToNode(ysonOrError.Value())->AsMap();

        if (auto errorNode = rspMap->FindChild("error")) {
            result.Error = ConvertTo<TError>(errorNode);
            continue;
        }

        auto version = ConvertTo<TString>(rspMap->GetChildOrThrow("version"));
        auto startTime = ConvertTo<TString>(rspMap->GetChildOrThrow("start_time"));

        result.Version = version;
        result.StartTime = startTime;
    }
    return results;
};

////////////////////////////////////////////////////////////////////////////////

TYsonString FormatInstances(const std::vector<TInstance>& instances)
{
    return BuildYsonStringFluently()
        .DoMapFor(instances, [&] (TFluentMap fluent, const TInstance& instance) {
            if (instance.Error.IsOK()) {
                fluent
                    .Item(instance.Address)
                    .BeginMap()
                        .Item("start_time").Value(instance.StartTime)
                        .Item("version").Value(instance.Version)
                    .EndMap();
            } else {
                fluent
                    .Item(instance.Address)
                    .BeginMap()
                        .Item("error").Value(instance.Error)
                    .EndMap();
            }
        });
}

void TDiscoverVersionsHandlerV1::HandleRequest(
    const NHttp::IRequestPtr& req,
    const NHttp::IResponseWriterPtr& rsp)
{
    if (MaybeHandleCors(req, rsp)) {
        return;
    }

    rsp->SetStatus(EStatusCode::OK);

    ReplyJson(rsp, [this] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("primary_masters").Value(FormatInstances(
                    GetAttributes("//sys/primary_masters", GetInstances("//sys/primary_masters"), "primary_master")
                 ))
                .Item("secondary_masters").Value(FormatInstances(
                    GetAttributes("//sys/secondary_masters", GetInstances("//sys/secondary_masters", true), "secondary_master")
                ))
                .Item("schedulers").Value(FormatInstances(
                    GetAttributes("//sys/scheduler/instances", GetInstances("//sys/scheduler/instances"), "scheduler")
                ))
                .Item("controller_agents").Value(FormatInstances(
                    GetAttributes("//sys/controller_agents/instances", GetInstances("//sys/controller_agents/instances"), "controller_agent")
                ))
                .Item("nodes").Value(FormatInstances(ListComponent("cluster_nodes", "node")))
                .Item("http_proxies").Value(FormatInstances(ListProxies("proxies", "http_proxy")))
                .Item("rpc_proxies").Value(FormatInstances(ListProxies("rpc_proxies", "rpc_proxy")))
            .EndMap();
    });
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

void Serialize(const TInstance& instance, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("address").Value(instance.Address)
            .Item("type").Value(instance.Type)
            .DoIf(instance.Error.IsOK(), [&] (auto fluent) {
                fluent
                    .Item("version").Value(instance.Version)
                    .Item("start_time").Value(instance.StartTime)
                    .Item("banned").Value(instance.Banned);

                if (instance.State != "") {
                    fluent.Item("state").Value(instance.State);
                }
            })
            .DoIf(!instance.Error.IsOK(), [&] (auto fluent) {
                fluent.Item("error").Value(instance.Error);
            })
        .EndMap();
}

void TDiscoverVersionsHandlerV2::HandleRequest(
    const NHttp::IRequestPtr& req,
    const NHttp::IResponseWriterPtr& rsp)
{
    if (MaybeHandleCors(req, rsp)) {
        return;
    }

    std::vector<TInstance> instances;
    auto add = [&] (auto part) {
        for (const auto& instance : part) {
            instances.push_back(instance);
        }
    };

    add(GetAttributes("//sys/primary_masters", GetInstances("//sys/primary_masters"), "primary_master"));
    add(GetAttributes("//sys/secondary_masters", GetInstances("//sys/secondary_masters", true), "secondary_master"));
    add(GetAttributes("//sys/scheduler/instances", GetInstances("//sys/scheduler/instances"), "scheduler"));
    add(GetAttributes("//sys/controller_agents/instances", GetInstances("//sys/controller_agents/instances"), "controller_agent"));
    add(ListComponent("cluster_nodes", "node"));
    add(GetAttributes("//sys/cluster_nodes", GetInstances("//sys/cluster_nodes"), "job_proxy", "/orchid/job_controller/job_proxy_build"));
    add(ListProxies("proxies", "http_proxy"));
    add(ListProxies("rpc_proxies", "rpc_proxy"));

    THashMap<TString, THashMap<TString, TVersionCounter>> summary;
    for (const auto& instance : instances) {
        auto count = [&] (const TString& key) {
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
