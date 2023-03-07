#include "coordinator.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"
#include "api.h"

#include <yt/server/lib/misc/address_helpers.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/client/api/client.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/http/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/net/local_address.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/resource_tracker.h>

#include <yt/core/ytree/ypath_proxy.h>

#include <yt/build/build.h>

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

TCoordinator::TCoordinator(
    const TProxyConfigPtr& config,
    TBootstrap* bootstrap)
    : Config_(config->Coordinator)
    , Bootstrap_(bootstrap)
    , Client_(bootstrap->GetRootClient())
    , UpdateStateExecutor_(New<TPeriodicExecutor>(
        bootstrap->GetControlInvoker(),
        BIND(&TCoordinator::UpdateState, MakeWeak(this)),
        TPeriodicExecutorOptions::WithJitter(Config_->HeartbeatInterval)))
    , UpdateDynamicConfigExecutor_(New<TPeriodicExecutor>(
        bootstrap->GetControlInvoker(),
        BIND(&TCoordinator::UpdateDynamicConfig, MakeWeak(this)),
        TPeriodicExecutorOptions::WithJitter(Config_->HeartbeatInterval)))
{
    Self_ = New<TProxyEntry>();
    Self_->Endpoint = Config_->PublicFqdn
        ? *Config_->PublicFqdn
        : Format("%v:%v", NNet::GetLocalHostName(), config->Port);
    Self_->Role = "data";

    auto dynamicConfig = New<TDynamicConfig>();
    dynamicConfig->SetDefaults();
    SetDynamicConfig(dynamicConfig);
}

void TCoordinator::Start()
{
    UpdateStateExecutor_->Start();
    UpdateStateExecutor_->ScheduleOutOfBand();

    UpdateDynamicConfigExecutor_->Start();

    auto result = WaitFor(FirstUpdateIterationFinished_.ToFuture());
    YT_LOG_INFO(result, "Initial coordination iteration finished");
}

bool TCoordinator::IsBanned() const
{
    auto guard = Guard(Lock_);
    return Self_->IsBanned;
}

bool TCoordinator::CanHandleHeavyRequests() const
{
    auto guard = Guard(Lock_);
    return Self_->Role != "control";
}

std::vector<TProxyEntryPtr> TCoordinator::ListProxies(std::optional<TString> roleFilter, bool includeDeadAndBanned)
{
    std::vector<TProxyEntryPtr> proxies;
    {
        auto guard = Guard(Lock_);
        proxies = Proxies_;
    }

    auto now = TInstant::Now();
    std::vector<TProxyEntryPtr> filtered;
    for (const auto& proxy : proxies) {
        if (roleFilter && proxy->Role != roleFilter) {
            continue;
        }

        if (includeDeadAndBanned) {
            filtered.push_back(proxy);
            continue;
        }

        if(!proxy->IsBanned && !IsDead(proxy, now)) {
            filtered.push_back(proxy);
        }
    }

    auto dynamicConfig = GetDynamicConfig();
    TString fitnessFunction = dynamicConfig->FitnessFunction;

    std::vector<std::pair<double, TProxyEntryPtr>> ordered;
    for (const auto& proxy : filtered) {
        auto liveness = proxy->Liveness;

        double fitness = 0.0;
        if (fitnessFunction == "cpu") {
            fitness = (liveness->UserCpu + liveness->SystemCpu) * dynamicConfig->CpuWeight
                + liveness->CpuWait * dynamicConfig->CpuWaitWeight
                + liveness->ConcurrentRequests * dynamicConfig->ConcurrentRequestsWeight;
        } else {
            auto adjustedNetworkLoad = std::pow(1.5, liveness->NetworkCoef);
            fitness = liveness->LoadAverage * Config_->LoadAverageWeight
                + adjustedNetworkLoad * Config_->NetworkLoadWeight
                + liveness->Dampening * Config_->DampeningWeight
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

TProxyEntryPtr TCoordinator::AllocateProxy(const TString& role)
{
    auto proxies = ListProxies(role);

    if (proxies.empty()) {
        return nullptr;
    }

    proxies[0]->Liveness->Dampening++;
    return proxies[0];
}

TProxyEntryPtr TCoordinator::GetSelf()
{
    auto guard = Guard(Lock_);
    return Self_;
}

const TCoordinatorConfigPtr& TCoordinator::GetConfig() const
{
    return Config_;
}

TDynamicConfigPtr TCoordinator::GetDynamicConfig()
{
    auto guard = Guard(Lock_);
    return DynamicConfig_;
}

TSampler* TCoordinator::GetTraceSampler()
{
    return &Sampler_;
}

std::vector<TProxyEntryPtr> TCoordinator::ListCypressProxies()
{
    TListNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    options.Attributes = {"role", "banned", "liveness", BanMessageAttributeName};

    auto proxiesYson = WaitFor(Client_->ListNode(SysProxies, options))
        .ValueOrThrow();
    auto proxiesList = ConvertTo<IListNodePtr>(proxiesYson);
    std::vector<TProxyEntryPtr> proxies;
    for (const auto& proxyNode : proxiesList->GetChildren()) {
        try {
            auto proxy = ConvertTo<TProxyEntryPtr>(proxyNode->Attributes());
            proxy->Endpoint = proxyNode->GetValue<TString>();
            proxies.emplace_back(std::move(proxy));
        } catch (std::exception& ex) {
            YT_LOG_WARNING(ex, "Broken proxy node found in Cypress (ProxyNode: %v)",
                ConvertToYsonString(proxyNode));
        }
    }

    return proxies;
}

void TCoordinator::UpdateDynamicConfig()
{
    try {
        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::Cache;

        auto configYsonOrError = WaitFor(Client_->GetNode(SysProxies + "/@config", options));
        if (configYsonOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            YT_LOG_INFO("Dynamic config is missing");
            return;
        }

        auto config = ConvertTo<TDynamicConfigPtr>(configYsonOrError.ValueOrThrow());
        SetDynamicConfig(config);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error loading dynamic config");
    }
}

void TCoordinator::SetDynamicConfig(TDynamicConfigPtr config)
{
    auto guard = Guard(Lock_);
    std::swap(config, DynamicConfig_);

    if (DynamicConfig_->Tracing) {
        Sampler_.UpdateConfig(DynamicConfig_->Tracing);
        Sampler_.ResetPerUserLimits();
    }
}

IYPathServicePtr TCoordinator::CreateOrchidService()
{
   return IYPathService::FromProducer(BIND(&TCoordinator::BuildOrchid, MakeStrong(this)));
}

void TCoordinator::BuildOrchid(IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("dynamic_config").Value(GetDynamicConfig())
        .EndMap();
}

void TCoordinator::UpdateState()
{
    auto selfPath = SysProxies + "/" + ToYPathLiteral(Self_->Endpoint);

    {
        auto guard = Guard(Lock_);
        Self_->Liveness = GetSelfLiveness();
    }

    try {
        if (Config_->Enable && !Initialized_ && Config_->Announce) {
            TCreateNodeOptions options;
            options.Recursive = true;
            options.Attributes = ConvertToAttributes(BuildYsonStringFluently()
                .BeginMap()
                    .Item("role").Value(Self_->Role)
                    .Item("banned").Value(false)
                    .Item("liveness").Value(Self_->Liveness)
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
                options.Recursive = true;
                options.IgnoreExisting = true;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("remote_addresses",
                    NYT::GetLocalAddresses({{"default", Self_->GetHost()}}, Bootstrap_->GetConfig()->RpcPort));
                options.Attributes = std::move(attributes);
                auto orchidPath = selfPath + "/orchid";
                WaitFor(Client_->CreateNode(orchidPath, EObjectType::Orchid, options))
                    .ThrowOnError();
                YT_LOG_INFO("Orchid node created (Path: %v)", orchidPath);
            }

            WaitFor(Client_->SetNode(selfPath + "/@version", ConvertToYsonString(NYT::GetVersion())))
                .ThrowOnError();
            WaitFor(Client_->SetNode(selfPath + "/@start_time", ConvertToYsonString(TInstant::Now().ToString())))
                .ThrowOnError();

            auto annotations = ConvertToYsonString(Bootstrap_->GetConfig()->CypressAnnotations);
            WaitFor(Client_->SetNode(selfPath + "/@annotations", annotations))
                .ThrowOnError();

            Initialized_ = true;
        }

        if (!Config_->Enable) {
            YT_LOG_INFO("Coordinator is disabled");
            FirstUpdateIterationFinished_.TrySet();
            return;
        }

        if (Config_->Announce) {
            WaitFor(Client_->SetNode(selfPath + "/@liveness", ConvertToYsonString(Self_->Liveness)))
                .ThrowOnError();
        }

        auto proxies = ListCypressProxies();

        {
            auto guard = Guard(Lock_);
            Proxies_ = proxies;
            for (auto& proxy : proxies) {
                if (proxy->Endpoint != Self_->Endpoint) {
                    continue;
                }

                if (proxy->IsBanned != Self_->IsBanned) {
                    YT_LOG_INFO("Updating self banned attribute (Old: %v, New: %v)",
                        Self_->IsBanned,
                        proxy->IsBanned);
                }

                if (proxy->Role != Self_->Role) {
                    YT_LOG_INFO("Updating self role attribute (Old: %v, New: %v)",
                        Self_->Role,
                        proxy->Role);
                    OnSelfRoleChanged_.Fire(proxy->Role);
                }

                Self_ = proxy;
            }
        }

        HttpProxyProfiler.Enqueue("/banned", Self_->IsBanned ? 1 : 0, EMetricType::Gauge);

        FirstUpdateIterationFinished_.TrySet();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Coordinator update failed");
        FirstUpdateIterationFinished_.TrySet(TError(ex));
    }
}

bool TCoordinator::IsDead(const TProxyEntryPtr& proxy, TInstant at) const
{
    if (!proxy->Liveness) {
        return true;
    }

    return proxy->Liveness->UpdatedAt + Config_->DeathAge < at;
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
        auto proxies = Coordinator_->ListProxies({}, true);
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

        auto proxies = Coordinator_->ListProxies(role);
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

    rsp->SetStatus(Coordinator_->IsBanned() ? EStatusCode::ServiceUnavailable : EStatusCode::OK);
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

        if (version && startTime) {
            instance.Version = *version;
            instance.StartTime = *startTime;
            instance.Banned = banned ? *banned : false;
        } else {
            instance.Error = TError("Cannot find all attribute in response")
                << TErrorAttribute("version", version)
                << TErrorAttribute("start_time", startTime);
        }

        if (type == "node") {
            if (nodeState) {
                instance.State = *nodeState;
            }
            instance.Online = (nodeState == TString("online"));
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

std::vector<TString> TDiscoverVersionsHandler::GetInstances(const TString& path, bool fromSubdirectories)
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
    const TString& path,
    const std::vector<TString>& instances,
    const TString& type)
{
    const auto OrchidTimeout = TDuration::Seconds(1);

    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.Timeout = OrchidTimeout;

    std::vector<TFuture<TYsonString>> responses;
    for (const auto& instance : instances) {
        responses.push_back(Client_->GetNode(path + "/" + instance + "/orchid/service"));
    }

    std::vector<TInstance> results;
    for (size_t i = 0; i < instances.size(); ++i) {
        auto ysonOrError = WaitFor(responses[i]);

        TInstance result;
        result.Type = type;

        TVector<TString> parts;
        Split(instances[i], "/", parts);
        result.Address = parts.back();
        if (ysonOrError.IsOK()) {
            auto rspMap = ConvertToNode(ysonOrError.Value())->AsMap();
            auto version = ConvertTo<TString>(rspMap->GetChild("version"));
            auto startTime = ConvertTo<TString>(rspMap->GetChild("start_time"));

            result.Version = version;
            result.StartTime = startTime;
        } else {
            result.Error = ysonOrError;
        }

        results.push_back(result);
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
                .Item("nodes").Value(FormatInstances(ListComponent("nodes", "node")))
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
    add(ListComponent("nodes", "node"));
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
