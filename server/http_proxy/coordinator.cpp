#include "coordinator.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/client/api/client.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/http/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/net/local_address.h>

#include <yt/core/ytree/ypath_proxy.h>

#include <yt/build/build.h>

#include <util/string/cgiparam.h>

#include <util/system/info.h>

namespace NYT {
namespace NHttpProxy {

static auto& Logger = HttpProxyLogger;

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
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
}

////////////////////////////////////////////////////////////////////////////////

TProxyEntry::TProxyEntry()
{
    RegisterParameter("role", Role);
    RegisterParameter("banned", IsBanned)
        .Default(false);
    RegisterParameter("liveness", Liveness)
        .DefaultNew();
    RegisterParameter("ban_message", BanMessage)
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
    , Periodic_(New<TPeriodicExecutor>(
        bootstrap->GetControlInvoker(),
        BIND(&TCoordinator::Update, MakeWeak(this)),
        Config_->HeartbeatInterval))
{
    Self_ = New<TProxyEntry>();
    Self_->Endpoint = Format("%v:%d", NNet::GetLocalHostName(), config->Port);
    Self_->Role = "data";
}

void TCoordinator::Start()
{
    Periodic_->Start();
    Periodic_->ScheduleOutOfBand();

    auto result = WaitFor(FirstUpdateIterationFinished_.ToFuture());
    LOG_INFO(result, "Initial coordination iteration finished");
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

std::vector<TProxyEntryPtr> TCoordinator::ListProxies(TNullable<TString> roleFilter, bool includeDeadAndBanned)
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

    std::vector<std::pair<double, TProxyEntryPtr>> ordered;
    for (const auto& proxy : filtered) {
        auto adjustedNetworkLoad = std::pow(1.5, proxy->Liveness->NetworkCoef);
        auto fitness = proxy->Liveness->LoadAverage * Config_->LoadAverageWeight
            + adjustedNetworkLoad * Config_->NetworkLoadWeight
            + proxy->Liveness->Dampening * Config_->DampeningWeight
            + RandomNumber<double>() * Config_->RandomnessWeight;

        ordered.emplace_back(fitness, proxy);
    }

    std::sort(ordered.begin(), ordered.end());

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

std::vector<TProxyEntryPtr> TCoordinator::ListCypressProxies()
{
    TListNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    options.Attributes = {"role", "banned", "liveness", "ban_message"};

    auto proxiesYson = WaitFor(Client_->ListNode("//sys/proxies", options))
        .ValueOrThrow();
    auto proxiesList = ConvertTo<IListNodePtr>(proxiesYson);
    std::vector<TProxyEntryPtr> proxies;
    for (const auto& proxyNode : proxiesList->GetChildren()) {
        try {
            auto proxy = ConvertTo<TProxyEntryPtr>(proxyNode->Attributes());
            proxy->Endpoint = proxyNode->GetValue<TString>();
            proxies.emplace_back(std::move(proxy));
        } catch (std::exception& ex) {
            LOG_WARNING(ex, "Broken proxy node found in cypress (ProxyNode: %v)", ConvertToYsonString(proxyNode));
        }
    }

    return proxies;
}

void TCoordinator::Update()
{
    auto selfPath = "//sys/proxies/" + Self_->Endpoint;

    {
        auto guard = Guard(Lock_);
        Self_->Liveness = GetSelfLiveness();
    }

    try {
        if (Config_->Enable && !IsInitialized_ && Config_->Announce) {
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
                LOG_INFO("Cypress node already exists (Path: %v)", selfPath);
            } else if (error.IsOK()) {
                LOG_INFO("Created cypress node (Path: %v)", selfPath);
            } else {
                error.ValueOrThrow();
            }

            WaitFor(Client_->SetNode(selfPath + "/@version", ConvertToYsonString(NYT::GetVersion())))
                .ThrowOnError();
            WaitFor(Client_->SetNode(selfPath + "/@start_time", ConvertToYsonString(TInstant::Now().ToString())))
                .ThrowOnError();

            auto annotations = ConvertToYsonString(Bootstrap_->GetConfig()->CypressAnnotations);
            WaitFor(Client_->SetNode(selfPath + "/@annotations", annotations))
                .ThrowOnError();

            IsInitialized_ = true;
        }

        if (!Config_->Enable) {
            LOG_INFO("Coordinator is disabled");
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
                    LOG_INFO("Updating self banned attribute (Old: %v, New: %v)",
                        Self_->IsBanned,
                        proxy->IsBanned);
                }

                if (proxy->Role != Self_->Role) {
                    LOG_INFO("Updating self role attribute (Old: %v, New: %v)",
                        Self_->Role,
                        proxy->Role);
                }

                Self_ = proxy;
            }
        }

        HttpProxyProfiler.Enqueue("/banned", Self_->IsBanned ? 1 : 0, EMetricType::Gauge);

        FirstUpdateIterationFinished_.TrySet();
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Coordinator update failed");
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

    TNullable<TString> role{"data"};
    TNullable<TString> suffix;
    bool returnJson = true;

    {
        auto path = req->GetUrl().Path;
        TCgiParameters query(req->GetUrl().RawQuery);

        auto roleIt = query.find("role");
        if (roleIt != query.end()) {
            role = roleIt->second;
        }

        if (path != "/hosts" && path != "/hosts/") {
            YCHECK(path.StartsWith("/hosts/"));
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
                            .Item("ban_message").Value(proxy->BanMessage)
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
    NApi::IClientPtr client)
    : Connection_(std::move(connection))
    , Client_(std::move(client))
{ }

void TDiscoverVersionsHandler::HandleRequest(
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
                .Item("primary_masters").Value(GetAttributes("//sys/primary_masters", GetInstances("//sys/primary_masters")))
                .Item("secondary_masters").Value(GetAttributes("//sys/secondary_masters", GetInstances("//sys/secondary_masters", true)))
                .Item("schedulers").Value(GetAttributes("//sys/scheduler/instances", GetInstances("//sys/scheduler/instances")))
                .Item("controller_agents").Value(GetAttributes("//sys/controller_agents/instances", GetInstances("//sys/controller_agents/instances")))
                .Item("nodes").Value(ListComponent("nodes", true))
                .Item("http_proxies").Value(ListComponent("proxies", false))
                .Item("rpc_proxies").Value(ListComponent("rpc_proxies", false))
            .EndMap();
    });
}

TYsonString TDiscoverVersionsHandler::ListComponent(const TString& component, bool isDataNode)
{
    TListNodeOptions options;
    if (isDataNode) {
        options.Attributes = {
            "register_time",
            "version",
        };
    } else {
        options.Attributes = {
            "start_time",
            "version",
        };
    }
    auto rsp = WaitFor(Client_->ListNode("//sys/" + component, options))
        .ValueOrThrow();
    auto rspList = ConvertToNode(rsp)->AsList();
    return BuildYsonStringFluently()
        .DoMapFor(rspList->GetChildren(), [isDataNode] (TFluentMap fluent, const INodePtr& node) {
            auto version = node->Attributes().Find<TString>("version");
            auto startTime = node->Attributes().Find<TString>(isDataNode ? "register_time" : "start_time");
            if (version && startTime) {
                fluent
                    .Item(node->GetValue<TString>())
                    .BeginMap()
                        .Item("start_time").Value(startTime)
                        .Item("version").Value(version)
                    .EndMap();
            } else {
                fluent
                    .Item(node->GetValue<TString>())
                    .BeginMap()
                        .Item("error")
                        .Value(TError("Cannot find all attribute in response")
                                    << TErrorAttribute("version", version)
                                    << TErrorAttribute("start_time", startTime))
                    .EndMap();
            }
        });
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

TYsonString TDiscoverVersionsHandler::GetAttributes(
    const TString& path,
    const std::vector<TString>& instances)
{
    auto channel = Connection_->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& instance : instances) {
        auto req = TYPathProxy::Get(path + "/" + instance + "/orchid/service");
        batchReq->AddRequest(req, instance);
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    return BuildYsonStringFluently()
        .DoMapFor(instances, [&] (TFluentMap fluent, const TString& instance) {
            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(instance);
            if (rspOrError.IsOK()) {
                auto rsp = rspOrError.Value();
                auto rspMap = ConvertToNode(TYsonString(rsp->value()))->AsMap();

                auto version = ConvertTo<TString>(rspMap->GetChild("version"));
                auto startTime = ConvertTo<TString>(rspMap->GetChild("start_time"));
                fluent
                    .Item(instance)
                    .BeginMap()
                        .Item("start_time").Value(startTime)
                        .Item("version").Value(version)
                    .EndMap();
            } else {
                fluent
                    .Item(instance)
                    .BeginMap()
                        .Item("error")
                        .Value(rspOrError)
                    .EndMap();
            }
        });
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
