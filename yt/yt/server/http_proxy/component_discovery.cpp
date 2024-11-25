#include "component_discovery.h"
#include "coordinator.h"

#include <yt/yt/client/api/client.h>

#include <util/string/split.h>

namespace NYT::NHttpProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillMasterReadOptions(TMasterReadOptions& options, const TMasterReadOptions& value)
{
    options = value;
}

bool IsComponentOptional(EClusterComponentType component)
{
    switch (component) {
        case EClusterComponentType::MasterCache:
        case EClusterComponentType::Discovery:
        case EClusterComponentType::TabletBalancer:
        case EClusterComponentType::ReplicatedTableTracker:
        case EClusterComponentType::QueueAgent:
        case EClusterComponentType::QueryTracker:
            return true;
        default:
            return false;
    }
}

// COMPAT(koloshmet)
bool IsComponentCompat(EClusterComponentType component)
{
    switch (component) {
        case EClusterComponentType::TabletBalancer:
        case EClusterComponentType::ReplicatedTableTracker:
            return true;
        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TClusterComponentInstance& instance, IYsonConsumer* consumer)
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

                if (!instance.State.empty()) {
                    fluent.Item("state").Value(instance.State);
                }
            })
            .DoIf(!instance.Error.IsOK(), [&] (auto fluent) {
                fluent.Item("error").Value(instance.Error);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TComponentDiscoverer::TComponentDiscoverer(
    IClientPtr client,
    TMasterReadOptions masterReadOptions,
    TComponentDiscoveryOptions componentDiscoveryOptions)
    : Client_(std::move(client))
    , MasterReadOptions_(std::move(masterReadOptions))
    , ComponentDiscoveryOptions_(std::move(componentDiscoveryOptions))
{
    YT_VERIFY(ComponentDiscoveryOptions_.ProxyDeathAgeCallback);
}

std::vector<TClusterComponentInstance> TComponentDiscoverer::ListClusterNodes(EClusterComponentType component) const
{
    switch (component) {
        case EClusterComponentType::ClusterNode:
        case EClusterComponentType::DataNode:
        case EClusterComponentType::TabletNode:
        case EClusterComponentType::ExecNode:
            break;
        default:
            YT_ABORT();
    }

    TListNodeOptions options;
    FillMasterReadOptions(options, MasterReadOptions_);
    options.Attributes = {
        "register_time",
        "version",
        "banned",
        "state",
        "job_proxy_build_version",
    };

    auto rsp = WaitFor(Client_->ListNode(GetCypressDirectory(component), options))
        .ValueOrThrow();
    auto rspList = ConvertToNode(rsp)->AsList();

    std::vector<TClusterComponentInstance> instances;
    instances.reserve(rspList->GetChildren().size());

    for (const auto& node : rspList->GetChildren()) {
        auto version = node->Attributes().Find<std::string>("version");
        auto nodeState = node->Attributes().Get<std::string>("state", /*defaultValue*/ "");
        auto startTime = node->Attributes().Find<std::string>("register_time");

        TClusterComponentInstance instance{
            .Type = component,
            .Address = node->GetValue<std::string>(),
            .Version = version.value_or(""),
            .StartTime = startTime.value_or(""),
            .Banned = node->Attributes().Get<bool>("banned", /*defaultValue*/ false),
            .Online = nodeState == "online",
            .State = nodeState,
            .Error = TError(),
            .JobProxyVersion = node->Attributes().Find<std::string>("job_proxy_build_version"),
        };

        if (instance.Online && (!version || !startTime)) {
            instance.Error = TError("Component is missing some of the required attributes in response")
                << TErrorAttribute("version", version)
                << TErrorAttribute("start_time", startTime);
        }

        instances.push_back(instance);
    }

    return instances;
}

std::vector<TClusterComponentInstance> TComponentDiscoverer::ListProxies(EClusterComponentType component) const
{
    TGetNodeOptions options;
    FillMasterReadOptions(options, MasterReadOptions_);

    switch (component) {
        case EClusterComponentType::RpcProxy:
            options.Attributes = {
                "start_time",
                "version",
                "banned",
            };
            break;
        case EClusterComponentType::HttpProxy:
            options.Attributes = {
                "liveness",
                "start_time",
                "version",
                "banned",
            };
            break;
        default:
            YT_ABORT();
    }

    auto nodeYson = WaitFor(Client_->GetNode(GetCypressDirectory(component), options))
        .ValueOrThrow();
    auto addressToNode = ConvertTo<THashMap<std::string, IMapNodePtr>>(nodeYson);

    std::vector<TClusterComponentInstance> instances;
    instances.reserve(addressToNode.size());

    auto timeNow = TInstant::Now();
    for (const auto& [address, node] : addressToNode) {
        auto version = node->Attributes().Find<std::string>("version");
        auto banned = node->Attributes().Find<bool>("banned");
        auto startTime = node->Attributes().Find<std::string>("start_time");

        TClusterComponentInstance instance{
            .Type = component,
            .Address = address,
        };

        if (version && startTime) {
            instance.Version = *version;
            instance.StartTime = *startTime;
            instance.Banned = banned.value_or(false);
        } else {
            instance.Error = TError("Cannot find required attributes in response")
                << TErrorAttribute("version", version)
                << TErrorAttribute("start_time", startTime);
        }

        if (component == EClusterComponentType::RpcProxy) {
            auto alive = node->AsMap()->FindChild("alive");
            instance.Online = static_cast<bool>(alive);
        } else if (auto livenessPtr = node->Attributes().Find<TLivenessPtr>("liveness")) {
            instance.Online = (livenessPtr->UpdatedAt + ComponentDiscoveryOptions_.ProxyDeathAgeCallback() >= timeNow);
        } else {
            instance.Error = TError("Liveness attribute is missing");
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

std::vector<TYPath> TComponentDiscoverer::GetCypressSubpaths(
    const NApi::IClientPtr& client,
    const NApi::TMasterReadOptions& masterReadOptions,
    EClusterComponentType component)
{
    std::vector<TYPath> paths;
    if (component == EClusterComponentType::SecondaryMaster) {
        TGetNodeOptions options;
        FillMasterReadOptions(options, masterReadOptions);
        auto directory = WaitFor(client->GetNode(GetCypressDirectory(component), options))
            .ValueOrThrow();
        for (const auto& [subdirectory, instances] : ConvertToNode(directory)->AsMap()->GetChildren()) {
            for (const auto& [instance, _] : instances->AsMap()->GetChildren()) {
                paths.push_back(Format("/%v/%v", subdirectory, instance));
            }
        }
    } else {
        TListNodeOptions options;
        FillMasterReadOptions(options, masterReadOptions);
        auto rspOrError = WaitFor(client->ListNode(GetCypressDirectory(component)));
        if (!rspOrError.IsOK() && IsComponentOptional(component)) {
            return paths;
        }
        auto rsp = std::move(rspOrError).ValueOrThrow();
        auto rspList = ConvertToNode(rsp)->AsList();
        for (const auto& node : rspList->GetChildren()) {
            paths.push_back(Format("/%v", node->GetValue<std::string>()));
        }
    }
    return paths;
}

std::vector<TYPath> TComponentDiscoverer::GetCypressPaths(
    const NApi::IClientPtr& client,
    const NApi::TMasterReadOptions& masterReadOptions,
    EClusterComponentType component)
{
    auto paths = GetCypressSubpaths(client, masterReadOptions, component);
    auto cypressDirectory = GetCypressDirectory(component);

    for (auto& path : paths) {
        path = cypressDirectory + path;
    }

    return paths;
}

TErrorOr<std::string> TComponentDiscoverer::GetCompatBinaryVersion(const TYPath& path) const
{
    auto rspOrError = WaitFor(Client_->GetNode(path + "/orchid/build_info/binary_version"));
    if (!rspOrError.IsOK()) {
        return std::move(static_cast<TError&>(rspOrError));
    }

    try {
        return ConvertTo<std::string>(rspOrError.Value());
    } catch (const std::exception& ex) {
        return ex;
    }
}

std::vector<TClusterComponentInstance> TComponentDiscoverer::GetAttributes(
    EClusterComponentType component,
    const std::vector<TYPath>& subpaths,
    EClusterComponentType instanceType,
    const TYPath& suffix) const
{
    const auto OrchidTimeout = TDuration::Seconds(1);

    TGetNodeOptions options;
    FillMasterReadOptions(options, MasterReadOptions_);
    options.Timeout = OrchidTimeout;

    std::vector<TFuture<TYsonString>> responses;
    responses.reserve(subpaths.size());
    for (const auto& subpath : subpaths) {
        responses.push_back(Client_->GetNode(GetCypressDirectory(component) + subpath + suffix));
    }

    std::vector<TClusterComponentInstance> results;
    results.reserve(subpaths.size());
    for (size_t index = 0; index < subpaths.size(); ++index) {
        auto ysonOrError = WaitFor(responses[index]);

        auto& result = results.emplace_back();
        result.Type = instanceType;

        result.Address = StringSplitter(subpaths[index]).Split('/').ToList<std::string>().back();
        if (!ysonOrError.IsOK()) {
            if (IsComponentCompat(component)) {
                auto versionOrError = GetCompatBinaryVersion(GetCypressDirectory(component) + subpaths[index]);
                if (versionOrError.IsOK()) {
                    result.Version = std::move(versionOrError).Value();
                } else {
                    result.Error = std::move(versionOrError);
                }
            } else {
                result.Error = std::move(ysonOrError);
            }
            continue;
        }

        try {
            auto rspMap = ConvertToNode(ysonOrError.Value())->AsMap();

            if (auto errorNode = rspMap->FindChild("error")) {
                result.Error = ConvertTo<TError>(errorNode);
                continue;
            }

            auto version = ConvertTo<std::string>(rspMap->GetChildOrThrow("version"));
            auto startTime = ConvertTo<std::string>(rspMap->GetChildOrThrow("start_time"));

            result.Version = std::move(version);
            result.StartTime = std::move(startTime);
        } catch (const std::exception& ex) {
            result.Error = ex;
        }
    }
    return results;
}

std::vector<TClusterComponentInstance> TComponentDiscoverer::ListJobProxies() const
{
    auto execNodeInstances = ListClusterNodes(EClusterComponentType::ExecNode);

    std::vector<TClusterComponentInstance> instances;
    instances.reserve(execNodeInstances.size());

    std::vector<TYPath> fallbackInstances;
    for (auto& instance : execNodeInstances) {
        if (instance.Banned) {
            continue;
        }

        if (instance.JobProxyVersion) {
            instance.Type = EClusterComponentType::JobProxy;
            instance.Version = *instance.JobProxyVersion;
            instances.emplace_back(std::move(instance));
        } else if (instance.Online) {
            fallbackInstances.emplace_back(std::move(instance.Address));
        }
    }

    if (!fallbackInstances.empty()) {
        YT_LOG_DEBUG(
            "Falling back to fetching job proxy versions from orchids (InstanceCount: %v)",
            fallbackInstances.size());

        auto fallbackJobProxies = GetAttributes(
            EClusterComponentType::ClusterNode,
            fallbackInstances,
            EClusterComponentType::JobProxy,
            "/orchid/job_controller/job_proxy_build");

        // COMPAT(arkady-e1ppa): Remove this when all nodes will be 23.2
        fallbackInstances.clear();

        for (auto& jobProxy : fallbackJobProxies) {
            if (jobProxy.Error.IsOK()) {
                instances.emplace_back(std::move(jobProxy));
            } else {
                fallbackInstances.emplace_back(std::move(jobProxy.Address));
            }
        }

        fallbackJobProxies = GetAttributes(
            EClusterComponentType::ClusterNode,
            fallbackInstances,
            EClusterComponentType::JobProxy,
            "/orchid/exec_node/job_controller/job_proxy_build");

        for (auto& jobProxy : fallbackJobProxies) {
            instances.emplace_back(std::move(jobProxy));
        }
    }

    return instances;
}

TYPath TComponentDiscoverer::GetCypressDirectory(EClusterComponentType component)
{
    switch (component) {
        case EClusterComponentType::PrimaryMaster:
        case EClusterComponentType::SecondaryMaster:
        case EClusterComponentType::ClusterNode:
        case EClusterComponentType::DataNode:
        case EClusterComponentType::TabletNode:
        case EClusterComponentType::ExecNode:
        case EClusterComponentType::TimestampProvider:
        case EClusterComponentType::MasterCache:
            return Format("//sys/%lvs", component);
        case EClusterComponentType::Scheduler:
            return "//sys/scheduler/instances";
        case EClusterComponentType::ControllerAgent:
            return "//sys/controller_agents/instances";
        case EClusterComponentType::Discovery:
            return "//sys/discovery_servers";
         case EClusterComponentType::TabletBalancer:
            return "//sys/tablet_balancer/instances";
        case EClusterComponentType::BundleController:
            return "//sys/cell_balancers/instances";
        case EClusterComponentType::ReplicatedTableTracker:
            return "//sys/replicated_table_tracker/instances";
        case EClusterComponentType::QueueAgent:
            return "//sys/queue_agents/instances";
        case EClusterComponentType::QueryTracker:
            return "//sys/query_tracker/instances";
        case EClusterComponentType::RpcProxy:
            return RpcProxiesPath;
        case EClusterComponentType::HttpProxy:
            return HttpProxiesPath;
        default:
            YT_ABORT();
    }
}

std::vector<TClusterComponentInstance> TComponentDiscoverer::GetInstances(EClusterComponentType component) const
{
    switch (component) {
        case EClusterComponentType::PrimaryMaster:
        case EClusterComponentType::SecondaryMaster:
        case EClusterComponentType::Scheduler:
        case EClusterComponentType::ControllerAgent:
        case EClusterComponentType::TimestampProvider:
        case EClusterComponentType::Discovery:
        case EClusterComponentType::MasterCache:
        case EClusterComponentType::TabletBalancer:
        case EClusterComponentType::BundleController:
        case EClusterComponentType::ReplicatedTableTracker:
        case EClusterComponentType::QueueAgent:
        case EClusterComponentType::QueryTracker:
            return GetAttributes(
                component,
                GetCypressSubpaths(Client_, MasterReadOptions_, component),
                /*instanceType*/ component);
        case EClusterComponentType::ClusterNode:
        case EClusterComponentType::DataNode:
        case EClusterComponentType::TabletNode:
        case EClusterComponentType::ExecNode:
            return ListClusterNodes(component);
        case EClusterComponentType::JobProxy:
            return ListJobProxies();
        case EClusterComponentType::HttpProxy:
        case EClusterComponentType::RpcProxy:
            return ListProxies(component);
        default:
            THROW_ERROR_EXCEPTION("Unknown component type %Qlv", component);
    }
}

// TODO(achulkov2): Parallelize discovery of different components.
std::vector<TClusterComponentInstance> TComponentDiscoverer::GetAllInstances() const
{
    std::vector<TClusterComponentInstance> instances;
    for (auto component : TEnumTraits<EClusterComponentType>::GetDomainValues()) {
        auto componentInstances = GetInstances(component);
        instances.reserve(instances.size() + componentInstances.size());
        instances.insert(instances.end(), componentInstances.begin(), componentInstances.end());
    }

    return instances;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
