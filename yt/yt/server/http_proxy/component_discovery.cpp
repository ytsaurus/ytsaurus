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

static const auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillMasterReadOptions(TMasterReadOptions& options, const TMasterReadOptions& value)
{
    options = value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

// NB: Keep this sad piece of code in sync with the enum values.
TString GetInstanceType(EYTComponentType componentType)
{
    switch (componentType) {
        case EYTComponentType::PrimaryMasters:
        case EYTComponentType::SecondaryMasters:
        case EYTComponentType::Schedulers:
        case EYTComponentType::ControllerAgents:
        case EYTComponentType::ClusterNodes:
        case EYTComponentType::DataNodes:
        case EYTComponentType::TabletNodes:
        case EYTComponentType::ExecNodes: {
            auto simplePlural = Format("%lv", componentType);
            YT_VERIFY(std::ssize(simplePlural) >= 1);
            simplePlural.pop_back();
            return simplePlural;
        }
        case EYTComponentType::JobProxies:
            return "job_proxy";
        case EYTComponentType::HttpProxies:
            return "http_proxy";
        case EYTComponentType::RpcProxies:
            return "rpc_proxy";
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYTInstance& instance, IYsonConsumer* consumer)
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
    TDuration proxyDeathAge)
    : Client_(std::move(client))
    , MasterReadOptions_(std::move(masterReadOptions))
    , ProxyDeathAge_(proxyDeathAge)
{ }

std::vector<TYTInstance> TComponentDiscoverer::ListNodes(EYTComponentType component) const
{
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

    std::vector<TYTInstance> instances;
    instances.reserve(rspList->GetChildren().size());

    for (const auto& node : rspList->GetChildren()) {
        auto version = node->Attributes().Find<TString>("version");
        auto nodeState = node->Attributes().Get<TString>("node_state", /*defaultValue*/ "");
        auto startTime = node->Attributes().Find<TString>("register_time");

        TYTInstance instance{
            .Type = GetInstanceType(component),
            .Address = node->GetValue<TString>(),
            .Version = version.value_or(""),
            .StartTime = startTime.value_or(""),
            .Banned = node->Attributes().Get<bool>("banned", /*defaultValue*/ false),
            .Online = nodeState == "online",
            .State = nodeState,
            .Error = TError(),
            .JobProxyVersion = node->Attributes().Find<TString>("job_proxy_build_version"),
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

std::vector<TYTInstance> TComponentDiscoverer::ListProxies(EYTComponentType component) const
{
    TGetNodeOptions options;
    FillMasterReadOptions(options, MasterReadOptions_);

    switch (component) {
        case EYTComponentType::RpcProxies:
            options.Attributes = {
                "start_time",
                "version",
                "banned",
            };
            break;
        case EYTComponentType::HttpProxies:
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
    auto addressToNode = ConvertTo<THashMap<TString, IMapNodePtr>>(nodeYson);

    std::vector<TYTInstance> instances;
    instances.reserve(addressToNode.size());

    auto timeNow = TInstant::Now();
    for (const auto& [address, node] : addressToNode) {
        auto version = node->Attributes().Find<TString>("version");
        auto banned = node->Attributes().Find<bool>("banned");
        auto startTime = node->Attributes().Find<TString>("start_time");

        TYTInstance instance{
            .Type = GetInstanceType(component),
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

        if (component == EYTComponentType::RpcProxies) {
            auto alive = node->AsMap()->FindChild("alive");
            instance.Online = static_cast<bool>(alive);
        } else if (auto livenessPtr = node->Attributes().Find<TLivenessPtr>("liveness")) {
            instance.Online = (livenessPtr->UpdatedAt + ProxyDeathAge_ >= timeNow);
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

std::vector<TString> TComponentDiscoverer::GetCypressSubpaths(EYTComponentType component) const
{
    std::vector<TString> paths;
    if (component == EYTComponentType::SecondaryMasters) {
        TGetNodeOptions options;
        FillMasterReadOptions(options, MasterReadOptions_);
        auto directory = WaitFor(Client_->GetNode(GetCypressDirectory(component), options))
            .ValueOrThrow();
        for (const auto& subdirectory : ConvertToNode(directory)->AsMap()->GetChildren()) {
            for (const auto& instance : subdirectory.second->AsMap()->GetChildren()) {
                paths.push_back(subdirectory.first + "/" + instance.first);
            }
        }
    } else {
        TListNodeOptions options;
        FillMasterReadOptions(options, MasterReadOptions_);
        auto rsp = WaitFor(Client_->ListNode(GetCypressDirectory(component)))
            .ValueOrThrow();
        auto rspList = ConvertToNode(rsp)->AsList();
        for (const auto& node : rspList->GetChildren()) {
            paths.push_back(node->GetValue<TString>());
        }
    }
    return paths;
}

std::vector<TYTInstance> TComponentDiscoverer::GetAttributes(
    EYTComponentType component,
    const std::vector<TString>& subpaths,
    const TString& instanceType,
    const TYPath& suffix) const
{
    const auto OrchidTimeout = TDuration::Seconds(1);

    TGetNodeOptions options;
    FillMasterReadOptions(options, MasterReadOptions_);
    options.Timeout = OrchidTimeout;

    std::vector<TFuture<TYsonString>> responses;
    responses.reserve(subpaths.size());
    for (const auto& subpath : subpaths) {
        responses.push_back(Client_->GetNode(GetCypressDirectory(component) + "/" + subpath + suffix));
    }

    std::vector<TYTInstance> results;
    results.reserve(subpaths.size());
    for (size_t index = 0; index < subpaths.size(); ++index) {
        auto ysonOrError = WaitFor(responses[index]);

        auto& result = results.emplace_back();
        result.Type = instanceType;

        result.Address = StringSplitter(subpaths[index]).Split('/').ToList<TString>().back();
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
}

std::vector<TYTInstance> TComponentDiscoverer::ListJobProxies() const
{
    auto execNodeInstances = ListNodes(EYTComponentType::ExecNodes);

    std::vector<TYTInstance> instances;
    instances.reserve(execNodeInstances.size());

    std::vector<TString> fallbackInstances;
    for (auto& instance : execNodeInstances) {
        if (instance.Banned) {
            continue;
        }

        if (instance.JobProxyVersion) {
            instance.Type = GetInstanceType(EYTComponentType::JobProxies);
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
            EYTComponentType::ClusterNodes,
            fallbackInstances,
            GetInstanceType(EYTComponentType::JobProxies),
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
            EYTComponentType::ClusterNodes,
            fallbackInstances,
            GetInstanceType(EYTComponentType::JobProxies),
            "/orchid/exec_node/job_controller/job_proxy_build");

        for (auto& jobProxy : fallbackJobProxies) {
            instances.emplace_back(std::move(jobProxy));
        }
    }

    return instances;
}

TString TComponentDiscoverer::GetCypressDirectory(EYTComponentType component)
{
    switch (component) {
        case EYTComponentType::PrimaryMasters:
        case EYTComponentType::SecondaryMasters:
        case EYTComponentType::ClusterNodes:
        case EYTComponentType::DataNodes:
        case EYTComponentType::TabletNodes:
        case EYTComponentType::ExecNodes:
            return Format("//sys/%lv", component);
        case EYTComponentType::Schedulers:
            return "//sys/scheduler/instances";
        case EYTComponentType::ControllerAgents:
            return Format("//sys/%lv/instances", component);
        case EYTComponentType::RpcProxies:
            return RpcProxiesPath;
        case EYTComponentType::HttpProxies:
            return HttpProxiesPath;
        default:
            YT_ABORT();
    }
}

std::vector<TYTInstance> TComponentDiscoverer::GetInstances(EYTComponentType component) const
{
    switch (component) {
        case EYTComponentType::PrimaryMasters:
        case EYTComponentType::SecondaryMasters:
        case EYTComponentType::Schedulers:
        case EYTComponentType::ControllerAgents:
            return GetAttributes(
                component,
                GetCypressSubpaths(component),
                GetInstanceType(component));
        case EYTComponentType::ClusterNodes:
        case EYTComponentType::DataNodes:
        case EYTComponentType::TabletNodes:
        case EYTComponentType::ExecNodes:
            return ListNodes(component);
        case EYTComponentType::JobProxies:
            return ListJobProxies();
        case EYTComponentType::HttpProxies:
        case EYTComponentType::RpcProxies:
            return ListProxies(component);
        default:
            THROW_ERROR_EXCEPTION("Unknown component type %Qv", component);
    }
}

// TODO(achulkov2): Parallelize discovery of different components.
std::vector<TYTInstance> TComponentDiscoverer::GetAllInstances() const
{
    std::vector<TYTInstance> instances;
    for (auto component : TEnumTraits<EYTComponentType>::GetDomainValues()) {
        auto componentInstances = GetInstances(component);
        instances.reserve(instances.size() + componentInstances.size());
        instances.insert(instances.end(), componentInstances.begin(), componentInstances.end());
    }

    return instances;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
