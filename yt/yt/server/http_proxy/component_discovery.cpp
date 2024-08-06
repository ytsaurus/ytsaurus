#include "component_discovery.h"
#include "coordinator.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <util/string/split.h>

namespace NYT::NHttpProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 DefaultCypressMaxSize = 1'000'000;

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
    NNative::IClientPtr client,
    TMasterReadOptions masterReadOptions,
    TComponentDiscoveryOptions componentDiscoveryOptions)
    : Client_(std::move(client))
    , MasterReadOptions_(std::move(masterReadOptions))
    , ComponentDiscoveryOptions_(std::move(componentDiscoveryOptions))
{ }

std::vector<TClusterComponentInstance> TComponentDiscoverer::ListClusterNodes(EClusterComponentType component) const
{
    YT_LOG_DEBUG("Listing cluster nodes (ComponentType: %lv)", component);

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

    options.MaxSize = DefaultCypressMaxSize;

    auto rsp = WaitFor(Client_->ListNode(GetCypressDirectory(component), options))
        .ValueOrThrow();
    auto rspList = ConvertToNode(rsp)->AsList();

    if (rspList->Attributes().Get("incomplete", false)) {
        THROW_ERROR_EXCEPTION("Received incomplete result while listing cluster node directory")
            << TErrorAttribute("path", GetCypressDirectory(component));
    }

    std::vector<TClusterComponentInstance> instances;
    instances.reserve(rspList->GetChildren().size());

    for (const auto& node : rspList->GetChildren()) {
        auto version = node->Attributes().Find<TString>("version");
        auto nodeState = node->Attributes().Get<TString>("state", /*defaultValue*/ "");
        auto startTime = node->Attributes().Find<TString>("register_time");

        TClusterComponentInstance instance{
            .Type = component,
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

std::vector<TClusterComponentInstance> TComponentDiscoverer::ListProxies(EClusterComponentType component) const
{
    YT_LOG_DEBUG("Listing cluster proxies (ComponentType: %lv)", component);

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

    options.MaxSize = DefaultCypressMaxSize;

    auto nodeYson = WaitFor(Client_->GetNode(GetCypressDirectory(component), options))
        .ValueOrThrow();
    auto nodeMap = ConvertTo<IMapNodePtr>(nodeYson);

    if (nodeMap->Attributes().Get("incomplete", false)) {
        THROW_ERROR_EXCEPTION("Received incomplete result while getting proxy attributes")
            << TErrorAttribute("path", GetCypressDirectory(component));
    }

    std::vector<TClusterComponentInstance> instances;
    instances.reserve(nodeMap->GetChildren().size());

    auto timeNow = TInstant::Now();
    for (const auto& [address, node] : nodeMap->GetChildren()) {
        auto version = node->Attributes().Find<TString>("version");
        auto banned = node->Attributes().Find<bool>("banned");
        auto startTime = node->Attributes().Find<TString>("start_time");

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
            instance.Online = (livenessPtr->UpdatedAt + ComponentDiscoveryOptions_.ProxyDeathAge >= timeNow);
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

std::vector<TString> TComponentDiscoverer::GetCypressSubpaths(EClusterComponentType component) const
{
    std::vector<TString> paths;
    TListNodeOptions options;
    FillMasterReadOptions(options, MasterReadOptions_);
    auto rsp = WaitFor(Client_->ListNode(GetCypressDirectory(component)))
        .ValueOrThrow();
    auto rspList = ConvertToNode(rsp)->AsList();
    paths.reserve(rspList->GetChildren().size());
    for (const auto& node : rspList->GetChildren()) {
        paths.push_back(node->GetValue<TString>());
    }
    return paths;
}

std::vector<TClusterComponentInstance> TComponentDiscoverer::GetAttributes(
    EClusterComponentType component,
    const std::vector<TString>& subpaths,
    EClusterComponentType instanceType,
    const TYPath& suffix) const
{
    YT_LOG_DEBUG("Fetching orchid attributes for cluster component (ComponentType: %lv)", component);

    auto proxy = CreateObjectServiceReadProxy(Client_, MasterReadOptions_.ReadFrom);
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetTimeout(ComponentDiscoveryOptions_.BatchRequestTimeout);

    for (const auto& subpath : subpaths) {
        batchReq->AddRequest(TYPathProxy::Get(GetCypressDirectory(component) + "/" + subpath + suffix));
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    if (!batchRspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error getting attributes from %lv orchids", component)
            << GetCumulativeError(batchRspOrError);
    }
    auto batchResponses = batchRspOrError.Value()->GetResponses<TYPathProxy::TRspGet>();

    std::vector<TClusterComponentInstance> results;
    results.reserve(subpaths.size());
    for (size_t index = 0; index < subpaths.size(); ++index) {
        auto responseOrError = batchResponses[index];

        auto& result = results.emplace_back();
        result.Type = instanceType;

        result.Address = StringSplitter(subpaths[index]).Split('/').ToList<TString>().back();
        if (!responseOrError.IsOK()) {
            result.Error = responseOrError;
            continue;
        }

        auto rspMap = ConvertToNode(TYsonString(responseOrError.Value()->value()))->AsMap();

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

std::vector<TClusterComponentInstance> TComponentDiscoverer::ListJobProxies() const
{
    YT_LOG_DEBUG("Listing cluster job proxies");

    auto execNodeInstances = ListClusterNodes(EClusterComponentType::ExecNode);

    std::vector<TClusterComponentInstance> instances;
    instances.reserve(execNodeInstances.size());

    std::vector<TString> fallbackInstances;
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
            "/orchid/exec_node/job_controller/job_proxy_build");

        for (auto& jobProxy : fallbackJobProxies) {
            instances.emplace_back(std::move(jobProxy));
        }
    }

    return instances;
}

TString TComponentDiscoverer::GetCypressDirectory(EClusterComponentType component)
{
    switch (component) {
        case EClusterComponentType::Master:
            return "//sys/cluster_masters";
        case EClusterComponentType::ClusterNode:
        case EClusterComponentType::DataNode:
        case EClusterComponentType::TabletNode:
        case EClusterComponentType::ExecNode:
            return Format("//sys/%lvs", component);
        case EClusterComponentType::Scheduler:
            return "//sys/scheduler/instances";
        case EClusterComponentType::ControllerAgent:
            return "//sys/controller_agents/instances";
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
    YT_LOG_DEBUG("Listing cluster component instances (ComponentType: %lv)", component);

    switch (component) {
        case EClusterComponentType::Master:
        case EClusterComponentType::Scheduler:
        case EClusterComponentType::ControllerAgent:
            return GetAttributes(
                component,
                GetCypressSubpaths(component),
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
            THROW_ERROR_EXCEPTION("Unknown component type %Qv", component);
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
