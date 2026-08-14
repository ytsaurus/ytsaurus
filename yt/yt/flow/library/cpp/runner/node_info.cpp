#include "node_info.h"

#include <yt/yt/flow/library/cpp/common/flow_core_version.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/flow/library/cpp/misc/debug_build_warning.h>
#include <yt/yt/flow/library/cpp/misc/node_address_provider.h>

#include <yt/yt/build/build.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>

#include <util/string/split.h>

namespace NYT::NFlow {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNodeInfoResolver
{
public:
    TNodeInfoResolver(const TFlowNodeConfigPtr& config, const TLogger& logger)
        : Logger(logger)
        , Config(config)
    { }

    // Can be called only if IsEnvironmentRecognized() is true.
    TNodeInfoPtr Resolve()
    {
        auto nodeInfo = New<TNodeInfo>();

        nodeInfo->Name = GetNodeName();

        auto ip = GetNodeIP();
        nodeInfo->RpcAddress = NNet::FormatNetworkAddress(ip, Config->RpcPort);
        nodeInfo->MonitoringAddress = NNet::FormatNetworkAddress(ip, Config->MonitoringPort);

        nodeInfo->RemoteShellCommand = GetRemoteShellCommand();
        nodeInfo->IncarnationId = GetIncarnationId();
        nodeInfo->VcpuFactor = TryGetVCpuFactor();
        nodeInfo->VcpuLimit = TryGetVCpuLimit();
        nodeInfo->BuildVersion = GetVersion();
        nodeInfo->FlowCoreVersion = ResolveFlowCoreVersion();
        nodeInfo->BuildType = CurrentBuildTypeDisplayName();

        return nodeInfo;
    }

    virtual bool IsEnvironmentRecognized()
    {
        return true;
    }

protected:
    const TLogger Logger;
    const TFlowNodeConfigPtr Config;

protected:
    TIncarnationId GetIncarnationId()
    {
        static const auto incarnationId = NWorker::TIncarnationId(TGuid::Create());
        return incarnationId;
    }

    virtual std::optional<double> TryGetVCpuFactor()
    {
        return std::nullopt;
    }

    virtual std::optional<double> TryGetVCpuLimit()
    {
        return std::nullopt;
    }

    std::string ResolveLocalAddress(const std::string& localFqdn)
    {
        auto* resolver = NNet::TAddressResolver::Get();
        auto address = NConcurrency::WaitFor(resolver->Resolve(localFqdn))
            .ValueOrThrow("Unable to resolve local address from fqdn %v", localFqdn);

        auto addressResolverConfig = Config->GetSingletonConfig<NNet::TAddressResolverConfig>();
        std::string addressStr;
        if (addressResolverConfig->EnableIPv6) {
            THROW_ERROR_EXCEPTION_UNLESS(address.IsIP6(),
                "Local FQDN %v resolved to non-IPv6 address %v",
                localFqdn,
                address);
            addressStr = ToString(address.ToIP6Address());
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(address.IsIP4(),
                "Local FQDN %v resolved to non-IPv4 address %v",
                localFqdn,
                address);
            addressStr = ToString(address, {.IncludePort = false, .IncludeTcpProtocol = false});
        }

        if (!resolver->IsLocalAddress(address)) {
            THROW_ERROR_EXCEPTION("Extracted IP of local fqdn is not resolved to one of local IP addresses; probably DNS is updating slowly")
                .With("local_fqdn", localFqdn)
                .With("resolved_ip", addressStr);
        }
        return addressStr;
    }

    std::optional<std::string> TryGetIPFromEnvironment()
    {
        const auto& provider = GetNodeAddressProvider();
        if (!provider) {
            return std::nullopt;
        }

        auto address = provider();
        if (!address) {
            return std::nullopt;
        }

        const auto& addressStr = *address;
        auto addressOrError = NNet::TNetworkAddress::TryParse(addressStr);
        if (!addressOrError.IsOK()) {
            YT_TLOG_WARNING("Malformed IP address in environment, falling back to DNS resolve")
                .With("Address", addressStr);
            return std::nullopt;
        }

        const auto& parsedAddress = addressOrError.Value();
        auto addressResolverConfig = Config->GetSingletonConfig<NNet::TAddressResolverConfig>();
        if (addressResolverConfig->EnableIPv6 ? !parsedAddress.IsIP6() : !parsedAddress.IsIP4()) {
            YT_TLOG_WARNING("IP address from environment does not match the configured stack, falling back to DNS resolve")
                .With("Address", addressStr);
            return std::nullopt;
        }

        if (!NNet::TAddressResolver::Get()->IsLocalAddress(parsedAddress)) {
            YT_TLOG_WARNING("IP address from environment is not assigned to a local interface, falling back to DNS resolve")
                .With("Address", addressStr);
            return std::nullopt;
        }

        YT_TLOG_INFO("Extracted node IP from environment")
            .With("Address", addressStr);
        return addressStr;
    }

    virtual std::string GetNodeName()
    {
        return NNet::GetLocalHostName();
    }

    virtual std::string GetNodeIP()
    {
        return ResolveLocalAddress(NNet::GetLocalHostName());
    }

    virtual std::string GetRemoteShellCommand()
    {
        return Format("ssh %v", GetNodeIP());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDeployNodeInfoResolver
    : public TNodeInfoResolver
{
public:
    using TNodeInfoResolver::TNodeInfoResolver;

    bool IsEnvironmentRecognized() override
    {
        return PodFqdn_ && BoxName_;
    }

protected:
    std::string GetNodeName() override
    {
        return GetDeployBoxFqdn();
    }

    std::string GetNodeIP() override
    {
        if (auto address = TryGetIPFromEnvironment()) {
            return *address;
        }
        BoxAddress_ = ResolveLocalAddress(GetDeployBoxFqdn());
        return *BoxAddress_;
    }

    std::string GetRemoteShellCommand() override
    {
        const auto& fqdn = TryGetDeployBoxFqdn();
        if (!fqdn) {
            return {};
        }

        // SSH answers on the box IP, not on the pod IP, and the box IP is only
        // available via DNS, which may still be propagating after a pod reschedule;
        // try to resolve it once and fall back to the box FQDN.
        if (!BoxAddress_) {
            try {
                BoxAddress_ = ResolveLocalAddress(*fqdn);
            } catch (const std::exception& ex) {
                YT_TLOG_WARNING("Failed to resolve box IP, using fqdn in remote shell command")
                    .With("Fqdn", *fqdn)
                    .With(ex);
            }
        }
        if (BoxAddress_) {
            return Format("ssh nobody@%v (or ssh nobody@%v)", *BoxAddress_, *fqdn);
        }
        return Format("ssh nobody@%v", *fqdn);
    }

    std::string GetDeployBoxFqdn()
    {
        return TryGetDeployBoxFqdn().value_or(Format("%v.%v", BoxName_, PodFqdn_));
    }

    //! Returns null when the cgroups cannot be read, so that a snapshot stage box
    //! cannot be told from a classic one.
    const std::optional<std::string>& TryGetDeployBoxFqdn()
    {
        if (!BoxFqdnBuilt_) {
            BoxFqdn_ = TryBuildDeployBoxFqdn();
            BoxFqdnBuilt_ = true;
        }
        return BoxFqdn_;
    }

    std::optional<std::string> TryBuildDeployBoxFqdn()
    {
        std::optional<std::string> snapshotId;
        try {
            snapshotId = TryExtractDeploySnapshotId(GetProcessCgroups());
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to extract snapshot id from process cgroups, box fqdn is unknown")
                .With(ex);
            return std::nullopt;
        }
        if (snapshotId) {
            YT_TLOG_INFO("Detected snapshot stage box, using suffixed box fqdn")
                .With("SnapshotId", *snapshotId);
            return Format("%v_sn_%v.%v", BoxName_, *snapshotId, PodFqdn_);
        }
        return Format("%v.%v", BoxName_, PodFqdn_);
    }

    std::optional<double> TryGetVCpuFactor() override
    {
        try {
            if (VcpuFactor_) {
                double value = FromString(VcpuFactor_);
                YT_TLOG_DEBUG("Extracted vcpu factor from YDeploy environment")
                    .With("VCpuFactor", value);
                return value;
            }
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to determine vcpu factor")
                .With(ex);
        }
        return std::nullopt;
    }

    std::optional<double> TryGetVCpuLimit() override
    {
        try {
            if (VcpuLimit_) {
                double value = FromString(VcpuLimit_);
                YT_TLOG_DEBUG("Extracted vcpu limit from YDeploy environment")
                    .With("VCpuLimit", value);
                return value;
            }
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to determine vcpu limit")
                .With(ex);
        }
        return std::nullopt;
    }

private:
    const char* PodFqdn_ = std::getenv("DEPLOY_POD_PERSISTENT_FQDN");
    const char* BoxName_ = std::getenv("DEPLOY_BOX_ID");
    const char* VcpuFactor_ = std::getenv("DEPLOY_CPU_TO_VCPU_FACTOR");
    const char* VcpuLimit_ = std::getenv("DEPLOY_VCPU_LIMIT");

    //! Box FQDN build result, cached to avoid re-reading cgroups.
    std::optional<std::string> BoxFqdn_;
    bool BoxFqdnBuilt_ = false;

    //! Box FQDN resolve result, cached to avoid a second DNS query.
    std::optional<std::string> BoxAddress_;
};

////////////////////////////////////////////////////////////////////////////////

class TVanillaJobNodeInfoResolver
    : public TNodeInfoResolver
{
public:
    using TNodeInfoResolver::TNodeInfoResolver;

    bool IsEnvironmentRecognized() override
    {
        return ClusterName_ && OperationId_ && JobId_;
    }

protected:
    std::string GetNodeIP() override
    {
        if (!Ip_) {
            return ResolveLocalAddress(NNet::GetLocalHostName());
        }

        auto address = NNet::TNetworkAddress::Parse(Ip_);
        auto addressResolverConfig = Config->GetSingletonConfig<NNet::TAddressResolverConfig>();
        if (addressResolverConfig->EnableIPv6) {
            THROW_ERROR_EXCEPTION_UNLESS(address.IsIP6(),
                "YT_IP_ADDRESS_DEFAULT %Qv is not an IPv6 address but \"enable_ipv6\" is set",
                Ip_);
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(address.IsIP4(),
                "YT_IP_ADDRESS_DEFAULT %Qv is not an IPv4 address but \"enable_ipv4\" is set",
                Ip_);
        }

        auto* resolver = NNet::TAddressResolver::Get();
        if (!resolver->IsLocalAddress(address)) {
            THROW_ERROR_EXCEPTION("Extracted IP of vanilla job is not one of local IP addresses")
                .With("extracted_ip", Ip_);
        }
        return std::string(Ip_);
    }

    std::string GetRemoteShellCommand() override
    {
        return Format("ya tool yt --proxy %v run-job-shell %v", ClusterName_, JobId_);
    }

    std::optional<double> TryGetVCpuFactor() override
    {
        try {
            if (VcpuFactor_) {
                double value = FromString(VcpuFactor_);
                YT_TLOG_DEBUG("Extracted vcpu factor from YT environment")
                    .With("VCpuFactor", value);
                return value;
            }
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to determine vcpu factor")
                .With(ex);
        }

        // TODO(YTFLOW-587): drop this fallback once YT sets YT_CPU_TO_VCPU_FACTOR in the vanilla
        // job environment itself. Until then resolve our exec node and read its cpu_to_vcpu_factor
        // annotation.
        return TryFetchVCpuFactorFromExecNode();
    }

    std::optional<double> TryGetVCpuLimit() override
    {
        try {
            if (VcpuLimit_) {
                double value = FromString(VcpuLimit_);
                YT_TLOG_DEBUG("Extracted vcpu limit from YT environment")
                    .With("VCpuLimit", value);
                return value;
            }
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to determine vcpu limit")
                .With(ex);
        }
        return std::nullopt;
    }

private:
    std::optional<double> TryFetchVCpuFactorFromExecNode()
    {
        if (!ClusterName_ || !OperationId_ || !JobId_) {
            return std::nullopt;
        }

        try {
            auto connection = NApi::NRpcProxy::CreateConnection(
                NApi::NRpcProxy::TConnectionConfig::CreateFromClusterUrl(ClusterName_));
            auto client = connection->CreateClient(NApi::GetClientOptionsFromEnvStatic());

            NApi::TGetJobOptions options;
            options.Attributes = THashSet<std::string>{"address"};
            auto attributesYson = NConcurrency::WaitFor(client->GetJob(
                NScheduler::TOperationIdOrAlias::FromString(OperationId_),
                NJobTrackerClient::TJobId(TGuid::FromString(JobId_)),
                options))
                .ValueOrThrow();

            auto attributes = NYTree::ConvertTo<NYTree::IMapNodePtr>(attributesYson);
            auto address = NYTree::ConvertTo<TString>(attributes->GetChildOrThrow("address"));
            auto factorYson = NConcurrency::WaitFor(client->GetNode(
                Format("//sys/exec_nodes/%v/@annotations/cpu_to_vcpu_factor", address)))
                .ValueOrThrow();

            auto value = NYTree::ConvertTo<double>(factorYson);
            YT_TLOG_DEBUG("Fetched vcpu factor from exec node annotations")
                .With("VCpuFactor", value);
            return value;
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to fetch vcpu factor from exec node annotations")
                .With(ex);
            return std::nullopt;
        }
    }

    const char* Ip_ = std::getenv("YT_IP_ADDRESS_DEFAULT"); // Backbone IP.
    const char* ClusterName_ = std::getenv("YT_CLUSTER_NAME");
    const char* OperationId_ = std::getenv("YT_OPERATION_ID");
    const char* JobId_ = std::getenv("YT_JOB_ID");
    const char* VcpuFactor_ = std::getenv("YT_CPU_TO_VCPU_FACTOR");
    const char* VcpuLimit_ = std::getenv("YT_VCPU_LIMIT");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Pod agent composes container names as "<box id>/workload_<workload id>_<command>",
// see TPathHolder::GetWorkloadContainerWithName in
// infra/pod_agent/libs/path_util/path_holder.cpp. On a snapshot stage both ids carry
// a "_sn_<snapshot id>" suffix, see StageConverter::patchPodAgentSpecBySnapshot in
// infra/snapshot_controller/service/src/main/java/ru/yandex/infra/snapshotctl/core/converter/StageConverter.java.
std::optional<std::string> TryExtractDeploySnapshotId(const std::vector<TProcessCgroup>& cgroups)
{
    constexpr TStringBuf StartSuffix = "_start";
    constexpr TStringBuf SnapshotInfix = "_sn_";

    for (const auto& cgroup : cgroups) {
        for (TStringBuf segment : StringSplitter(cgroup.Path).Split('/').SkipEmpty()) {
            // The long-running workload command is the "_start" container; the other
            // commands (readiness, stop, ...) never host this process.
            if (!segment.ChopSuffix(StartSuffix)) {
                continue;
            }
            // The snapshot suffix is appended last, so a user workload id
            // containing "_sn_" cannot shadow it.
            auto infixPos = segment.rfind(SnapshotInfix);
            if (infixPos == TStringBuf::npos) {
                continue;
            }
            auto id = segment.SubStr(infixPos + SnapshotInfix.size());
            auto isSnapshotIdChar = [] (char c) {
                return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F');
            };
            if (!id.empty() && AllOf(id, isSnapshotIdChar)) {
                return std::string(id);
            }
        }
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TNodeInfoPtr GetNodeInfo(const TFlowNodeConfigPtr& config, const TLogger& logger)
{
    const TLogger& Logger = logger;

    auto addressResolverConfig = config->GetSingletonConfig<NNet::TAddressResolverConfig>();
    THROW_ERROR_EXCEPTION_IF(
        addressResolverConfig->EnableIPv4 == addressResolverConfig->EnableIPv6,
        "Exactly one of \"enable_ipv4\" and \"enable_ipv6\" must be set in address resolver config: "
        "enable_ipv4 is %v, enable_ipv6 is %v",
        addressResolverConfig->EnableIPv4,
        addressResolverConfig->EnableIPv6);

    auto vanillaJobResolver = TVanillaJobNodeInfoResolver(config, logger);
    auto deployResolver = TDeployNodeInfoResolver(config, logger);
    auto defaultResolver = TNodeInfoResolver(config, logger);

    if (vanillaJobResolver.IsEnvironmentRecognized() && deployResolver.IsEnvironmentRecognized()) {
        YT_TLOG_FATAL("Environment is recognized ambiguously: as YDeploy and as vanilla job");
    }

    TNodeInfoPtr nodeInfo = nullptr;
    if (vanillaJobResolver.IsEnvironmentRecognized()) {
        YT_TLOG_INFO("Node environment is recognized as vanilla job");
        nodeInfo = vanillaJobResolver.Resolve();
    } else if (deployResolver.IsEnvironmentRecognized()) {
        YT_TLOG_INFO("Node environment is recognized as YDeploy box");
        nodeInfo = deployResolver.Resolve();
    } else {
        YT_TLOG_INFO("Node environment is not recognized, default node info resolver is used");
        nodeInfo = defaultResolver.Resolve();
    }

    YT_TLOG_INFO("Node info is resolved")
        .With("NodeInfo", ConvertToYsonString(nodeInfo, NYson::EYsonFormat::Text));

    return nodeInfo;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
