#include "node_info.h"

#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/flow_core_version.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/flow/library/cpp/misc/debug_build_warning.h>

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
        nodeInfo->BinaryChecksum = GetBinaryChecksum();
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
                "Address resolved from FQDN is not IPv6 address "
                "(LocalFqdn: %v, ResolvedAddress: %v)",
                localFqdn,
                address);
            addressStr = ToString(address.ToIP6Address());
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(address.IsIP4(),
                "Address resolved from FQDN is not IPv4 address "
                "(LocalFqdn: %v, ResolvedAddress: %v)",
                localFqdn,
                address);
            addressStr = ToString(address, {.IncludePort = false, .IncludeTcpProtocol = false});
        }

        if (!resolver->IsLocalAddress(address)) {
            THROW_ERROR_EXCEPTION("Extracted IP of local fqdn is not resolved to one of local IP addresses; probably DNS is updating slowly")
                << TErrorAttribute("local_fqdn", localFqdn)
                << TErrorAttribute("resolved_ip", addressStr);
        }
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
        return ResolveLocalAddress(GetDeployBoxFqdn());
    }

    std::string GetRemoteShellCommand() override
    {
        return Format("ssh nobody@%v", GetNodeIP());
    }

    std::string GetDeployBoxFqdn()
    {
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
                "YT_IP_ADDRESS_DEFAULT is not an IPv6 address but enable_ipv6 is set "
                "(IP: %v)",
                Ip_);
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(address.IsIP4(),
                "YT_IP_ADDRESS_DEFAULT is not an IPv4 address but enable_ipv4 is set "
                "(IP: %v)",
                Ip_);
        }

        auto* resolver = NNet::TAddressResolver::Get();
        if (!resolver->IsLocalAddress(address)) {
            THROW_ERROR_EXCEPTION("Extracted IP of vanilla job is not one of local IP addresses")
                << TErrorAttribute("extracted_ip", Ip_);
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

TNodeInfoPtr GetNodeInfo(const TFlowNodeConfigPtr& config, const TLogger& logger)
{
    const TLogger& Logger = logger;

    auto addressResolverConfig = config->GetSingletonConfig<NNet::TAddressResolverConfig>();
    THROW_ERROR_EXCEPTION_IF(
        addressResolverConfig->EnableIPv4 == addressResolverConfig->EnableIPv6,
        "Exactly one of enable_ipv4 or enable_ipv6 must be set in address_resolver config "
        "(EnableIPv4: %v, EnableIPv6: %v)",
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
