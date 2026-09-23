#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/debug_build_warning.h>
#include <yt/yt/flow/library/cpp/misc/node_address_provider.h>
#include <yt/yt/flow/library/cpp/misc/node_info.h>
#include <yt/yt/flow/library/cpp/misc/proto/node_info.pb.h>
#include <yt/yt/flow/library/cpp/misc/testing/env_guard.h>
#include <yt/yt/flow/library/cpp/runner/config.h>
#include <yt/yt/flow/library/cpp/runner/node_info.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/crypto/config.h>
#include <yt/yt/core/crypto/tls.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/helpers.h>

#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <cstdlib>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("NodeInfoTest");

TFlowNodeConfigPtr MakeConfig(const TString& yson)
{
    return NYTree::ConvertTo<TFlowNodeConfigPtr>(NYson::TYsonString(yson));
}

void ConfigureAddressResolver(const TFlowNodeConfigPtr& config)
{
    auto addressResolverConfig = config->GetSingletonConfig<NNet::TAddressResolverConfig>();
    NNet::TAddressResolver::Get()->Configure(addressResolverConfig);
}

////////////////////////////////////////////////////////////////////////////////

using NTesting::TEnvGuard;

////////////////////////////////////////////////////////////////////////////////

//! RAII helper to install a node address provider for the duration of a test.
class TNodeAddressProviderGuard
{
public:
    explicit TNodeAddressProviderGuard(TNodeAddressProvider provider)
        : Old_(GetNodeAddressProvider())
    {
        SetNodeAddressProvider(std::move(provider));
    }

    ~TNodeAddressProviderGuard()
    {
        SetNodeAddressProvider(std::move(Old_));
    }

    TNodeAddressProviderGuard(const TNodeAddressProviderGuard&) = delete;
    TNodeAddressProviderGuard& operator=(const TNodeAddressProviderGuard&) = delete;

private:
    TNodeAddressProvider Old_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TGetNodeInfoTest, DualStackRejected)
{
    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %true;
            enable_ipv6 = %true;
            localhost_name_override = "127.0.0.1";
        };
    })"));

    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "Exactly one of \"enable_ipv4\" and \"enable_ipv6\" must be set");
}

TEST(TGetNodeInfoTest, NoStackRejected)
{
    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %false;
            enable_ipv6 = %false;
            localhost_name_override = "127.0.0.1";
        };
    })"));

    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "Exactly one of \"enable_ipv4\" and \"enable_ipv6\" must be set");
}

TEST(TGetNodeInfoTest, IPv4OnlyWithLocalhostOverride)
{
    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        rpc_port = 1234;
        monitoring_port = 5678;
        address_resolver = {
            enable_ipv4 = %true;
            enable_ipv6 = %false;
            localhost_name_override = "127.0.0.1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    auto nodeInfo = GetNodeInfo(config, Logger);

    // FormatNetworkAddress wraps address in brackets: [127.0.0.1]:port.
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("127.0.0.1"));
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("1234"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("127.0.0.1"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("5678"));

    // Every node reports its build type so describe can surface the leader-controller's
    // value via the flow view. Compare against the helper rather than a literal so the
    // assertion is independent of the build mode CI happens to use.
    EXPECT_FALSE(nodeInfo->BuildType.empty());
    EXPECT_EQ(nodeInfo->BuildType, CurrentBuildTypeDisplayName());
}

TEST(TNodeInfoProtoTest, RoundTripsBuildType)
{
    TNodeInfoBase info;
    info.Name = "node-1";
    info.RpcAddress = "node-1.net:80";
    info.MonitoringAddress = "node-1.net:81";
    info.RemoteShellCommand = "ssh node-1";
    info.IncarnationId = NWorker::TIncarnationId(TGuid::Create());
    info.BuildVersion = "v1";
    info.FlowCoreVersion = "core-v1";
    info.BuildType = "ASAN";

    NProto::TNodeInfo proto;
    ToProto(&proto, info);

    TNodeInfoBase decoded;
    FromProto(&decoded, proto);

    EXPECT_EQ(decoded.BuildType, "ASAN");
}

TEST(TGetNodeInfoTest, IPv6OnlyWithLocalhostOverride)
{
    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        rpc_port = 1234;
        monitoring_port = 5678;
        address_resolver = {
            enable_ipv4 = %false;
            enable_ipv6 = %true;
            localhost_name_override = "::1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    auto nodeInfo = GetNodeInfo(config, Logger);

    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("::1"));
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("1234"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("::1"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("5678"));
}

TEST(TGetNodeInfoTest, IPv6ConfigWithIPv4OverrideMismatch)
{
    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %false;
            enable_ipv6 = %true;
            localhost_name_override = "127.0.0.1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "non-IPv6 address");
}

TEST(TGetNodeInfoTest, IPv4ConfigWithIPv6OverrideMismatch)
{
    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %true;
            enable_ipv6 = %false;
            localhost_name_override = "::1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "non-IPv4 address");
}

TEST(TGetNodeInfoTest, VanillaJobIPv4AddressWithIPv4Config)
{
    TEnvGuard ipGuard("YT_IP_ADDRESS_DEFAULT", "127.0.0.1");
    TEnvGuard clusterGuard("YT_CLUSTER_NAME", "test-cluster");
    TEnvGuard opGuard("YT_OPERATION_ID", "1-2-3-4");
    TEnvGuard jobGuard("YT_JOB_ID", "5-6-7-8");

    TEnvGuard factorGuard("YT_CPU_TO_VCPU_FACTOR", "1.5");
    TEnvGuard cpuLimitGuard("YT_FLOW_CPU_LIMIT", "10");
    TEnvGuard vcpuLimitGuard("YT_VCPU_LIMIT");

    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        rpc_port = 9999;
        monitoring_port = 8888;
        address_resolver = {
            enable_ipv4 = %true;
            enable_ipv6 = %false;
            localhost_name_override = "127.0.0.1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    auto nodeInfo = GetNodeInfo(config, Logger);

    EXPECT_EQ(nodeInfo->VcpuLimit, std::optional<double>(15000));
    {
        TEnvGuard explicitLimitGuard("YT_VCPU_LIMIT", "23000");
        EXPECT_EQ(GetNodeInfo(config, Logger)->VcpuLimit, std::optional<double>(23000));
    }
    {
        TEnvGuard noCpuLimitGuard("YT_FLOW_CPU_LIMIT");
        EXPECT_FALSE(GetNodeInfo(config, Logger)->VcpuLimit);
    }

    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("127.0.0.1"));
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("9999"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("127.0.0.1"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("8888"));
}

TEST(TGetNodeInfoTest, VanillaJobIPv6AddressWithIPv6Config)
{
    TEnvGuard ipGuard("YT_IP_ADDRESS_DEFAULT", "::1");
    TEnvGuard clusterGuard("YT_CLUSTER_NAME", "test-cluster");
    TEnvGuard opGuard("YT_OPERATION_ID", "1-2-3-4");
    TEnvGuard jobGuard("YT_JOB_ID", "5-6-7-8");

    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        rpc_port = 9999;
        monitoring_port = 8888;
        address_resolver = {
            enable_ipv4 = %false;
            enable_ipv6 = %true;
            localhost_name_override = "::1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    auto nodeInfo = GetNodeInfo(config, Logger);

    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("::1"));
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("9999"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("::1"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("8888"));
}

TEST(TGetNodeInfoTest, VanillaJobIPv4AddressWithIPv6ConfigMismatch)
{
    TEnvGuard ipGuard("YT_IP_ADDRESS_DEFAULT", "10.0.0.1");
    TEnvGuard clusterGuard("YT_CLUSTER_NAME", "test-cluster");
    TEnvGuard opGuard("YT_OPERATION_ID", "1-2-3-4");
    TEnvGuard jobGuard("YT_JOB_ID", "5-6-7-8");

    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %false;
            enable_ipv6 = %true;
            localhost_name_override = "::1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "is not an IPv6 address but \"enable_ipv6\" is set");
}

TEST(TGetNodeInfoTest, VanillaJobIPv6AddressWithIPv4ConfigMismatch)
{
    TEnvGuard ipGuard("YT_IP_ADDRESS_DEFAULT", "fd00::1");
    TEnvGuard clusterGuard("YT_CLUSTER_NAME", "test-cluster");
    TEnvGuard opGuard("YT_OPERATION_ID", "1-2-3-4");
    TEnvGuard jobGuard("YT_JOB_ID", "5-6-7-8");

    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %true;
            enable_ipv6 = %false;
            localhost_name_override = "127.0.0.1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "is not an IPv4 address but \"enable_ipv4\" is set");
}

TEST(TGetNodeInfoTest, DeployIPFromEnvironmentProvider)
{
    TEnvGuard fqdnGuard("DEPLOY_POD_PERSISTENT_FQDN", "pod-1.invalid");
    TEnvGuard boxGuard("DEPLOY_BOX_ID", "box");
    TNodeAddressProviderGuard providerGuard([] {
        return std::optional<std::string>("::1");
    });

    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        rpc_port = 9999;
        monitoring_port = 8888;
        address_resolver = {
            enable_ipv4 = %false;
            enable_ipv6 = %true;
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    // The node IP comes from the provider without DNS; the ssh hint's box IP
    // resolve attempt fails non-fatally ("box.pod-1.invalid" never resolves,
    // RFC 6761) and falls back to the box FQDN.
    auto nodeInfo = GetNodeInfo(config, Logger);

    EXPECT_EQ(nodeInfo->Name, "box.pod-1.invalid");
    EXPECT_EQ(nodeInfo->RemoteShellCommand, "ssh nobody@box.pod-1.invalid");
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("::1"));
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("9999"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("::1"));
    EXPECT_THAT(nodeInfo->MonitoringAddress, testing::HasSubstr("8888"));
}

TEST(TGetNodeInfoTest, DeployMalformedEnvironmentAddressFallsBackToDns)
{
    TEnvGuard fqdnGuard("DEPLOY_POD_PERSISTENT_FQDN", "pod-1.invalid");
    TEnvGuard boxGuard("DEPLOY_BOX_ID", "box");
    TNodeAddressProviderGuard providerGuard([] {
        return std::optional<std::string>("not-an-ip");
    });

    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %false;
            enable_ipv6 = %true;
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    // The malformed provider address is ignored and the resolver falls back
    // to DNS; "box.pod-1.invalid" never resolves (RFC 6761).
    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "Unable to resolve local address from fqdn");
}

TEST(TGetNodeInfoTest, DeployWrongStackEnvironmentAddressFallsBackToDns)
{
    TEnvGuard fqdnGuard("DEPLOY_POD_PERSISTENT_FQDN", "pod-1.invalid");
    TEnvGuard boxGuard("DEPLOY_BOX_ID", "box");
    TNodeAddressProviderGuard providerGuard([] {
        return std::optional<std::string>("::1");
    });

    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %true;
            enable_ipv6 = %false;
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    // The IPv6 provider address does not match the IPv4-only config, so the
    // resolver falls back to DNS, which never resolves "box.pod-1.invalid".
    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "Unable to resolve local address from fqdn");
}

TEST(TGetNodeInfoTest, DeployNonLocalEnvironmentAddressFallsBackToDns)
{
    TEnvGuard fqdnGuard("DEPLOY_POD_PERSISTENT_FQDN", "pod-1.invalid");
    TEnvGuard boxGuard("DEPLOY_BOX_ID", "box");
    TNodeAddressProviderGuard providerGuard([] {
        return std::optional<std::string>("2a02:6b8::1:1");
    });

    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            enable_ipv4 = %false;
            enable_ipv6 = %true;
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    // The provider address is not assigned to any local interface, so the
    // resolver falls back to DNS, which never resolves "box.pod-1.invalid".
    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "Unable to resolve local address from fqdn");
}

TEST(TGetNodeInfoTest, DefaultConfigUsesIPv6)
{
    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        rpc_port = 4321;
        monitoring_port = 8765;
        address_resolver = {
            localhost_name_override = "::1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    auto nodeInfo = GetNodeInfo(config, Logger);

    // Default behavior should resolve to IPv6.
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("::1"));
    EXPECT_THAT(nodeInfo->RpcAddress, testing::HasSubstr("4321"));
}

TEST(TTryExtractDeploySnapshotIdTest, SnapshotWorkloadStart)
{
    std::vector<TProcessCgroup> cgroups{
        {1, "freezer", {"freezer"}, "/porto%noop-pipeline-snap-worker-57/pod_agent_workload_flow-main_sn_C9CB3595F9_start"},
    };
    EXPECT_EQ(TryExtractDeploySnapshotId(cgroups), std::optional<std::string>("C9CB3595F9"));
}

TEST(TTryExtractDeploySnapshotIdTest, ClassicWorkloadStart)
{
    std::vector<TProcessCgroup> cgroups{
        {1, "freezer", {"freezer"}, "/porto%noop-pipeline-worker-1/pod_agent_workload_flow-main_start"},
    };
    EXPECT_EQ(TryExtractDeploySnapshotId(cgroups), std::nullopt);
}

TEST(TTryExtractDeploySnapshotIdTest, CgroupV2SingleLine)
{
    std::vector<TProcessCgroup> cgroups{
        {0, "", {}, "/porto%pod-1/pod_agent_workload_flow-main_sn_ABCDEF_start"},
    };
    EXPECT_EQ(TryExtractDeploySnapshotId(cgroups), std::optional<std::string>("ABCDEF"));
}

TEST(TTryExtractDeploySnapshotIdTest, LastSnapshotInfixWins)
{
    std::vector<TProcessCgroup> cgroups{
        {1, "freezer", {"freezer"}, "/porto%pod-1/pod_agent_workload_user_sn_123_sn_456_start"},
    };
    EXPECT_EQ(TryExtractDeploySnapshotId(cgroups), std::optional<std::string>("456"));
}

TEST(TTryExtractDeploySnapshotIdTest, LowercaseIdRejected)
{
    std::vector<TProcessCgroup> cgroups{
        {1, "freezer", {"freezer"}, "/porto%pod-1/pod_agent_workload_flow-main_sn_c9cb_start"},
    };
    EXPECT_EQ(TryExtractDeploySnapshotId(cgroups), std::nullopt);
}

TEST(TTryExtractDeploySnapshotIdTest, EmptyIdRejected)
{
    std::vector<TProcessCgroup> cgroups{
        {1, "freezer", {"freezer"}, "/porto%pod-1/pod_agent_workload_flow-main_sn__start"},
    };
    EXPECT_EQ(TryExtractDeploySnapshotId(cgroups), std::nullopt);
}

TEST(TTryExtractDeploySnapshotIdTest, NonStartContainersIgnored)
{
    std::vector<TProcessCgroup> cgroups{
        {1, "freezer", {"freezer"}, "/porto%pod-1/pod_agent_workload_flow-main_sn_C9CB3595F9_readiness"},
    };
    EXPECT_EQ(TryExtractDeploySnapshotId(cgroups), std::nullopt);
}

TEST(TTryExtractDeploySnapshotIdTest, EmptyCgroups)
{
    EXPECT_EQ(TryExtractDeploySnapshotId({}), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TGetNodeInfoTest, DefaultConfigRejectsIPv4Address)
{
    auto config = MakeConfig(TString(R"({
        cluster_url = "test-cluster";
        path = "//home/test";
        address_resolver = {
            localhost_name_override = "127.0.0.1";
            resolve_hostname_into_fqdn = %false;
        };
    })"));

    ConfigureAddressResolver(config);

    EXPECT_THROW_WITH_SUBSTRING(
        GetNodeInfo(config, Logger),
        "non-IPv6 address");
}

////////////////////////////////////////////////////////////////////////////////

NCrypto::TX509Ptr ReadCertificate(const std::string& pem)
{
    NCrypto::TBioPtr bio(BIO_new_mem_buf(pem.data(), pem.size()));
    NCrypto::TX509Ptr certificate(PEM_read_bio_X509(bio.get(), /*x*/ nullptr, /*cb*/ nullptr, /*u*/ nullptr));
    YT_VERIFY(certificate);
    return certificate;
}

TEST(TGenerateIncarnationCertificateTest, IPv6AddressAndHostName)
{
    TNodeInfoBase nodeInfo;
    nodeInfo.Name = "flow-node.example.net";
    nodeInfo.RpcAddress = "[2001:db8::1]:8080";
    nodeInfo.IncarnationId = TIncarnationId(TGuid::Create());

    auto certificate = ReadCertificate(GenerateIncarnationCertificate(nodeInfo).CertificatePem);
    EXPECT_EQ(X509_check_ip_asc(certificate.get(), "2001:db8::1", /*flags*/ 0), 1);
    EXPECT_EQ(X509_check_host(certificate.get(), "flow-node.example.net", /*chklen*/ 0, /*flags*/ 0, /*peername*/ nullptr), 1);

    char commonName[64] = {};
    X509_NAME_get_text_by_NID(X509_get_subject_name(certificate.get()), NID_commonName, commonName, sizeof(commonName));
    EXPECT_EQ(std::string(commonName), Format("yt-flow-%v", nodeInfo.IncarnationId));
}

TEST(TGenerateIncarnationCertificateTest, IPv4AddressAsName)
{
    TNodeInfoBase nodeInfo;
    nodeInfo.Name = "127.0.0.1";
    nodeInfo.RpcAddress = "[127.0.0.1]:8080";

    auto certificate = ReadCertificate(GenerateIncarnationCertificate(nodeInfo).CertificatePem);
    EXPECT_EQ(X509_check_ip_asc(certificate.get(), "127.0.0.1", /*flags*/ 0), 1);
    EXPECT_EQ(X509_check_host(certificate.get(), "127.0.0.1", /*chklen*/ 0, /*flags*/ 0, /*peername*/ nullptr), 0);
}

TEST(TGenerateIncarnationCertificateTest, CertificateIsSerializedOnlyWhenSet)
{
    auto nodeInfo = New<TNodeInfo>();
    nodeInfo->RpcAddress = "[::1]:8080";
    EXPECT_EQ(NYTree::ConvertToNode(nodeInfo)->AsMap()->FindChild("certificate_pem"), nullptr);

    auto certificate = GenerateIncarnationCertificate(*nodeInfo);
    nodeInfo->CertificatePem = certificate.CertificatePem;
    nodeInfo->CertificateSha256 = certificate.CertificateSha256;
    auto parsed = NYTree::ConvertTo<TNodeInfoPtr>(NYson::ConvertToYsonString(nodeInfo));
    EXPECT_EQ(parsed->CertificatePem, certificate.CertificatePem);
    EXPECT_EQ(parsed->CertificateSha256, certificate.CertificateSha256);
}

NBus::NTcp::TBusServerConfigPtr MakeBusServerConfig()
{
    return NBus::NTcp::TBusServerConfig::CreateTcp(/*port*/ 8080);
}

TNodeInfoPtr MakeControllerNodeInfo()
{
    auto nodeInfo = New<TNodeInfo>();
    nodeInfo->RpcAddress = "[::1]:8080";
    return nodeInfo;
}

TEST(TIncarnationCertificateTest, ServesAndPublishesGeneratedCertificate)
{
    auto nodeInfo = MakeControllerNodeInfo();
    auto busServerConfig = MakeBusServerConfig();
    auto servingConfig = CreateBusServerConfigWithIncarnationCertificate(
        nodeInfo.Get(),
        busServerConfig,
        NLogging::TLogger("Test"));

    ASSERT_TRUE(nodeInfo->CertificatePem);
    ASSERT_TRUE(nodeInfo->CertificateSha256);
    ASSERT_TRUE(servingConfig->CertificateChain);
    ASSERT_TRUE(servingConfig->PrivateKey);
    EXPECT_EQ(servingConfig->CertificateChain->Value, *nodeInfo->CertificatePem);
    EXPECT_EQ(NCrypto::GetFingerprintSHA256(ReadCertificate(*nodeInfo->CertificatePem)), *nodeInfo->CertificateSha256);
    // Encryption stays optional: peers that do not ask for TLS keep plain TCP.
    EXPECT_EQ(servingConfig->EncryptionMode, NBus::EEncryptionMode::Optional);
    // The rest of the bus server configuration survives the copy.
    EXPECT_EQ(servingConfig->Port, busServerConfig->Port);

    // The private key reaches neither the node config nor the published node info.
    EXPECT_NE(servingConfig, busServerConfig);
    EXPECT_FALSE(busServerConfig->CertificateChain);
    EXPECT_FALSE(busServerConfig->PrivateKey);
    EXPECT_THAT(
        NYson::ConvertToYsonString(busServerConfig).ToString(),
        ::testing::Not(::testing::HasSubstr("PRIVATE KEY")));
    EXPECT_EQ(NYTree::ConvertToNode(nodeInfo)->AsMap()->FindChild("private_key"), nullptr);
    EXPECT_THAT(NYson::ConvertToYsonString(nodeInfo).ToString(), ::testing::Not(::testing::HasSubstr("PRIVATE KEY")));
}

TEST(TIncarnationCertificateTest, KeepsConfiguredTlsMaterial)
{
    auto configured = GenerateSelfSignedCertificate({.CommonName = "configured"});
    auto busServerConfig = MakeBusServerConfig();
    busServerConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    busServerConfig->CertificateChain->Value = configured.CertificatePem;
    busServerConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    busServerConfig->PrivateKey->Value = configured.PrivateKeyPem;
    auto certificateChain = busServerConfig->CertificateChain;
    auto privateKey = busServerConfig->PrivateKey;

    auto nodeInfo = MakeControllerNodeInfo();
    auto servingConfig = CreateBusServerConfigWithIncarnationCertificate(
        nodeInfo.Get(),
        busServerConfig,
        NLogging::TLogger("Test"));

    EXPECT_EQ(servingConfig, busServerConfig);
    EXPECT_EQ(busServerConfig->CertificateChain, certificateChain);
    EXPECT_EQ(busServerConfig->CertificateChain->Value, configured.CertificatePem);
    EXPECT_EQ(busServerConfig->PrivateKey, privateKey);
    EXPECT_EQ(busServerConfig->PrivateKey->Value, configured.PrivateKeyPem);
    EXPECT_FALSE(nodeInfo->CertificatePem);
    EXPECT_FALSE(nodeInfo->CertificateSha256);
}

TEST(TIncarnationCertificateTest, NothingWithDisabledEncryption)
{
    auto busServerConfig = MakeBusServerConfig();
    busServerConfig->EncryptionMode = NBus::EEncryptionMode::Disabled;

    auto nodeInfo = MakeControllerNodeInfo();
    auto servingConfig = CreateBusServerConfigWithIncarnationCertificate(
        nodeInfo.Get(),
        busServerConfig,
        NLogging::TLogger("Test"));

    EXPECT_EQ(servingConfig, busServerConfig);
    EXPECT_FALSE(busServerConfig->CertificateChain);
    EXPECT_FALSE(busServerConfig->PrivateKey);
    EXPECT_FALSE(nodeInfo->CertificatePem);
    EXPECT_FALSE(nodeInfo->CertificateSha256);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
