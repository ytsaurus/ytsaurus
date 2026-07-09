#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/debug_build_warning.h>
#include <yt/yt/flow/library/cpp/misc/node_info.h>
#include <yt/yt/flow/library/cpp/misc/proto/node_info.pb.h>
#include <yt/yt/flow/library/cpp/runner/config.h>
#include <yt/yt/flow/library/cpp/runner/node_info.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/helpers.h>

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

//! RAII helper to set/unset environment variables for vanilla job resolver tests.
class TEnvGuard
{
public:
    TEnvGuard(const std::string& name, const std::string& value)
        : Name_(name)
    {
        const char* old = std::getenv(name.c_str());
        if (old) {
            OldValue_ = old;
            HadOldValue_ = true;
        }
        ::setenv(name.c_str(), value.c_str(), /*overwrite*/ 1);
    }

    ~TEnvGuard()
    {
        if (HadOldValue_) {
            ::setenv(Name_.c_str(), OldValue_.c_str(), /*overwrite*/ 1);
        } else {
            ::unsetenv(Name_.c_str());
        }
    }

    TEnvGuard(const TEnvGuard&) = delete;
    TEnvGuard& operator=(const TEnvGuard&) = delete;

private:
    std::string Name_;
    std::string OldValue_;
    bool HadOldValue_ = false;
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
        "Exactly one of enable_ipv4 or enable_ipv6 must be set");
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
        "Exactly one of enable_ipv4 or enable_ipv6 must be set");
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
        "not IPv6 address");
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
        "not IPv4 address");
}

TEST(TGetNodeInfoTest, VanillaJobIPv4AddressWithIPv4Config)
{
    TEnvGuard ipGuard("YT_IP_ADDRESS_DEFAULT", "127.0.0.1");
    TEnvGuard clusterGuard("YT_CLUSTER_NAME", "test-cluster");
    TEnvGuard opGuard("YT_OPERATION_ID", "1-2-3-4");
    TEnvGuard jobGuard("YT_JOB_ID", "5-6-7-8");

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
        "YT_IP_ADDRESS_DEFAULT is not an IPv6 address");
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
        "YT_IP_ADDRESS_DEFAULT is not an IPv4 address");
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
        "not IPv6 address");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
