#include <yt/core/test_framework/framework.h>

#include <yp/server/nodes/porto.h>

#include <util/string/join.h>
#include <util/string/split.h>

namespace NYP::NServer::NNodes::NTests {
namespace {

using namespace ::NYP::NServer::NObjects;

////////////////////////////////////////////////////////////////////////////////

TString SortedProps(const TString& s)
{
    TVector<TString> tokens;
    Split(s, TString(";"), tokens);
    Sort(tokens.begin(), tokens.end());
    return JoinSeq(";", tokens);
}

THashMap<TString, TString> BuildProps(
    const NClient::NApi::NProto::TResourceSpec_TCpuSpec& cpuSpec,
    const NProto::TPodSpecOther& podSpecOther,
    const NProto::TPodStatusOther& podStatusOther)
{
    const auto builtProperties = BuildPortoProperties(cpuSpec, podSpecOther, podStatusOther);
    THashMap<TString, TString> props(builtProperties.begin(), builtProperties.end());
    EXPECT_EQ(props.size(), builtProperties.size());
    return props;
}

TEST(BuildPortoProperties, TestCpuToVcpuFactor)
{
    NClient::NApi::NProto::TResourceSpec_TCpuSpec cpuSpec;
    NProto::TPodSpecOther podSpecOther;

    cpuSpec.set_cpu_to_vcpu_factor(2.0);

    podSpecOther.mutable_resource_requests()->set_vcpu_guarantee(2000);
    podSpecOther.mutable_resource_requests()->set_vcpu_limit(4000);

    const auto props = BuildProps(cpuSpec, podSpecOther, {});
    EXPECT_EQ("1.000c", props.at("cpu_guarantee"));
    EXPECT_EQ("2.000c", props.at("cpu_limit"));
}

TEST(BuildPortoProperties, TestInternetAddresses)
{
    NProto::TPodSpecOther podSpecOther;
    NProto::TPodStatusOther podStatusOther;

    auto* resourceRequest = podSpecOther.mutable_resource_requests();
    resourceRequest->set_vcpu_guarantee(1000);

    auto* alloc1 = podStatusOther.add_ip6_address_allocations();
    alloc1->set_vlan_id("fastbone");
    alloc1->set_address("2a02:6b8:fc08:aa7:10c:ba05:0:1bee");

    auto* alloc2 = podStatusOther.add_ip6_address_allocations();
    alloc2->set_vlan_id("backbone");
    alloc2->set_address("2a02:6b8:c0c:aa7:10c:ba05:0:1bee");

    auto* inetAddr = alloc2->mutable_internet_address();
    inetAddr->set_id("42");
    inetAddr->set_ip4_address("95.108.129.55");

    const auto props = BuildProps({}, podSpecOther, podStatusOther);

    EXPECT_EQ("L3 veth;ipip6 ip_ext_tun0 2a02:6b8:b010:a0ff::1 2a02:6b8:c0c:aa7:10c:ba05:0:1bee;MTU ip_ext_tun0 1400", props.at("net"));
    EXPECT_EQ(
        SortedProps("veth 2a02:6b8:fc08:aa7:10c:ba05:0:1bee;veth 2a02:6b8:c0c:aa7:10c:ba05:0:1bee;ip_ext_tun0 95.108.129.55"),
        SortedProps(props.at("ip")));
}

TEST(BuildPortoProperties, TestTunnelProperties)
{
    NClient::NApi::NProto::TResourceSpec_TCpuSpec cpuSpec;
    NProto::TPodSpecOther podSpecOther;
    NProto::TPodStatusOther podStatusOther;

    cpuSpec.set_cpu_to_vcpu_factor(1);

    auto* resourceRequest = podSpecOther.mutable_resource_requests();
    resourceRequest->set_vcpu_guarantee(1000);

    auto* alloc1 = podStatusOther.add_ip6_address_allocations();
    alloc1->set_vlan_id("backbone");
    alloc1->set_address("5678");

    auto* dns = podStatusOther.mutable_dns();
    dns->set_transient_fqdn("hello_world.yandex.net");

    auto* vsStatus1 = alloc1->add_virtual_services();
    vsStatus1->add_ip6_addresses("1234");
    vsStatus1->add_ip4_addresses("1.2.3.4");

    {
        auto props = BuildProps(cpuSpec, podSpecOther, podStatusOther);

        EXPECT_EQ("1.000c", props.at("cpu_guarantee"));
        EXPECT_EQ("hello_world.yandex.net", props.at("hostname"));
        EXPECT_EQ("L3 veth;ipip6 tun0 2a02:6b8:0:3400::aaaa 5678;MTU tun0 1450;MTU ip6tnl0 1450", props.at("net"));
        EXPECT_EQ("net.ipv4.conf.all.rp_filter:0;net.ipv4.conf.default.rp_filter:0;net.ipv4.conf.veth.rp_filter:0;net.ipv4.conf.ip6tnl0.rp_filter:0", props.at("sysctl"));
        EXPECT_EQ(
            SortedProps("veth 5678;ip6tnl0 1234;tun0 1.2.3.4"),
            SortedProps(props.at("ip")));
    }

    {
        auto* vsStatus2 = alloc1->add_virtual_services();
        vsStatus2->add_ip4_addresses("2.3.4.5");

        auto props = BuildProps(cpuSpec, podSpecOther, podStatusOther);

        EXPECT_EQ("L3 veth;ipip6 tun0 2a02:6b8:0:3400::aaaa 5678;MTU tun0 1450;MTU ip6tnl0 1450", props.at("net"));
        EXPECT_EQ("veth 5678;ip6tnl0 1234;tun0 1.2.3.4;tun0 2.3.4.5", props.at("ip"));

        auto* alloc2 = podStatusOther.add_ip6_address_allocations();
        alloc2->set_vlan_id("42bone");
        alloc2->set_address("42:42:42:42");

        auto* vsStatus3 = alloc2->add_virtual_services();
        vsStatus3->add_ip6_addresses("4242");
        vsStatus3->add_ip6_addresses("42424242");
        vsStatus3->add_ip4_addresses("42.42.42.42");
    }

    {
        auto props = BuildProps(cpuSpec, podSpecOther, podStatusOther);

        EXPECT_EQ("L3 veth;ipip6 tun0 2a02:6b8:0:3400::aaaa 5678;ipip6 tun0 2a02:6b8:0:3400::aaaa 42:42:42:42;MTU tun0 1450;MTU ip6tnl0 1450", props.at("net"));
        EXPECT_EQ(
            SortedProps("veth 5678;ip6tnl0 1234;ip6tnl0 4242;ip6tnl0 42424242;tun0 1.2.3.4;tun0 2.3.4.5;tun0 42.42.42.42;veth 42:42:42:42"),
            SortedProps(props.at("ip")));
    }
}

TEST(BuildPortoProperties, Limits)
{
    NClient::NApi::NProto::TResourceSpec_TCpuSpec cpuSpec;
    NProto::TPodSpecOther podSpecOther;
    NProto::TPodStatusOther podStatusOther;

    cpuSpec.set_cpu_to_vcpu_factor(1);

    auto* resourceRequest = podSpecOther.mutable_resource_requests();
    resourceRequest->set_vcpu_guarantee(1000);
    resourceRequest->set_vcpu_limit(2000);
    resourceRequest->set_memory_guarantee(2048);
    resourceRequest->set_memory_limit(4096);
    resourceRequest->set_anonymous_memory_limit(8192);
    resourceRequest->set_dirty_memory_limit(9999);

    auto dns = podStatusOther.mutable_dns();
    dns->set_transient_fqdn("limits.yandex.net");

    const auto props = BuildProps(cpuSpec, podSpecOther, podStatusOther);

    EXPECT_EQ("1.000c", props.at("cpu_guarantee"));
    EXPECT_EQ("2.000c", props.at("cpu_limit"));
    EXPECT_EQ("2048", props.at("memory_guarantee"));
    EXPECT_EQ("4096", props.at("memory_limit"));
    EXPECT_EQ("8192", props.at("anon_limit"));
    EXPECT_EQ("9999", props.at("dirty_limit"));
    EXPECT_EQ("limits.yandex.net", props.at("hostname"));
    EXPECT_EQ("L3 veth", props.at("net"));
}

TEST(BuildPortoProperties, TestSubnetsPortoProperties)
{
    NClient::NApi::NProto::TResourceSpec_TCpuSpec cpuSpec;
    NProto::TPodSpecOther podSpecOther;
    NProto::TPodStatusOther podStatusOther;

    auto* alloc1 = podStatusOther.add_ip6_subnet_allocations();
    alloc1->set_vlan_id("fastbone");
    alloc1->set_subnet("2a02:6b8:fc08:aa7:10c:ba05:0:0/112");

    auto* alloc2 = podStatusOther.add_ip6_subnet_allocations();
    alloc2->set_vlan_id("backbone");
    alloc2->set_subnet("2a02:6b8:c0c:aa7:10c:ba05:1:0/112");

    const auto props = BuildProps(cpuSpec, podSpecOther, podStatusOther);

    EXPECT_EQ("L3 veth", props.at("net"));
    EXPECT_EQ(
        SortedProps("veth 2a02:6b8:fc08:aa7:10c:ba05:0:0/112;veth 2a02:6b8:c0c:aa7:10c:ba05:1:0/112"),
        SortedProps(props.at("ip")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NObjects::NTests
