#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/runner/endpoint_provider.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowEndpointProviderTest, ComponentName)
{
    auto provider = New<TFlowEndpointProvider>(12345, 54321);
    EXPECT_EQ(TString("flow_node"), provider->GetComponentName());
}

TEST(TFlowEndpointProviderTest, Endpoints)
{
    auto provider = New<TFlowEndpointProvider>(12345, 54321);
    auto endpoints = provider->GetEndpoints();
    ASSERT_EQ(2u, endpoints.size());
    EXPECT_EQ(TString("flow_node"), endpoints[0].Name);
    EXPECT_EQ(TString("http://localhost:12345/solomon/all"), endpoints[0].Address);
    EXPECT_EQ(TString("companion"), endpoints[1].Name);
    EXPECT_EQ(TString("http://localhost:54321/metrics"), endpoints[1].Address);
}

TEST(TFlowEndpointProviderTest, SkipsCompanionEndpointWithoutMonitoringPort)
{
    auto provider = New<TFlowEndpointProvider>(12345, 0);
    auto endpoints = provider->GetEndpoints();
    ASSERT_EQ(1u, endpoints.size());
    EXPECT_EQ(TString("flow_node"), endpoints[0].Name);
    EXPECT_EQ(TString("http://localhost:12345/solomon/all"), endpoints[0].Address);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
