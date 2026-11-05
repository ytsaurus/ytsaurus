#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/monitoring.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/solomon/exporter.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/json/yson/json2yson.h>

#include <library/cpp/monlib/encode/format.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

//! Test collection grid.
constexpr auto TestGridStep = TDuration::Seconds(1);
constexpr auto CollectionTimeout = TDuration::Seconds(10);
constexpr auto PollPeriod = TDuration::MilliSeconds(10);
constexpr TStringBuf CollectionBarrierSensor = "yt.flow.companion.unittest.collection_barrier";

NCompanion::TCompanionExecutionConfigPtr MakeConfig(int monitoringPort)
{
    auto config = New<NCompanion::TCompanionExecutionConfig>();
    config->MonitoringPort = monitoringPort;
    config->PipelinePath = "//tmp/pipeline";
    config->ClusterUrl = "test-cluster";
    config->Monitoring->GridStep = TestGridStep;
    return config;
}

NHttp::IResponsePtr Get(int port, TStringBuf path, TStringBuf accept)
{
    auto poller = CreateThreadPoolPoller(1, "TestPoller");
    auto client = NHttp::CreateClient(New<NHttp::TClientConfig>(), poller);
    auto headers = New<NHttp::THeaders>();
    headers->Set("Accept", TString(accept));
    auto response = client
        ->Get(Format("http://localhost:%v%v", port, path), headers)
        .BlockingGet()
        .ValueOrThrow();
    poller->Shutdown();
    return response;
}

std::vector<INodePtr> ParseSensors(TStringBuf json)
{
    auto yson = NYson::TYsonString(NJson2Yson::SerializeJsonValueAsYson(NJson::ReadJsonFastTree(json)));
    return ConvertToNode(yson)->AsMap()->GetChildOrThrow("sensors")->AsList()->GetChildren();
}

std::optional<std::string> FindLabel(const INodePtr& sensor, const std::string& name)
{
    return sensor->AsMap()->GetChildOrThrow("labels")->AsMap()->FindChildValue<std::string>(name);
}

void WaitForNextCollection(
    const NProfiling::TSolomonRegistryPtr& registry,
    const NProfiling::TSolomonExporterPtr& exporter)
{
    auto barrier = NProfiling::TProfiler(registry, /*prefix*/ "", "yt.flow.companion")
        .WithProjectionsDisabled()
        .Gauge("/unittest/collection_barrier");
    barrier.Update(1);

    auto deadline = TInstant::Now() + CollectionTimeout;
    while (TInstant::Now() < deadline) {
        if (auto json = exporter->ReadJson()) {
            for (const auto& sensor : ParseSensors(*json)) {
                if (FindLabel(sensor, "sensor") == std::string(CollectionBarrierSensor) &&
                    sensor->AsMap()->GetChildValueOrThrow<double>("value") == 1)
                {
                    return;
                }
            }
        }
        Sleep(PollPeriod);
    }
    THROW_ERROR_EXCEPTION("Timed out waiting for sensor collection");
}

////////////////////////////////////////////////////////////////////////////////

// A zero port disables companion monitoring.
TEST(TCompanionMonitoringTest, DisabledWithoutMonitoringPort)
{
    auto monitoring = New<TCompanionMonitoring>(
        MakeConfig(/*monitoringPort*/ 0),
        New<NProfiling::TSolomonRegistry>());
    EXPECT_FALSE(monitoring->GetSolomonExporter());

    monitoring->Start();
    monitoring->Stop();
}

// A disabled node exporter also disables companion monitoring.
TEST(TCompanionMonitoringTest, DisabledWhenTheNodeExporterIs)
{
    auto port = NTesting::GetFreePort();
    auto config = MakeConfig(static_cast<int>(port));
    config->Monitoring->Enable = false;
    auto monitoring = New<TCompanionMonitoring>(config, New<NProfiling::TSolomonRegistry>());
    EXPECT_FALSE(monitoring->GetSolomonExporter());

    monitoring->Start();
    monitoring->Stop();
}

// Serve the proxy's fixed |/metrics| path.
TEST(TCompanionMonitoringTest, ServesMetrics)
{
    auto port = NTesting::GetFreePort();
    auto registry = New<NProfiling::TSolomonRegistry>();
    auto monitoring = New<TCompanionMonitoring>(MakeConfig(static_cast<int>(port)), registry);
    monitoring->Start();
    auto stopGuard = Finally([&] {
        monitoring->Stop();
    });

    auto counter = NProfiling::TProfiler(registry, "yt.flow.companion").Counter("/monitoring_ut/count");
    counter.Increment();

    WaitForNextCollection(registry, monitoring->GetSolomonExporter());

    {
        auto response = Get(
            static_cast<int>(port),
            "/metrics",
            ::NMonitoring::ContentTypeByFormat(::NMonitoring::EFormat::SPACK));
        EXPECT_EQ(response->GetStatusCode(), NHttp::EStatusCode::OK);
        EXPECT_FALSE(response->ReadAll().Empty());
    }

    // Keep the exporter's native routes.
    {
        auto response = Get(static_cast<int>(port), "/solomon/status", "application/json");
        EXPECT_EQ(response->GetStatusCode(), NHttp::EStatusCode::OK);
    }
}

// Companion sensors inherit node tags but override companion-owned tags.
TEST(TCompanionMonitoringTest, TagsEverySensorAsCompanion)
{
    auto port = NTesting::GetFreePort();
    auto registry = New<NProfiling::TSolomonRegistry>();
    auto config = MakeConfig(static_cast<int>(port));
    config->Monitoring->InstanceTags["deployment"] = "blue";
    config->Monitoring->InstanceTags[std::string(CompanionProcessTag)] = "worker";
    auto monitoring = New<TCompanionMonitoring>(config, registry);
    monitoring->Start();
    auto stopGuard = Finally([&] {
        monitoring->Stop();
    });

    auto counter = NProfiling::TProfiler(registry, "yt.flow.companion").Counter("/monitoring_ut/tagged");
    counter.Increment();

    WaitForNextCollection(registry, monitoring->GetSolomonExporter());

    auto json = monitoring->GetSolomonExporter()->ReadJson();
    ASSERT_TRUE(json);
    auto sensors = ParseSensors(*json);
    ASSERT_FALSE(sensors.empty());

    for (const auto& sensor : sensors) {
        EXPECT_EQ(FindLabel(sensor, "flow_process"), std::optional<std::string>("companion"));
        EXPECT_EQ(FindLabel(sensor, "pipeline_path"), std::optional<std::string>("//tmp/pipeline"));
        EXPECT_EQ(FindLabel(sensor, "pipeline_cluster"), std::optional<std::string>("test-cluster"));
        EXPECT_EQ(FindLabel(sensor, "deployment"), std::optional<std::string>("blue"));
    }
}

// A mismatched grid makes the companion endpoint reject the pull.
TEST(TCompanionMonitoringTest, RejectsPullPeriodOffTheGrid)
{
    auto port = NTesting::GetFreePort();
    auto registry = New<NProfiling::TSolomonRegistry>();
    auto monitoring = New<TCompanionMonitoring>(
        MakeConfig(static_cast<int>(port)),
        registry);
    monitoring->Start();
    auto stopGuard = Finally([&] {
        monitoring->Stop();
    });

    WaitForNextCollection(registry, monitoring->GetSolomonExporter());

    auto response = Get(
        static_cast<int>(port),
        Format("/metrics?period=%vms&now=%v",
        (TestGridStep + TestGridStep / 2).MilliSeconds(),
        TInstant::Now().ToStringUpToSeconds()),
        ::NMonitoring::ContentTypeByFormat(::NMonitoring::EFormat::SPACK));
    EXPECT_NE(response->GetStatusCode(), NHttp::EStatusCode::OK);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
