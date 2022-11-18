#include "yt/yt/library/profiling/solomon/registry.h"
#include <gtest/gtest.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

namespace NYT::NProfiling {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TSolomonExporter, MemoryLeak)
{
    auto registry = New<TSolomonRegistry>();
    auto counter = TProfiler{registry, "yt"}.Counter("/foo");

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;

    auto exporter = New<TSolomonExporter>(config, registry);
    auto json = exporter->ReadJson();
    EXPECT_FALSE(json);

    exporter->Start();

    Sleep(TDuration::Seconds(5));

    json = exporter->ReadJson();
    EXPECT_TRUE(json);
    EXPECT_FALSE(json->empty());

    exporter->Stop();
}

TEST(TSolomonExporter, ReadJsonHistogram)
{
    auto registry = New<TSolomonRegistry>();
    auto hist = TProfiler{registry, "yt"}.Histogram("/foo", TDuration::MilliSeconds(1), TDuration::Seconds(1));

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;

    auto exporter = NYT::New<TSolomonExporter>(config, registry);
    auto json = exporter->ReadJson();
    EXPECT_FALSE(json);

    exporter->Start();

    hist.Record(TDuration::MilliSeconds(500));
    hist.Record(TDuration::MilliSeconds(500));
    hist.Record(TDuration::MilliSeconds(500));
    Sleep(TDuration::Seconds(5));

    json = exporter->ReadJson();
    ASSERT_TRUE(json);
    Cerr << *json;

    exporter->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
