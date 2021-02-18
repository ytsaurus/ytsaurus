#include "yt/library/profiling/solomon/registry.h"
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
    auto counter = TRegistry{registry, "yt"}.Counter("/foo");

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;

    auto actionQueue = New<TActionQueue>("Leak");

    auto exporter = NYT::New<TSolomonExporter>(config, actionQueue->GetInvoker(), registry);
    auto json = exporter->ReadJson();
    EXPECT_FALSE(json);

    exporter->Start();

    Sleep(TDuration::Seconds(5));

    json = exporter->ReadJson();
    EXPECT_TRUE(json);
    EXPECT_FALSE(json->empty());

    exporter->Stop();

    actionQueue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
