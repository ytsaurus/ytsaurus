#include <gtest/gtest.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

namespace NYT::NProfiling {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TSolomonExporter, MemoryLeak)
{
    auto config = New<TSolomonExporterConfig>();
    config->EnableCoreProfilingCompatibility = true;

    auto actionQueue = New<TActionQueue>("Leak");

    auto exporter = NYT::New<TSolomonExporter>(config, actionQueue->GetInvoker());
    exporter->Start();

    Sleep(TDuration::Seconds(5));

    exporter->Stop();

    actionQueue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
