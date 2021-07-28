#include <random>

#include <unistd.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/profiling/resource_tracker.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/ytalloc/bindings.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/solomon/exporter.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#include <util/stream/output.h>
#include <util/system/compiler.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

using namespace NYT;
using namespace NYT::NHttp;
using namespace NYT::NConcurrency;
using namespace NYT::NProfiling;

int main(int argc, char* argv[])
{
    try {
        if (argc != 2 && argc != 3) {
            throw yexception() << "usage: " << argv[0] << " PORT [--fast]";
        }

        auto port = FromString<int>(argv[1]);
        auto fast = TString{"--fast"} == TString{argv[2]};
        auto poller = CreateThreadPoolPoller(1, "Example");
        auto server = CreateServer(port, poller);
        auto actionQueue = New<TActionQueue>("Control");

        auto threadPool = New<TThreadPool>(16, "Pool");

        auto internalShardConfig = New<TShardConfig>();
        internalShardConfig->Filter = {"yt/solomon"};

        auto ytallocShardConfig = New<TShardConfig>();
        ytallocShardConfig->Filter = {"yt/yt_alloc"};

        if (!fast) {
            ytallocShardConfig->GridStep = TDuration::Seconds(30);
        } else {
            ytallocShardConfig->GridStep = TDuration::Seconds(6);
        }

        auto defaultShardConfig = New<TShardConfig>();
        defaultShardConfig->Filter = {""};

        auto config = New<TSolomonExporterConfig>();
        config->Shards = {
            {"internal", internalShardConfig},
            {"ytalloc", ytallocShardConfig},
            {"default", defaultShardConfig},
        };

        if (fast) {
            config->GridStep = TDuration::Seconds(2);
        }

        // Deprecated option. Enabled for testing.
        config->EnableCoreProfilingCompatibility = true;

        // Offload sensor processing to the thread pool.
        config->ThreadPoolSize = 16;

        auto exporter = New<TSolomonExporter>(config, actionQueue->GetInvoker());
        exporter->Register("/solomon", server);
        exporter->Start();

        server->Start();

        NYTAlloc::EnableYTProfiling();
        EnableRefCountedTrackerProfiling();

        TProfiler r{"/my_loop"};

        auto iterationCount = r.WithTag("thread", "main").Counter("/iteration_count");
        auto randomNumber = r.WithTag("thread", "main").Gauge("/random_number");

        auto invalidCounter = r.Counter("/invalid");
        auto invalidGauge = r.Gauge("/invalid");

        auto sparseCounter = r.WithSparse().Counter("/sparse_count");

        auto histogram = r.WithSparse().Histogram(
            "/histogram",
            TDuration::MilliSeconds(1),
            TDuration::Seconds(1));

        auto poolUsage = r.WithTag("pool", "prime").WithGlobal().Gauge("/cpu");
        poolUsage.Update(3000.0);

        auto remoteActionQueue = New<TActionQueue>("Remote");
        for (int i = 0; i < 10; i++) {
            auto remoteRegistry = New<TSolomonRegistry>();
            auto config = New<TSolomonExporterConfig>();
            config->EnableSelfProfiling = false;
            config->ReportRestart = false;

            auto remoteExporter = New<TSolomonExporter>(config, remoteActionQueue->GetInvoker(), remoteRegistry);

            TProfiler r{remoteRegistry, "/remote"};
            r.AddFuncGauge("/value", remoteExporter, [] { return 1.0; });

            exporter->AttachRemoteProcess(BIND([remoteExporter] () -> TFuture<TSharedRef> {
                return MakeFuture(remoteExporter->DumpSensors());
            }));
        }

        exporter->AttachRemoteProcess(BIND([] () -> TFuture<TSharedRef> {
            THROW_ERROR_EXCEPTION("Process is dead");
        }));
        exporter->AttachRemoteProcess(BIND([] () -> TFuture<TSharedRef> {
            return MakeFuture<TSharedRef>(TError("Process is dead"));
        }));

        std::default_random_engine rng;
        double value = 0.0;

        for (i64 i = 0; true; ++i) {
            YT_PROFILE_TIMING("/loop_start") {
                iterationCount.Increment();
                randomNumber.Update(value);
            }
            value += std::uniform_real_distribution<double>(-1, 1)(rng);

            YT_PROFILE_TIMING("/busy_wait") {
                // Busy wait to demonstrate CPU tracker.
                auto endBusyTime = TInstant::Now() + TDuration::MilliSeconds(10);
                while (TInstant::Now() < endBusyTime)
                { }
            }

            histogram.Record(RandomDuration(TDuration::Seconds(1)));

            if (i % 18000 == 0) {
                sparseCounter.Increment();
            }
        }
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        _exit(1);
    }

    return 0;
}
