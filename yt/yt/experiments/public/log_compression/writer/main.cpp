#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <library/cpp/getopt/last_getopt.h>

using namespace NYT;
using namespace NYT::NLogging;
using namespace NYT::NConcurrency;

static const TLogger Logger("Stress");

int main(int argc, const char *argv[])
{
    NLastGetopt::TOpts opts;
    TString configPath;
    int threadCount;
    int eventCount;
    opts.AddLongOption("config")
        .RequiredArgument("filename")
        .StoreResult(&configPath);
    opts.AddLongOption("threads")
        .DefaultValue(8)
        .StoreResult(&threadCount);
    opts.AddLongOption("events")
        .DefaultValue(1'000'000)
        .StoreResult(&eventCount);
    NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);

    auto config = TLogManagerConfig::CreateFromFile(configPath);
    TLogManager::Get()->Configure(config);

    auto pool = CreateThreadPool(threadCount, "MyThread");

    TInstant startTime = TInstant::Now();

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < threadCount; ++i) {
        auto asyncResult = BIND([threadNum = i, eventCount] {
            for (int i = 0; i < eventCount; ++i) {
                YT_LOG_DEBUG("Debug event (Seq: %v, Thread: %v)", i, threadNum);
            }
        })
            .AsyncVia(pool->GetInvoker())
            .Run();
        futures.emplace_back(asyncResult);
    }
    AllSucceeded(futures).Get();

    Cout << "Enqueue time: " << (TInstant::Now() - startTime).MilliSeconds() << " ms" << Endl;

    TLogManager::Get()->Shutdown();
    Cout << "Shutdown time: " << (TInstant::Now() - startTime).MilliSeconds() << " ms" << Endl;

    return 0;
}
