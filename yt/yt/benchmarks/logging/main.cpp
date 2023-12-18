#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <util/system/thread.h>

#include <iostream>

using namespace NYT;

NLogging::TLogger Logger("TestLogger");

struct TLoggingBenchmark
{
    std::atomic<bool> Stopped = {false};
    std::atomic<size_t> MessageCount = 0;

    void RunLogging()
    {
        size_t messageCount = 0;
        while (!Stopped) {
            for (size_t index = 0; index < 100; ++index) {
                YT_LOG_INFO("Starting access statistics commit for nodes");
                ++messageCount;
            }
        }

        MessageCount += messageCount;
    }
};

int main(int /*argc*/, char** /*argv*/)
{
    auto config = NLogging::TLogManagerConfig::CreateFromFile("config.yson");
    NLogging::TLogManager::Get()->Configure(config);

    TLoggingBenchmark benchmark;

    constexpr int N = 10;
    std::unique_ptr<TThread> threads[N];

    for (size_t index = 0; index < N; ++index) {
        threads[index] = std::make_unique<TThread>([] (void* opaque) -> void* {
            auto benchmark = static_cast<TLoggingBenchmark*>(opaque);
            //NLogging::TLogManager::Get()->SetPerThreadBatchingPeriod(TDuration::MilliSeconds(100));
            benchmark->RunLogging();
            return nullptr;
        }, &benchmark);
    }

    for (size_t index = 0; index < N; ++index) {
        threads[index]->Start();
    }

    Sleep(TDuration::Seconds(2));
    benchmark.Stopped.store(true);

    for (size_t index = 0; index < N; ++index) {
        threads[index]->Join();
    }

    NLogging::TLogManager::Get()->Synchronize();

    Cout << Format("Logged messages: %v", benchmark.MessageCount.load()) << Endl;

    return 0;
}

