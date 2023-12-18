#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/new_fair_share_thread_pool.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/signal_registry.h>

#include <library/cpp/yt/threading/count_down_latch.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/system/spinlock.h>

#include <random>

using namespace NYT;
using namespace NConcurrency;
using namespace NThreading;
using namespace NProfiling;

const NLogging::TLogger Logger("Main");


void Spin(TCpuDuration duration)
{
    auto start = GetCpuInstant();
    while (GetCpuInstant() - start < duration) {
        SpinLockPause();
    }
}

void Spin(TDuration duration)
{
    Spin(DurationToCpuDuration(duration));
}

using TCountDownLatchPtr = NYT::TIntrusivePtr<TCountDownLatch>;

struct THistogram
{
    THistogram(int binCount, double startOffset)
    {
        TimeBins_.resize(binCount);

        TimeOffsets_.reserve(TimeBins_.size());
        double offset = startOffset;
        for (int i = 0; i < std::ssize(TimeBins_); ++i, offset *= Ratio) {
            TimeOffsets_.push_back(offset);
        }
        TimeOffsets_.back() = std::numeric_limits<i64>::max();
    }

    void Add(i64 duration)
    {
        i64 position = LowerBound(TimeOffsets_.begin(), TimeOffsets_.end(), duration) - TimeOffsets_.begin();
        ++TimeBins_[std::min(position, std::ssize(TimeBins_) - 1)];
    }

    static std::vector<i64> ComputePercentileValues(
        TRange<i64> timeBins,
        TRange<i64> timeOffsets,
        TRange<double> percentiles)
    {
        i64 shotCount = std::accumulate(timeBins.begin(), timeBins.end(), 0ll);

        std::vector<i64> result;
        i64 count = 0;
        int currentPercentile = 0;

        for (int index = 0;
            index < std::ssize(timeBins) && currentPercentile < std::ssize(percentiles);
            ++index)
        {
            count += timeBins[index];

            if (count >= percentiles[currentPercentile] * shotCount) {
                result.push_back(timeOffsets[index]);
                ++currentPercentile;
            }
        }

        return result;
    }

    static constexpr double Ratio = 1.1;

    std::vector<i64> TimeBins_;
    std::vector<i64> TimeOffsets_;
};

YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, HistogramsLock);
std::vector<THistogram*> Histograms;

struct TThreadHistogram
    : public THistogram
{
    TThreadHistogram()
        : THistogram(128, 1)
    {
        auto guard = Guard(HistogramsLock);
        Histograms.push_back(this);
    }

    static std::vector<i64> ComputePercentileValues(TRange<double> percentiles)
    {
        std::vector<i64> timeBins(128, 0);
        std::vector<i64> timeOffsets;

        auto guard = Guard(HistogramsLock);
        for (auto histogram : Histograms) {
            for (size_t i = 0; i < histogram->TimeBins_.size(); ++i) {
                timeBins[i] += histogram->TimeBins_[i];
            }

            timeOffsets = histogram->TimeOffsets_;
        }

        return THistogram::ComputePercentileValues(timeBins, timeOffsets, percentiles);
    }
};

thread_local TThreadHistogram ThreadHistogram;

struct TTask final
{
    TCallback<IInvoker*(size_t)> InvokerProvider;
    TCountDownLatchPtr Latch;
    std::atomic<size_t> IterationsLeft;
    TCpuDuration SpinCpuDuration;
    TClosure Closure;
    bool WaitTimeHistogram = true;

    TTask(
        TCallback<IInvoker*(size_t)> invokerProvider,
        TCountDownLatchPtr latch,
        size_t iterationCount,
        TDuration spinDuration)
        : InvokerProvider(std::move(invokerProvider))
        , Latch(std::move(latch))
        , IterationsLeft(iterationCount)
        , SpinCpuDuration(DurationToCpuDuration(spinDuration))
    {
        Closure = BIND(&TTask::Run, MakeStrong(this));
    }

    void Invoke(size_t iteration)
    {
        if (WaitTimeHistogram) {
            auto scheduledAt = GetCpuInstant();
            InvokerProvider(iteration)->Invoke(BIND([scheduledAt, this] {
                auto waitTime = CpuDurationToDuration(GetCpuInstant() - scheduledAt);
                ThreadHistogram.Add(waitTime.MicroSeconds());
                Run();
            }));
        } else {
            InvokerProvider(iteration)->Invoke(Closure);
        }
    }

    void Run()
    {
        auto iteration = IterationsLeft--;
        YT_VERIFY(iteration > 0);

        Spin(SpinCpuDuration);

        if (iteration == 1) {
            Closure.Reset();
            Latch->CountDown();
        } else {
            Invoke(iteration);
        }
    }
};

using TTaskPtr = NYT::TIntrusivePtr<TTask>;

std::vector<std::pair<size_t, TDuration>> BuildActionArray(
    std::default_random_engine& engine,
    size_t poolCount,
    size_t iterations)
{
    std::uniform_int_distribution<size_t> uniform(0, poolCount - 1);

    std::vector<std::pair<size_t, TDuration>> randomAction;
    randomAction.reserve(iterations);

    for (size_t index = 0; index < iterations; ++index) {
        auto poolIndex = std::uniform_int_distribution<size_t>(0, poolCount - 1)(engine);
        auto duration = TDuration::MicroSeconds(std::uniform_int_distribution<size_t>(1, 10)(engine));

        randomAction.emplace_back(poolIndex, duration);
    }

    return randomAction;
}

void BenchmarkFSActionQueue(size_t iterations, size_t poolCount, TDuration spinDuration)
{
    YT_VERIFY(poolCount > 0);

    std::vector<TString> poolNames;
    for (size_t index = 0; index < poolCount; ++index) {
        poolNames.push_back(Format("Pool: %v", index));
    }

    auto actionQueue = CreateFairShareActionQueue("Test", poolNames);

    auto actionCount = iterations;

    std::default_random_engine engine;
    auto randomArray = BuildActionArray(engine, poolCount, 1024);

    auto test = [&] {
        auto latch = New<TCountDownLatch>(1);

        auto task = New<TTask>(
            BIND([&] (size_t index) {
                auto [poolIndex, duration] = randomArray[index % 1024];
                return actionQueue->GetInvoker(poolIndex).Get();
            }),
            latch,
            actionCount,
            spinDuration);
        task->Invoke(actionCount);

        latch->Wait();
    };

    // Warmup
    test();

    auto startTime = NProfiling::GetCpuInstant();

    test();

    auto cpuDuration = NProfiling::GetCpuInstant() - startTime;

    actionQueue.Reset();

    auto duration = NProfiling::CpuDurationToDuration(cpuDuration);

    Cout << Format("Fair share action queue (Time: %v, Cycles/Action: %v, Actions/Sec: %v)\n",
        duration,
        cpuDuration / actionCount,
        actionCount / duration.SecondsFloat());
}

// Average queue size is max(taskCount - threadCount, 0)

void BenchmarkThreadPool(size_t iterations, size_t threadCount, size_t taskCount, TDuration spinDuration)
{
    auto threadPool = CreateThreadPool(threadCount, "Test");

    Sleep(TDuration::MilliSeconds(10));

    auto test = [&] (size_t actionCount) {
        auto actionsPerTask = actionCount / taskCount;

        auto latch = New<TCountDownLatch>(taskCount);

        auto startTime = NProfiling::GetCpuInstant();

        for (size_t index = 0; index < taskCount; ++index) {
            auto task = New<TTask>(
                BIND([=] (size_t /*index*/) {
                    return threadPool->GetInvoker().Get();
                }),
                latch,
                actionsPerTask,
                spinDuration);
            task->Invoke(actionsPerTask);
        }

        latch->Wait();

        return NProfiling::GetCpuInstant() - startTime;
    };

    auto actionCount = iterations * std::min(threadCount, taskCount);

    YT_LOG_DEBUG("WARMUP");

    // Warmup
    test(actionCount / 10);

    YT_LOG_DEBUG("TESTING");

    auto cpuDuration = test(actionCount);

    threadPool.Reset();

    auto duration = NProfiling::CpuDurationToDuration(cpuDuration);

    Cout << Format("Moody camel thread pool (Time: %v, Cycles/Action: %v, Actions/Sec: %v)\n",
        duration,
        cpuDuration / actionCount,
        actionCount / duration.SecondsFloat());

    auto times = TThreadHistogram::ComputePercentileValues({0.50, 0.75, 0.90, 0.95, 0.98, 0.99, 0.995, 0.999, 1.0});
    Cout << Format("Percentile times (0.50, 0.75, 0.90, 0.95, 0.98, 0.99, 0.995, 0.999, 1.0): %v", times) << Endl;
}

void BenchmarkFSThreadPool(
    size_t iterations,
    size_t threadCount,
    size_t taskCount,
    size_t poolCount,
    TDuration spinDuration,
    bool newPool)
{
    auto threadPool = newPool
        ? CreateNewTwoLevelFairShareThreadPool(threadCount, "Test")
        : CreateTwoLevelFairShareThreadPool(threadCount, "Test");

    Sleep(TDuration::MilliSeconds(10));

    auto test = [&] (size_t actionCount) {

        auto actionsPerTask = actionCount / taskCount;

        auto latch = New<TCountDownLatch>(taskCount);

        std::vector<TTaskPtr> tasks;
        std::vector<IInvokerPtr> invokers;

        for (size_t index = 0; index < poolCount; ++index) {
            invokers.push_back(threadPool->GetInvoker("", Format("Pool%v", index)));
        }

        std::default_random_engine engine;

        for (size_t index = 0; index < taskCount; ++index) {
            auto randomArray = BuildActionArray(engine, poolCount, 1024);

            tasks.push_back(New<TTask>(
                BIND([=, randomArray = std::move(randomArray)] (size_t iteration) {
                    Y_UNUSED(randomArray);
                    Y_UNUSED(iteration);

                    return invokers[randomArray[iteration % 1024].first].Get();
                    //return invokers[index];
                }),
                latch,
                actionsPerTask,
                spinDuration));
        }

        auto startTime = NProfiling::GetCpuInstant();
        for (const auto& task : tasks) {
            task->Invoke(actionsPerTask);
        }

        latch->Wait();

        return NProfiling::GetCpuInstant() - startTime;
    };

    auto actionCount = iterations * std::min(threadCount, taskCount);

    YT_LOG_DEBUG("WARMUP");

    // Warmup
    test(actionCount / 10);

    YT_LOG_DEBUG("TESTING");

    auto cpuDuration = test(actionCount);

    auto duration = NProfiling::CpuDurationToDuration(cpuDuration);

    Cout << Format("Fair share thread pool (Time: %v, Cycles/Action: %v, Actions/Sec: %v, FC: %v)\n",
        duration,
        cpuDuration / actionCount,
        actionCount / duration.SecondsFloat(),
        newPool);

    auto times = TThreadHistogram::ComputePercentileValues({0.50, 0.75, 0.90, 0.95, 0.98, 0.99, 0.995, 0.999, 1.0});
    Cout << Format("Percentile times (0.50, 0.75, 0.90, 0.95, 0.98, 0.99, 0.995, 0.999, 1.0): %v", times) << Endl;

    threadPool.Reset();
}

int main(int argc, char** argv)
{
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, CrashSignalHandler);
    TSignalRegistry::Get()->PushDefaultSignalHandler(AllCrashSignals);

    bool enableThreadPool = false;
    bool enableFSThreadPool = false;
    bool enableNewFSThreadPool = false;

    int iterations = 1000000;
    int threadCount = 1;
    int taskCount = 8;
    int poolCount = 4;
    int spinDurationMcs = 0;


    {
        auto opts = NLastGetopt::TOpts::Default();

        opts.SetTitle("Benchmark invokers");
        opts.AddLongOption("tp", "Benchmark thread pool")
            .StoreTrue(&enableThreadPool);
        opts.AddLongOption("fs", "Benchmark fair share thread pool")
            .StoreTrue(&enableFSThreadPool);
        opts.AddLongOption("nfs", "Benchmark new fair share thread pool")
            .StoreTrue(&enableNewFSThreadPool);
        opts.AddLongOption("threads", "Thread count")
            .StoreResult(&threadCount);
        opts.AddLongOption("tasks", "Task count")
            .StoreResult(&taskCount);
        opts.AddLongOption("pools", "Pool count")
            .StoreResult(&poolCount);
        opts.AddLongOption("iterations", "Iteration count")
            .StoreResult(&iterations);
        opts.AddLongOption("spin", "Spin duration in mcs")
            .StoreResult(&spinDurationMcs);

        NLastGetopt::TOptsParseResult args(&opts, argc, argv);
    }

    NYT::NLogging::TLogManager::Get()->ConfigureFromEnv();


    auto spinDuration = TDuration::MicroSeconds(spinDurationMcs);

    //BenchmarkFSActionQueue(10);

    if (enableThreadPool) {
        BenchmarkThreadPool(iterations, threadCount, taskCount, spinDuration);
    }

    if (enableFSThreadPool) {
        BenchmarkFSThreadPool(iterations, threadCount, taskCount, poolCount, spinDuration, false);
    }

    if (enableNewFSThreadPool) {
        BenchmarkFSThreadPool(iterations, threadCount, taskCount, poolCount, spinDuration, true);
    }

    return 0;
}

