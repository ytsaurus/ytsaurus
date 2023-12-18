#include "driver.h"
#include "driver_factory.h"
#include "worker.h"

#include <yt/yt/core/profiling/timing.h>

#include <util/system/thread.h>

#include <util/random/random.h>

namespace NYT::NIOTest {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TWorker
    : public IWorker
{
public:
    explicit TWorker(TConfigurationPtr configuration, int threadIndex)
        : WorkerThread_(std::move(configuration), threadIndex)
    { }

    void Run() override
    {
        WorkerThread_.Start();
    }

    void Join() override
    {
        WorkerThread_.Join();
    }

    TStatistics Statistics() override
    {
        return WorkerThread_.Statistics();
    }

private:
    class TWorkerThread
        : public TThread
    {
    public:
        TWorkerThread(TConfigurationPtr configuration, int threadIndex)
            : TThread(ThreadMain, (void*) this)
            , Configuration_(std::move(configuration))
            , ThreadIndex_(threadIndex)
        { }

        void ThreadMain()
        {
            // TODO(savrus) Set affinity.

            SetRandomSeed(GetCpuInstant());

            Driver_ = CreateDriver(Configuration_);
            auto operationGenerator = CreateOperationGenerator(Configuration_, ThreadIndex_);

            if (Configuration_->Oneshot) {
                Driver_->Oneshot(std::move(operationGenerator));
            } else {
                Driver_->Burst(std::move(operationGenerator));
            }
        }

        TStatistics Statistics()
        {
            return Driver_->GetStatistics();
        }

    private:
        TConfigurationPtr Configuration_;
        int ThreadIndex_;
        IDriverPtr Driver_;

        static void* ThreadMain(void* opaque)
        {
            static_cast<TWorkerThread*>(opaque)->ThreadMain();
            return nullptr;
        }
    };

    TWorkerThread WorkerThread_;
};

////////////////////////////////////////////////////////////////////////////////

IWorkerPtr CreateWorker(TConfigurationPtr configuration, int threadIndex)
{
    return New<TWorker>(std::move(configuration), threadIndex);
}

////////////////////////////////////////////////////////////////////////////////

class TRusageWatcher
    : public IWatcher
{
public:
    explicit TRusageWatcher(TInstant start)
        : WatcherThread_(start)
    { }

    void Run() override
    {
        WatcherThread_.Start();
    }

    void Join() override
    {
        WatcherThread_.Join();
    }

    void Stop() override
    {
        WatcherThread_.Stop();
    }

    TStatistics Statistics() override
    {
        return WatcherThread_.Statistics();
    }

private:
    class TWatcherThread
        : public TThread
    {
    public:
        TWatcherThread(TInstant start)
            : TThread(ThreadMain, (void*) this)
            , Statistics_(start)
        { }

        void Stop()
        {
            Stop_ = true;
        }

        void ThreadMain()
        {
            TRusageMeter processMeter(ERusageWho::Process);

            do {
                PopulatePollerMeters();
                Statistics_.UpdateRusage(processMeter.Tick(), PollerMetersTick());
                SleepUntil(Now() + WatcherInterval_);
            } while(!Stop_);
        }

        TStatistics Statistics()
        {
            return Statistics_;
        }

    private:
        TStatistics Statistics_;
        std::atomic<bool> Stop_ = {false};
        std::vector<TProcessRusageMeter> PollerMeters_;
        THashSet<TProcessId> RegisteredProcesses_;

        static void* ThreadMain(void* opaque)
        {
            static_cast<TWatcherThread*>(opaque)->ThreadMain();
            return nullptr;
        }

        void PopulatePollerMeters()
        {
            auto processes = FindProcessIds("io_uring-sq");
            for (const auto& process : processes) {
                if (!RegisteredProcesses_.contains(process)) {
                    PollerMeters_.emplace_back(process);
                    RegisteredProcesses_.insert(process);
                }
            }
        }

        TRusage PollerMetersTick()
        {
            TRusage pollerRusage;
            for (auto& meter : PollerMeters_) {
                pollerRusage += meter.Tick();
            }
            return pollerRusage;
        }
    };

    TWatcherThread WatcherThread_;
    static constexpr TDuration WatcherInterval_ = TDuration::Seconds(1);
};

////////////////////////////////////////////////////////////////////////////////

IWatcherPtr CreateRusageWatcher(TInstant start)
{
    return New<TRusageWatcher>(start);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
