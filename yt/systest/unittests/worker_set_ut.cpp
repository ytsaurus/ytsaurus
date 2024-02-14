
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/systest/worker_set.h>

#include <mutex>
#include <condition_variable>

namespace NYT::NTest {

class TWorkerSetTest : public ::testing::Test
{
protected:
    static TFuture<TWorkerSet::TToken> PickWorker(TWorkerSet* workerSet)
    {
        return workerSet->PickWorker();
    }

    std::vector<TWorkerSet::TToken> Tokens;
};

class TStubFetcherBase
{
public:
    void WaitToRun(int threshold)
    {
        std::unique_lock guard(Mutex_);
        while (NumRuns_ < threshold) {
            CondVar_.wait(guard);
        }
    }

protected:
    TStubFetcherBase()
        : NumRuns_(0)
    {
    }

    // Use thread (not fiber) synchoronization tools here for simplicity.
    std::mutex Mutex_;
    std::condition_variable CondVar_;
    int NumRuns_;
};

class TDeterministicStubFetcher
    : public TStubFetcherBase
{
public:
    TDeterministicStubFetcher()
        : Stopped_(false)
    {
    }

    void Populate(std::vector<TString> values)
    {
        std::unique_lock guard(Mutex_);
        Data_.push_back(std::move(values));
    }

    void Stop()
    {
        std::unique_lock guard(Mutex_);
        YT_VERIFY(!Data_.empty());
        Stopped_ = true;
        CondVar_.notify_all();
    }

    std::vector<TString> Fetch()
    {
        std::unique_lock guard(Mutex_);
        while (NumRuns_ == std::ssize(Data_) && !Stopped_) {
            CondVar_.wait(guard);
        }
        CondVar_.notify_all();
        if (NumRuns_ < std::ssize(Data_)) {
            return Data_[NumRuns_++];
        } else {
            // Stopped_ is true
            return Data_.back();
        }
    }

private:
    bool Stopped_;
    std::vector<std::vector<TString>> Data_;
};

class TRandomStubFetcher
    : public TStubFetcherBase
{
public:
    TRandomStubFetcher(int numNames, int numExclude)
        : NumNames(numNames)
        , NumExclude(numExclude)
        , Engine_(0)
    {
    }

    std::vector<TString> Fetch()
    {
        int seed;
        {
            auto guard = Guard(Lock_);
            seed = Engine_();
        }
        std::mt19937_64 engine(seed);
        std::vector<TString> names;
        names.reserve(NumNames);
        for (int i = 0; i < NumNames; ++i) {
            names.push_back(std::to_string(i));
        }
        std::shuffle(names.begin(), names.end(), engine);
        {
            std::unique_lock guard(Mutex_);
            NumRuns_ += 1;
            CondVar_.notify_all();
        }
        return std::vector<TString>(names.begin(), names.end() - NumExclude);
    }

private:
    const int NumNames;
    const int NumExclude;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::mt19937_64 Engine_;
};

TEST_F(TWorkerSetTest, Waiting)
{
    const int NumThreads = 2;
    const int NumNames = 10;
    const int NumExclude = 0;

    auto pool = NConcurrency::CreateThreadPool(NumThreads, "worker_set_ut");
    TRandomStubFetcher fetcher(NumNames, NumExclude);
    TWorkerSet workerSet(
        pool->GetInvoker(),
        [&fetcher] { return fetcher.Fetch(); },
        TDuration::MicroSeconds(1),
        TDuration::MicroSeconds(1));

    workerSet.Start();

    // If the fetcher was ran two times, the worker set has been assigned at least once.
    fetcher.WaitToRun(2);

    for (int i = 0; i < NumNames; ++i) {
        auto future = TWorkerSetTest::PickWorker(&workerSet);
        EXPECT_TRUE(future.IsSet());
        Tokens.push_back(future.Get().ValueOrThrow());
    }

    auto future = TWorkerSetTest::PickWorker(&workerSet);
    EXPECT_FALSE(future.IsSet());

    Tokens[0].Release();
    auto token = future.Get().ValueOrThrow();
    token.Release();

    for (int i = 1; i < NumNames; i++) {
        Tokens[i].Release();
    }

    workerSet.Stop();
}

TEST_F(TWorkerSetTest, FailureBackoff)
{
    const int NumThreads = 2;
    const int NumNames = 10;
    const int NumExclude = 0;

    auto pool = NConcurrency::CreateThreadPool(NumThreads, "worker_set_ut");
    TRandomStubFetcher fetcher(NumNames, NumExclude);
    TWorkerSet workerSet(
        pool->GetInvoker(),
        [&fetcher] { return fetcher.Fetch(); },
        TDuration::MilliSeconds(1),
        TDuration::MilliSeconds(500));

    workerSet.Start();

    // If the fetcher was ran two times, the worker set has been assigned at least once.
    fetcher.WaitToRun(2);

    for (int i = 0; i < NumNames; ++i) {
        auto future = TWorkerSetTest::PickWorker(&workerSet);
        EXPECT_TRUE(future.IsSet());
        auto token = future.Get().ValueOrThrow();
        token.MarkFailure();
        token.Release();
    }

    auto future = TWorkerSetTest::PickWorker(&workerSet);
    EXPECT_FALSE(future.IsSet());
    auto token = future.Get().ValueOrThrow();
    token.Release();

    workerSet.Stop();
}

TEST_F(TWorkerSetTest, ChangeWorkerSet)
{
    const int NumThreads = 2;
    TDeterministicStubFetcher fetcher;

    auto pool = NConcurrency::CreateThreadPool(NumThreads, "worker_set_ut");
    TWorkerSet workerSet(
        pool->GetInvoker(),
        [&fetcher] { return fetcher.Fetch(); },
        TDuration::MicroSeconds(1),
        TDuration::MicroSeconds(1));

    workerSet.Start();

    std::vector<TString> roundA{{"a", "b", "c"}};
    std::vector<TString> roundB{{"x", "y", "z"}};

    fetcher.Populate(roundA);

    auto token = TWorkerSetTest::PickWorker(&workerSet).Get().ValueOrThrow();
    std::find(roundA.begin(), roundA.end(), token.HostPort());
    token.Release();

    fetcher.Populate(roundB);
    fetcher.WaitToRun(2);

    token = TWorkerSetTest::PickWorker(&workerSet).Get().ValueOrThrow();
    std::find(roundB.begin(), roundB.end(), token.HostPort());
    token.Release();

    fetcher.Stop();

    workerSet.Stop();
}

TEST_F(TWorkerSetTest, ConcurrentWork)
{
    NLogging::TLogger Logger("worker_set");

    const int NumThreads = 16;
    const int NumClients = 20;
    const int NumOperations = 1000;
    const int NumNames = 16;
    const int NumExclude = 8;

    auto pool = NConcurrency::CreateThreadPool(NumThreads, "worker_set_ut");

    TRandomStubFetcher fetcher(NumNames, NumExclude);

    TWorkerSet workerSet(
        pool->GetInvoker(),
        [&fetcher] { return fetcher.Fetch(); },
        TDuration::MicroSeconds(1),
        TDuration::MicroSeconds(5));

    workerSet.Start();

    std::vector<int64_t> numRuns(NumNames, 0);
    std::vector<TFuture<void>> clients;

    for (int i = 0; i < NumClients; i++) {
        auto cb = [&]() {
            for (int i = 0; i < NumOperations; i++) {
                auto token = TWorkerSetTest::PickWorker(&workerSet).Get().ValueOrThrow();
                int num = atoi(token.HostPort().c_str());
                ++numRuns[num];
                if (i % 3 == 0) {
                    token.MarkFailure();
                }
                if (i % 4 == 0) {
                    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MicroSeconds(10));
                }
                token.Release();
            }
        };
        auto clientFuture = BIND(cb).AsyncVia(pool->GetInvoker()).Run();
        clients.push_back(clientFuture);
    }

    AllSucceeded(std::move(clients)).Get().ThrowOnError();

    int64_t totalRuns = 0;
    for (int64_t runs : numRuns) {
        totalRuns += runs;
    }

    YT_LOG_INFO("Completed client runs (TotalRuns: %v)", totalRuns);

    workerSet.Stop();
}

}  // namespace NYT::NTest
