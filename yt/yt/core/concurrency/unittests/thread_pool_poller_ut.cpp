#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <thread>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TPollableMock
    : public IPollable
{
public:
    virtual void SetCookie(IPollable::TCookiePtr cookie) override
    {
        Cookie_ = std::move(cookie);
    }

    virtual void* GetCookie() const override
    {
        return static_cast<void*>(Cookie_.Get());
    }

    virtual const TString& GetLoggingTag() const override
    {
        return LoggingTag_;
    }

    virtual void OnEvent(EPollControl control) override
    {
        // NB: Retry is the only event we trigger in this unittest via |IPoller::Retry|.
        EXPECT_EQ(control, EPollControl::Retry);
        EXPECT_FALSE(ShutdownPromise_.IsSet());
        RetryPromise_.Set();
    }

    virtual void OnShutdown() override
    {
        ShutdownPromise_.Set();
    }

    TFuture<void> GetRetryFuture() const
    {
        return RetryPromise_;
    }

    TFuture<void> GetShutdownFuture() const
    {
        return ShutdownPromise_;
    }

private:
    const TString LoggingTag_;

    const TPromise<void> RetryPromise_ = NewPromise<void>();
    const TPromise<void> ShutdownPromise_ = NewPromise<void>();

    IPollable::TCookiePtr Cookie_;
};

////////////////////////////////////////////////////////////////////////////////

class TThreadPoolPollerTest
    : public ::testing::Test
{
public:
    virtual void SetUp() override
    {
        Poller = CreateThreadPoolPoller(InitialThreadCount, "TestPoller");
    }

    virtual void TearDown() override
    {
        Poller->Shutdown();
    }

    template <class T>
    void ExpectSuccessfullySetFuture(const TFuture<T>& future)
    {
        EXPECT_TRUE(future.WithTimeout(TDuration::Seconds(5)).Get().IsOK());
    }

protected:
    const int InitialThreadCount = 4;

    IPollerPtr Poller;
};

TEST_F(TThreadPoolPollerTest, SimplePollable)
{
    auto pollable = New<TPollableMock>();
    EXPECT_TRUE(Poller->TryRegister(pollable));
    Poller->Retry(pollable);

    ExpectSuccessfullySetFuture(pollable->GetRetryFuture());

    std::vector<TFuture<void>> futures{
        Poller->Unregister(pollable),
        pollable->GetShutdownFuture()
    };
    ExpectSuccessfullySetFuture(AllSucceeded(futures));
}

TEST_F(TThreadPoolPollerTest, SimpleCallback)
{
    TPromise<void> promise = NewPromise<void>();
    auto callback = BIND([=] { promise.Set(); });

    Poller->GetInvoker()->Invoke(callback);

    ExpectSuccessfullySetFuture(promise.ToFuture());
}

TEST_F(TThreadPoolPollerTest, SimpleReconfigure)
{
    auto pollable = New<TPollableMock>();
    EXPECT_TRUE(Poller->TryRegister(pollable));

    Poller->Reconfigure(InitialThreadCount * 2);

    std::vector<TFuture<void>> futures{
        Poller->Unregister(pollable),
        pollable->GetShutdownFuture()
    };
    ExpectSuccessfullySetFuture(AllSucceeded(futures));
}

TEST_F(TThreadPoolPollerTest, Stress)
{
    std::vector<std::thread> threads;

    std::vector<std::thread> auxThreads;
    auxThreads.emplace_back([&] {
        for (int i = 0; i < 10; ++i) {
            threads.emplace_back([&] {
                std::vector<TIntrusivePtr<TPollableMock>> pollables;
                for (int j = 0; j < 20000; ++j) {
                    pollables.push_back(New<TPollableMock>());
                    EXPECT_TRUE(Poller->TryRegister(pollables.back()));
                }

                Sleep(TDuration::MicroSeconds(1));

                std::vector<TFuture<void>> retryFutures;
                std::vector<TFuture<void>> unregisterFutures;
                for (int j = 0; j < 20000; j += 2) {
                    Poller->Retry(pollables[j]);
                    retryFutures.push_back(pollables[j]->GetRetryFuture());

                    Poller->Retry(pollables[j + 1]);
                    std::vector<TFuture<void>> futures{
                        Poller->Unregister(pollables[j + 1]),
                        pollables[j + 1]->GetShutdownFuture()
                    };
                    unregisterFutures.push_back(AllSucceeded(futures));
                }

                ExpectSuccessfullySetFuture(AllSucceeded(retryFutures));
                ExpectSuccessfullySetFuture(AllSucceeded(unregisterFutures));
            });

            Sleep(TDuration::MicroSeconds(1));
        }
    });
    auxThreads.emplace_back([&] {
        for (int j = 0; j < 10; ++j) {
            for (int i = 1, sign = -1; i < 10; ++i, sign *= -1) {
                Poller->Reconfigure(InitialThreadCount + sign * i);

            }
            Sleep(TDuration::MicroSeconds(1));
        }
    });

    for (auto& thread : auxThreads) {
        thread.join();
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
