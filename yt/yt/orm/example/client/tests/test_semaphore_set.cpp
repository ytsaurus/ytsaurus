#include "common.h"

#include <yt/yt/orm/client/native/semaphore_guard.h>

#include <util/thread/pool.h>

namespace NYT::NOrm::NExample::NClient::NTests {

using NOrm::NClient::NNative::TSemaphoreSetGuard;

////////////////////////////////////////////////////////////////////////////////

class TSemaphoreSetClientTestSuite
    : public TNativeClientTestSuite
{
public:
    void SetUp() override
    {
        TNativeClientTestSuite::SetUp();
        WaitFor(
            Client_->CreateObject(
                TObjectTypeValues::SemaphoreSet,
                BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                    NYTree::BuildYsonFluently(consumer)
                        .BeginMap()
                            .Item("meta")
                                .BeginMap()
                                    .Item("id").Value(SemaphoreSetId_)
                                .EndMap()
                        .EndMap();
                })),
                TCreateObjectOptions{
                    .Format = EPayloadFormat::Protobuf,
                }))
            .ValueOrThrow();
        for (int i = 0; i < SemaphoreCount_; ++i) {
            WaitFor(
                Client_->CreateObject(
                    TObjectTypeValues::Semaphore,
                    BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                        NYTree::BuildYsonFluently(consumer)
                            .BeginMap()
                                .Item("meta")
                                    .BeginMap()
                                        .Item("semaphore_set_id").Value(SemaphoreSetId_)
                                    .EndMap()
                                .Item("spec")
                                    .BeginMap()
                                        .Item("budget").Value(Budget_)
                                    .EndMap()
                            .EndMap();
                    })),
                    TCreateObjectOptions{
                        .Format = EPayloadFormat::Protobuf,
                    }))
                .ValueOrThrow();
        }
    }

protected:
    const TString SemaphoreSetId_{testing::UnitTest::GetInstance()->current_test_info()->name()};
    const int Budget_{2};
    const int SemaphoreCount_{3};
};

TEST_F(TSemaphoreSetClientTestSuite, TestMutualExclusion)
{
    TSemaphoreGuardOptions semaphoreGuardOptions{
        TSemaphoreGuardOptions::TAcquireOptions{
            .RetryOptions = TRetryOptions()
                .WithCount(100)
                .WithSleep(TDuration::MilliSeconds(500)),
            .LeaseDuration = TDuration::Seconds(100),
        },
        std::nullopt,
        TSemaphoreGuardOptions::TReleaseOptions{
            .RetryOptions = TRetryOptions()
                .WithCount(10)
                .WithSleep(TDuration::MilliSeconds(500)),
        },
    };
    std::atomic<int> concurrentAccessCount{0};

    ::TThreadPool pool{TThreadPoolParams{}.SetCatching(false)};
    pool.Start(Budget_ * SemaphoreCount_ * 2);

    for (int i = 0; i < Budget_ * SemaphoreCount_ * 4; ++i) {
        EXPECT_EQ(true, pool.AddFunc([this, &semaphoreGuardOptions, &concurrentAccessCount] {
            auto guard = New<NOrm::NClient::NNative::TSemaphoreSetGuard>(Client_, SemaphoreSetId_, semaphoreGuardOptions);
            ++concurrentAccessCount;
            Sleep(TDuration::Seconds(1));
            EXPECT_LE(concurrentAccessCount, Budget_ * SemaphoreCount_);
            EXPECT_TRUE(guard->IsAcquired());
            --concurrentAccessCount;
        }));
    }

    pool.Stop();
    EXPECT_EQ(0, concurrentAccessCount);
}

TEST_F(TSemaphoreSetClientTestSuite, TestPingedLease)
{
    TSemaphoreGuardOptions semaphoreGuardOptions{
        TSemaphoreGuardOptions::TAcquireOptions{
            .RetryOptions = TRetryOptions()
                .WithCount(100)
                .WithSleep(TDuration::MilliSeconds(500)),
            .LeaseDuration = TDuration::Seconds(100),
        },
        std::nullopt,
        TSemaphoreGuardOptions::TReleaseOptions{
            .RetryOptions = TRetryOptions()
                .WithCount(10)
                .WithSleep(TDuration::MilliSeconds(500)),
        },
    };
    ::TThreadPool pool{TThreadPoolParams{}.SetCatching(false)};
    pool.Start(2);

    for (int i = 0; i < Budget_ * SemaphoreCount_; ++i) {
        EXPECT_EQ(true, pool.AddFunc([this, &semaphoreGuardOptions] {
            auto guard = New<NOrm::NClient::NNative::TSemaphoreSetGuard>(Client_, SemaphoreSetId_, semaphoreGuardOptions);
            for (int j = 0; j < 3; ++j) {
                Sleep(TDuration::Seconds(1));
                EXPECT_EQ(true, guard->IsAcquired());
            }
        }));
    }

    pool.Stop();
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NTests
