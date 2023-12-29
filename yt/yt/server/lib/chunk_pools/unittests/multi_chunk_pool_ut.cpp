#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/chunk_pools/mock/chunk_pool.h>

#include <yt/yt/server/scheduler/public.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/input_chunk_mapping.h>
#include <yt/yt/server/lib/chunk_pools/multi_chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/progress_counter.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe_key.h>

#include <yt/yt/core/logging/log.h>

#include <random>

namespace NYT::NChunkPools {
namespace {

using ::testing::InSequence;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;
using ::testing::Test;
using ::testing::_;

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkPoolTestBase
    : public Test
{
protected:
    TMultiChunkPoolTestBase()
    {
        Stripes_.reserve(100);
        for (int i = 0; i < 100; i++) {
            Stripes_.push_back(New<TChunkStripe>());
        }
    }

    std::vector<TChunkStripePtr> Stripes_;
};

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkPoolInputTest
    : public TMultiChunkPoolTestBase
{
protected:
    TMultiChunkPoolInputTest()
    {
        constexpr int UnderlyingPoolCount = 10;
        std::vector<IPersistentChunkPoolInputPtr> mockPtrs;
        Mocks_.reserve(UnderlyingPoolCount);
        mockPtrs.reserve(UnderlyingPoolCount);
        for (int poolIndex = 0; poolIndex < UnderlyingPoolCount; ++poolIndex) {
            Mocks_.push_back(New<TChunkPoolInputMock>());
            mockPtrs.push_back(Mocks_.back());
        }

        Pool_ = CreateMultiChunkPoolInput(mockPtrs);
    }

    std::vector<TIntrusivePtr<TChunkPoolInputMock>> Mocks_;
    IMultiChunkPoolInputPtr Pool_;
};

TEST_F(TMultiChunkPoolInputTest, TestAdd)
{
    EXPECT_CALL(*Mocks_[0], Add(Stripes_[0]))
        .WillOnce(Return(42));

    Stripes_[0]->PartitionTag = 0;

    EXPECT_EQ(Pool_->Add(Stripes_[0]), 0);
}

TEST_F(TMultiChunkPoolInputTest, TestAddWithKey)
{
    TChunkStripeKey key;

    EXPECT_CALL(*Mocks_[0], AddWithKey(Stripes_[0], key))
        .WillOnce(Return(42));

    Stripes_[0]->PartitionTag = 0;

    EXPECT_EQ(Pool_->AddWithKey(Stripes_[0], key), 0);
}

TEST_F(TMultiChunkPoolInputTest, TestSuspend)
{
    InSequence sequence;
    EXPECT_CALL(*Mocks_[0], Add(Stripes_[0]))
        .WillOnce(Return(42));
    EXPECT_CALL(*Mocks_[0], Suspend(42))
        .Times(1);

    Stripes_[0]->PartitionTag = 0;

    EXPECT_EQ(Pool_->Add(Stripes_[0]), 0);
    Pool_->Suspend(0);
}

TEST_F(TMultiChunkPoolInputTest, TestResume)
{
    InSequence sequence;
    EXPECT_CALL(*Mocks_[0], Add(Stripes_[0]))
        .WillOnce(Return(42));
    EXPECT_CALL(*Mocks_[0], Resume(42))
        .Times(1);

    Stripes_[0]->PartitionTag = 0;

    EXPECT_EQ(Pool_->Add(Stripes_[0]), 0);
    Pool_->Resume(0);
}

TEST_F(TMultiChunkPoolInputTest, TestReset)
{
    NLogging::TLogger logger("InputChunkMapping");
    auto mapping = New<TInputChunkMapping>(EChunkMappingMode::Sorted, logger);

    InSequence sequence;
    EXPECT_CALL(*Mocks_[0], Add(Stripes_[0]))
        .WillOnce(Return(42));
    EXPECT_CALL(*Mocks_[0], Reset(42, Stripes_[1], mapping))
        .Times(1);

    Stripes_[0]->PartitionTag = 0;
    Stripes_[1]->PartitionTag = 0;

    EXPECT_EQ(Pool_->Add(Stripes_[0]), 0);
    Pool_->Reset(0, Stripes_[1], mapping);
}

TEST_F(TMultiChunkPoolInputTest, TestFinish)
{
    for (auto& mock : Mocks_) {
        EXPECT_CALL(*mock, Finish())
            .Times(1);
    }

    EXPECT_FALSE(Pool_->IsFinished());
    Pool_->Finish();
    EXPECT_TRUE(Pool_->IsFinished());
}

TEST_F(TMultiChunkPoolInputTest, TestFinishPool)
{
    std::vector<int> finishPermutation = {3, 5, 2, 1, 6, 0};

    InSequence sequence;
    for (auto poolIndex : finishPermutation) {
        EXPECT_CALL(*Mocks_[poolIndex], Finish())
            .Times(1);
    }
    for (auto poolIndex : finishPermutation) {
        Pool_->FinishPool(poolIndex);
    }
    EXPECT_FALSE(Pool_->IsFinished());
}

TEST_F(TMultiChunkPoolInputTest, TestPartitionTag)
{
    const std::vector<int> partitions = {0, 1, 3, 2, 1, 0, 2};

    InSequence sequence;
    for (int index = 0; index < std::ssize(partitions); ++index) {
        EXPECT_CALL(*Mocks_[partitions[index]], Add(Stripes_[index]))
            .WillOnce(Return(42));
    }

    for (int index = 0; index < std::ssize(partitions); ++index) {
        Stripes_[index]->PartitionTag = partitions[index];
        EXPECT_EQ(Pool_->Add(Stripes_[index]), index);
    }
}

TEST_F(TMultiChunkPoolInputTest, TestCookieMapping)
{
    // In this test we add stripes one by one and after each
    // addition check external cookie to cookie mapping using suspend call.

    // (pool, cookie)
    const std::vector<std::pair<int, int>> cookies = {
        {0, 0},
        {0, 1},
        {1, 0},
        {2, 0},
        {0, 42},
        {8, 123},
        {2, 2}
    };

    for (int i = 0; i < std::ssize(cookies); i++) {
        auto [pool, cookie] = cookies[i];
        EXPECT_CALL(*Mocks_[pool], Add(Stripes_[i]))
            .WillOnce(Return(cookie));
        EXPECT_CALL(*Mocks_[pool], Suspend(cookie))
            .Times(cookies.size() - i);
    }

    for (int i = 0; i < std::ssize(cookies); i++) {
        auto [pool, cookie] = cookies[i];
        Stripes_[i]->PartitionTag = pool;
        EXPECT_EQ(Pool_->Add(Stripes_[i]), i);
        for (int j = 0; j <= i; j++) {
            Pool_->Suspend(j);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// This suite contains trivial scenarios for checking methods general correctness.
// For advanced scenarios look into TSortedChunkPoolTestRandomized.
// NB: Some tests here heavily rely on the order of underlying pools in multipool which is
// not important in practice.
class TMultiChunkPoolOutputTest
    : public TMultiChunkPoolTestBase
{
protected:
    TMultiChunkPoolOutputTest() = default;

    void InitPools(
        std::vector<int> stripeCounts,
        bool finalize = true,
        std::optional<int> poolsToAdd = std::nullopt)
    {
        Mocks_.reserve(stripeCounts.size());
        for (int poolIndex = 0; poolIndex < std::ssize(stripeCounts); ++poolIndex) {
            Mocks_.push_back(New<TChunkPoolOutputMock>());
            Mocks_.back()->JobCounter->AddPending(stripeCounts[poolIndex]);
        }

        StripeCounts_ = stripeCounts;

        for (int index = 0; index < std::ssize(Mocks_); ++index) {
            if (stripeCounts[index]) {
                EXPECT_CALL(*Mocks_[index], Extract(NNodeTrackerClient::TNodeId(0)))
                    .Times(stripeCounts[index])
                    .WillRepeatedly(InvokeWithoutArgs([this, index] {
                        auto& mock = Mocks_[index];
                        auto& jobCounter = mock->JobCounter;
                        auto cookie = StripeCounts_[index] - jobCounter->GetPending();
                        jobCounter->AddPending(-1);
                        if (jobCounter->GetPending() == 0) {
                            Mocks_[index]->Complete();
                        }
                        return cookie;
                    }));
            }
        }

        for (int index = 0; index < std::ssize(Mocks_); ++index) {
            EXPECT_CALL(*Mocks_[index], IsCompleted())
                .WillRepeatedly(InvokeWithoutArgs([this, index] {
                    return Mocks_[index]->JobCounter->GetPending() == 0;
                }));
            EXPECT_CALL(*Mocks_[index], GetStripeList(_))
                .WillRepeatedly(InvokeWithoutArgs([] {
                    return New<TChunkStripeList>();
                }));
        }

        CreatePool(poolsToAdd.value_or(Mocks_.size()));

        if (finalize) {
            Pool_->Finalize();
        }
    }

    void CreatePool(int poolsToAdd)
    {
        std::vector<IPersistentChunkPoolOutputPtr> mockPtrs;
        mockPtrs.reserve(poolsToAdd);
        for (int poolIndex = 0; poolIndex < poolsToAdd; ++poolIndex) {
            const auto& mock = Mocks_[poolIndex];
            if (poolIndex < poolsToAdd) {
                mockPtrs.push_back(mock);
            }
            // Multi chunk pool checks that underlying pool does not have
            // output order during initialization.
            EXPECT_CALL(*mock, GetOutputOrder())
                .WillOnce(Return(nullptr));
        }

        Pool_ = CreateMultiChunkPoolOutput(mockPtrs);
    }

    std::vector<TIntrusivePtr<TChunkPoolOutputMock>> Mocks_;
    IMultiChunkPoolOutputPtr Pool_;
    std::vector<int> StripeCounts_;
};

TEST_F(TMultiChunkPoolOutputTest, TestExtract)
{
    InitPools({3, 2, 1, 4});
    int cookieCount = 0;
    while (!Pool_->IsCompleted()) {
        EXPECT_EQ(Pool_->Extract(), cookieCount);
        cookieCount++;
    }

    EXPECT_EQ(cookieCount, 10);
}

TEST_F(TMultiChunkPoolOutputTest, TestEmptyPools1)
{
    InitPools({0, 0, 1, 2, 0, 3, 0});
    int cookieCount = 0;
    while (!Pool_->IsCompleted()) {
        EXPECT_EQ(Pool_->Extract(), cookieCount);
        cookieCount++;
    }

    EXPECT_EQ(cookieCount, 6);
}

TEST_F(TMultiChunkPoolOutputTest, TestEmptyPools2)
{
    InitPools({0, 0, 0});
    int cookieCount = 0;
    while (!Pool_->IsCompleted()) {
        EXPECT_EQ(Pool_->Extract(), cookieCount);
        cookieCount++;
    }

    EXPECT_EQ(cookieCount, 0);
}

TEST_F(TMultiChunkPoolOutputTest, TestEmptyPools3)
{
    InitPools({});
    int cookieCount = 0;
    while (!Pool_->IsCompleted()) {
        EXPECT_EQ(Pool_->Extract(), cookieCount);
        cookieCount++;
    }

    EXPECT_EQ(cookieCount, 0);
}

TEST_F(TMultiChunkPoolOutputTest, TestTeleportChunks)
{
    // Teleport chunks are not supported for now.
    InitPools({0, 0});

    TInputChunkPtr chunk1 = New<TInputChunk>();
    TInputChunkPtr chunk2 = New<TInputChunk>();
    TInputChunkPtr chunk3 = New<TInputChunk>();

    std::vector<std::pair<TInputChunkPtr, int>> teleportChunks;
    Pool_->SubscribeChunkTeleported(BIND([&] (TInputChunkPtr teleportChunk, std::any tag) {
        teleportChunks.emplace_back(std::move(teleportChunk), std::any_cast<int>(tag));
    }));

    Mocks_[1]->TeleportChunk(chunk1);
    Mocks_[0]->TeleportChunk(chunk2);
    Mocks_[1]->TeleportChunk(chunk3);

    EXPECT_EQ(std::ssize(teleportChunks), 3);
    EXPECT_EQ(teleportChunks[0], std::pair(chunk1, 1));
    EXPECT_EQ(teleportChunks[1], std::pair(chunk2, 0));
    EXPECT_EQ(teleportChunks[2], std::pair(chunk3, 1));

    EXPECT_TRUE(Pool_->IsCompleted());
}

TEST_F(TMultiChunkPoolOutputTest, TestGetOutputOrder)
{
    // Output order is not supported for now.
    InitPools({1});
    EXPECT_EQ(Pool_->GetOutputOrder(), TOutputOrderPtr{});
    EXPECT_EQ(Pool_->Extract(), 0);
}

TEST_F(TMultiChunkPoolOutputTest, TestGetLocality)
{
    InitPools({0});

    EXPECT_EQ(Pool_->GetLocality(NNodeTrackerClient::TNodeId(42)), 0);
    EXPECT_TRUE(Pool_->IsCompleted());
}

TEST_F(TMultiChunkPoolOutputTest, TestGetStripeList)
{
    InitPools({2, 1});

    auto stripeList00 = New<TChunkStripeList>();
    auto stripeList01 = New<TChunkStripeList>();
    auto stripeList10 = New<TChunkStripeList>();

    InSequence sequence;
    EXPECT_CALL(*Mocks_[1], GetStripeList(0))
        .WillOnce(Return(stripeList10));
    EXPECT_CALL(*Mocks_[0], GetStripeList(0))
        .WillOnce(Return(stripeList00));
    EXPECT_CALL(*Mocks_[0], GetStripeList(1))
        .WillOnce(Return(stripeList01));

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(Pool_->Extract(), i);
    }

    EXPECT_EQ(Pool_->GetStripeList(0), stripeList10);
    EXPECT_EQ(Pool_->GetStripeList(1), stripeList00);
    EXPECT_EQ(Pool_->GetStripeList(2), stripeList01);

    EXPECT_EQ(stripeList00->PartitionTag, 0);
    EXPECT_EQ(stripeList01->PartitionTag, 0);
    EXPECT_EQ(stripeList10->PartitionTag, 1);
}

TEST_F(TMultiChunkPoolOutputTest, TestGetStripeListSliceCount)
{
    InitPools({2, 1});

    InSequence sequence;
    EXPECT_CALL(*Mocks_[1], GetStripeListSliceCount(0))
        .WillOnce(Return(42));
    EXPECT_CALL(*Mocks_[0], GetStripeListSliceCount(0))
        .WillOnce(Return(52));
    EXPECT_CALL(*Mocks_[0], GetStripeListSliceCount(1))
        .WillOnce(Return(25));

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(Pool_->Extract(), i);
    }

    EXPECT_EQ(Pool_->GetStripeListSliceCount(0), 42);
    EXPECT_EQ(Pool_->GetStripeListSliceCount(1), 52);
    EXPECT_EQ(Pool_->GetStripeListSliceCount(2), 25);
}

TEST_F(TMultiChunkPoolOutputTest, TestCompleted)
{
    InitPools({2, 1});

    InSequence sequence;
    EXPECT_CALL(*Mocks_[1], Completed(0, _))
        .Times(1);
    EXPECT_CALL(*Mocks_[0], Completed(0, _))
        .Times(1);
    EXPECT_CALL(*Mocks_[0], Completed(1, _))
        .Times(1);

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(Pool_->Extract(), i);
    }

    Pool_->Completed(0, TCompletedJobSummary{});
    Pool_->Completed(1, TCompletedJobSummary{});
    Pool_->Completed(2, TCompletedJobSummary{});
}

TEST_F(TMultiChunkPoolOutputTest, TestFailed)
{
    InitPools({2, 1});

    InSequence sequence;
    EXPECT_CALL(*Mocks_[1], Failed(0))
        .Times(1);
    EXPECT_CALL(*Mocks_[0], Failed(0))
        .Times(1);
    EXPECT_CALL(*Mocks_[0], Failed(1))
        .Times(1);

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(Pool_->Extract(), i);
    }

    Pool_->Failed(0);
    Pool_->Failed(1);
    Pool_->Failed(2);
}

TEST_F(TMultiChunkPoolOutputTest, TestAborted)
{
    InitPools({2, 1});

    InSequence sequence;
    EXPECT_CALL(*Mocks_[1], Aborted(0, EAbortReason::AccountLimitExceeded))
        .Times(1);
    EXPECT_CALL(*Mocks_[0], Aborted(0, EAbortReason::FailedChunks))
        .Times(1);
    EXPECT_CALL(*Mocks_[0], Aborted(1, EAbortReason::Scheduler))
        .Times(1);

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(Pool_->Extract(), i);
    }

    Pool_->Aborted(0, EAbortReason::AccountLimitExceeded);
    Pool_->Aborted(1, EAbortReason::FailedChunks);
    Pool_->Aborted(2, EAbortReason::Scheduler);
}

TEST_F(TMultiChunkPoolOutputTest, TestLost)
{
    InitPools({2, 1});

    InSequence sequence;
    EXPECT_CALL(*Mocks_[1], Lost(0))
        .Times(1);
    EXPECT_CALL(*Mocks_[0], Lost(0))
        .Times(1);
    EXPECT_CALL(*Mocks_[0], Lost(1))
        .Times(1);

    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(Pool_->Extract(), i);
    }

    Pool_->Lost(0);
    Pool_->Lost(1);
    Pool_->Lost(2);
}

TEST_F(TMultiChunkPoolOutputTest, TestCookieMapping)
{
    std::vector<int> poolSizes = {0, 3, 1, 4, 0, 1, 5, 9, 0};

    InitPools(poolSizes);

    // external_cookie -> (pool, cookie) mapping.
    std::vector<std::pair<int, int>> cookies;
    for (int pool = static_cast<int>(poolSizes.size()) - 1; pool >= 0; --pool) {
        for (int cookie = 0; cookie < poolSizes[pool]; ++cookie) {
            cookies.emplace_back(pool, cookie);
        }
    }

    for (int i = 0; i < std::ssize(cookies); ++i) {
        EXPECT_EQ(Pool_->Extract(), i);
    }

    std::vector<int> permutation(cookies.size());
    std::iota(permutation.begin(), permutation.end(), 0);
    std::mt19937 rng(42);
    std::shuffle(permutation.begin(), permutation.end(), rng);

    InSequence sequence;
    for (auto externalCookie : permutation) {
        auto [pool, cookie] = cookies[externalCookie];
        EXPECT_CALL(*Mocks_[pool], Failed(cookie))
            .Times(1);
    }

    for (auto externalCookie : permutation) {
        Pool_->Failed(externalCookie);
    }
}

TEST_F(TMultiChunkPoolOutputTest, TestFinalize)
{
    InitPools({2, 1}, /*finalize=*/false);

    for (int cookie = 0; cookie < 3; ++cookie) {
        EXPECT_EQ(Pool_->Extract(), cookie);
        EXPECT_FALSE(Pool_->IsCompleted());
    }

    Pool_->Finalize();
    EXPECT_TRUE(Pool_->IsCompleted());
}

TEST_F(TMultiChunkPoolOutputTest, TestAddPoolOutput)
{
    InitPools({3, 2, 1}, /*finalize=*/false, /*poolsToAdd=*/2);

    for (int cookie = 0; cookie < 5; ++cookie) {
        EXPECT_EQ(Pool_->Extract(), cookie);
        EXPECT_FALSE(Pool_->IsCompleted());
    }

    Pool_->AddPoolOutput(Mocks_[2], 42);
    Pool_->Finalize();

    EXPECT_FALSE(Pool_->IsCompleted());
    EXPECT_EQ(Pool_->Extract(), 5);
    EXPECT_TRUE(Pool_->IsCompleted());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools
