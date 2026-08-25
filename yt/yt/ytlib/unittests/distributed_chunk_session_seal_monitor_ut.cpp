#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/seal_monitor.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/suspendable_action_queue.h>

#include <atomic>
#include <stdexcept>

namespace NYT::NDistributedChunkSessionClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionSealMonitorTest
    : public ::testing::Test
{
protected:
    static constexpr auto TestTimeout = TDuration::Seconds(5);

    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("SealMonitorTest");

    static TChunkId MakeChunkId(TCellTag cellTag, ui64 counter)
    {
        return MakeId(EObjectType::JournalChunk, cellTag, counter, 0);
    }

    static TDistributedChunkSessionSealSummary MakeSealSummary(TChunkId chunkId)
    {
        return TDistributedChunkSessionSealSummary{
            .ChunkId = chunkId,
            .RecordCount = 10,
            .CompressedDataSize = 20,
        };
    }

    static std::vector<TDistributedChunkSessionSealSummary> MakeSealSummaries(
        const std::vector<TChunkId>& chunkIds)
    {
        std::vector<TDistributedChunkSessionSealSummary> summaries;
        summaries.reserve(chunkIds.size());
        for (auto chunkId : chunkIds) {
            summaries.push_back(MakeSealSummary(chunkId));
        }
        return summaries;
    }

    static TDistributedChunkSessionSealMonitorConfigPtr CreateConfig()
    {
        auto config = New<TDistributedChunkSessionSealMonitorConfig>();
        config->SetDefaults();
        return config;
    }

    IDistributedChunkSessionSealMonitorPtr CreateMonitor(
        TDistributedChunkSessionSealSummaryFetchCallback fetchSealSummaries,
        TDistributedChunkSessionSealMonitorConfigPtr config = CreateConfig())
    {
        return CreateDistributedChunkSessionSealMonitor(
            std::move(config),
            std::move(fetchSealSummaries),
            ActionQueue_->GetInvoker());
    }

    void DrainActionQueue()
    {
        auto drainedPromise = NewPromise<void>();
        ActionQueue_->GetInvoker()->Invoke(BIND([drainedPromise] {
            drainedPromise.TrySet();
        }));
        WaitFor(drainedPromise.ToFuture().WithTimeout(TestTimeout))
            .ThrowOnError();
    }

    template <class T>
    static T WaitForValue(const TFuture<T>& future)
    {
        return WaitFor(future.WithTimeout(TestTimeout))
            .ValueOrThrow();
    }

    static void WaitForCompletion(const TFuture<void>& future)
    {
        WaitFor(future.WithTimeout(TestTimeout))
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedChunkSessionSealMonitorTest, FetchesNewChunksImmediately)
{
    auto config = CreateConfig();
    config->PollPeriod = TDuration::Seconds(30);

    auto chunkId = MakeChunkId(TCellTag(1), 1);
    auto deliveredPromise = NewPromise<std::vector<TDistributedChunkSessionSealSummary>>();
    auto monitor = CreateMonitor(
        BIND([=] (std::vector<TChunkId> chunkIds) {
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        config);
    auto subscription = monitor->Subscribe(BIND(
        [deliveredPromise] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            deliveredPromise.TrySet(std::move(summaries));
        }));

    subscription->TrackChunks({chunkId});

    auto summaries = WaitForValue(deliveredPromise.ToFuture());
    ASSERT_EQ(summaries.size(), 1u);
    EXPECT_EQ(summaries[0].ChunkId, chunkId);
}

TEST_F(TDistributedChunkSessionSealMonitorTest, FetchesNewChunkWhileUnsealedChunkIsDelayed)
{
    auto config = CreateConfig();
    config->PollPeriod = TDuration::Seconds(30);

    auto firstChunkId = MakeChunkId(TCellTag(1), 1);
    auto secondChunkId = MakeChunkId(TCellTag(1), 2);
    auto firstFetchStartedPromise = NewPromise<void>();
    auto firstFetchPromise = NewPromise<std::vector<TDistributedChunkSessionSealSummary>>();
    auto secondChunkDeliveredPromise = NewPromise<void>();
    std::atomic<int> fetchCount = 0;

    auto monitor = CreateMonitor(
        BIND([&] (std::vector<TChunkId> chunkIds) {
            if (++fetchCount == 1) {
                firstFetchStartedPromise.TrySet();
                return firstFetchPromise.ToFuture();
            }
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        config);
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            for (const auto& summary : summaries) {
                if (summary.ChunkId == secondChunkId) {
                    secondChunkDeliveredPromise.TrySet();
                }
            }
        }));

    subscription->TrackChunks({firstChunkId});
    WaitForCompletion(firstFetchStartedPromise.ToFuture());
    DrainActionQueue();
    firstFetchPromise.TrySet(std::vector<TDistributedChunkSessionSealSummary>{});
    DrainActionQueue();

    subscription->TrackChunks({secondChunkId});

    WaitForCompletion(secondChunkDeliveredPromise.ToFuture());
    EXPECT_EQ(fetchCount.load(), 2);
}

TEST_F(TDistributedChunkSessionSealMonitorTest, PollsUnsealedChunksAgain)
{
    auto config = CreateConfig();
    config->PollPeriod = TDuration::MilliSeconds(500);

    auto chunkId = MakeChunkId(TCellTag(1), 1);
    auto firstFetchStartedPromise = NewPromise<void>();
    auto deliveredPromise = NewPromise<void>();
    std::atomic<int> fetchCount = 0;

    auto monitor = CreateMonitor(
        BIND([&] (std::vector<TChunkId> chunkIds) {
            if (++fetchCount == 1) {
                firstFetchStartedPromise.TrySet();
                return MakeFuture<std::vector<TDistributedChunkSessionSealSummary>>({});
            }
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        config);
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary>) {
            deliveredPromise.TrySet();
        }));

    subscription->TrackChunks({chunkId});
    WaitForCompletion(firstFetchStartedPromise.ToFuture());
    WaitForCompletion(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(100)));
    EXPECT_EQ(fetchCount.load(), 1);

    WaitForCompletion(deliveredPromise.ToFuture());
    EXPECT_EQ(fetchCount.load(), 2);
}

TEST_F(TDistributedChunkSessionSealMonitorTest, RetriesFailedFetch)
{
    auto config = CreateConfig();
    config->ErrorBackoff.MinBackoff = TDuration::MilliSeconds(500);
    config->ErrorBackoff.MaxBackoff = TDuration::MilliSeconds(500);
    config->ErrorBackoff.BackoffJitter = 0;

    auto chunkId = MakeChunkId(TCellTag(1), 1);
    auto firstFetchStartedPromise = NewPromise<void>();
    auto deliveredPromise = NewPromise<void>();
    std::atomic<int> fetchCount = 0;

    auto monitor = CreateMonitor(
        BIND([&] (std::vector<TChunkId> chunkIds) {
            if (++fetchCount == 1) {
                firstFetchStartedPromise.TrySet();
                return MakeFuture<std::vector<TDistributedChunkSessionSealSummary>>(
                    TError("Failed to fetch seal summaries"));
            }
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        config);
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary>) {
            deliveredPromise.TrySet();
        }));

    subscription->TrackChunks({chunkId});
    WaitForCompletion(firstFetchStartedPromise.ToFuture());
    WaitForCompletion(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(100)));
    EXPECT_EQ(fetchCount.load(), 1);

    WaitForCompletion(deliveredPromise.ToFuture());
    EXPECT_EQ(fetchCount.load(), 2);
}

TEST_F(TDistributedChunkSessionSealMonitorTest, ReconfiguresFetchBatchSize)
{
    auto config = CreateConfig();
    config->MaxChunksPerFetch = 1;

    auto firstFetchStartedPromise = NewPromise<void>();
    auto firstFetchPromise = NewPromise<std::vector<TDistributedChunkSessionSealSummary>>();
    auto secondBatchPromise = NewPromise<std::vector<TChunkId>>();
    auto allDeliveredPromise = NewPromise<void>();
    std::atomic<int> fetchCount = 0;
    std::atomic<int> deliveredCount = 0;

    auto monitor = CreateMonitor(
        BIND([&] (std::vector<TChunkId> chunkIds) {
            if (++fetchCount == 1) {
                firstFetchStartedPromise.TrySet();
                return firstFetchPromise.ToFuture();
            }
            secondBatchPromise.TrySet(chunkIds);
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        config);
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            if (deliveredCount.fetch_add(summaries.size()) + summaries.size() == 3) {
                allDeliveredPromise.TrySet();
            }
        }));

    subscription->TrackChunks({
        MakeChunkId(TCellTag(1), 1),
        MakeChunkId(TCellTag(1), 2),
        MakeChunkId(TCellTag(1), 3),
    });
    WaitForCompletion(firstFetchStartedPromise.ToFuture());
    DrainActionQueue();

    auto reconfigured = CreateConfig();
    reconfigured->MaxChunksPerFetch = 2;
    monitor->Reconfigure(reconfigured);
    DrainActionQueue();

    firstFetchPromise.TrySet(std::vector<TDistributedChunkSessionSealSummary>{
        MakeSealSummary(MakeChunkId(TCellTag(1), 1)),
    });

    auto secondBatch = WaitForValue(secondBatchPromise.ToFuture());
    EXPECT_EQ(secondBatch.size(), 2u);
    WaitForCompletion(allDeliveredPromise.ToFuture());
}

TEST_F(TDistributedChunkSessionSealMonitorTest, LimitsFetchBatchSize)
{
    auto config = CreateConfig();
    config->MaxChunksPerFetch = 2;

    std::vector<int> batchSizes;
    auto allDeliveredPromise = NewPromise<void>();
    std::atomic<int> deliveredCount = 0;

    auto monitor = CreateMonitor(
        BIND([&] (std::vector<TChunkId> chunkIds) {
            batchSizes.push_back(chunkIds.size());
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        config);
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            if (deliveredCount.fetch_add(summaries.size()) + summaries.size() == 5) {
                allDeliveredPromise.TrySet();
            }
        }));

    subscription->TrackChunks({
        MakeChunkId(TCellTag(1), 1),
        MakeChunkId(TCellTag(1), 2),
        MakeChunkId(TCellTag(1), 3),
        MakeChunkId(TCellTag(1), 4),
        MakeChunkId(TCellTag(1), 5),
    });

    WaitForCompletion(allDeliveredPromise.ToFuture());
    EXPECT_EQ(batchSizes, (std::vector<int>{2, 2, 1}));
}

TEST_F(TDistributedChunkSessionSealMonitorTest, DoesNotOverlapFetchesWithinCell)
{
    auto config = CreateConfig();
    config->MaxChunksPerFetch = 1;

    auto firstBatchPromise = NewPromise<std::vector<TChunkId>>();
    auto firstFetchPromise = NewPromise<std::vector<TDistributedChunkSessionSealSummary>>();
    auto allDeliveredPromise = NewPromise<void>();
    std::atomic<int> fetchCount = 0;
    std::atomic<int> deliveredCount = 0;

    auto monitor = CreateMonitor(
        BIND([&] (std::vector<TChunkId> chunkIds) {
            if (++fetchCount == 1) {
                firstBatchPromise.TrySet(chunkIds);
                return firstFetchPromise.ToFuture();
            }
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        config);
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            if (deliveredCount.fetch_add(summaries.size()) + summaries.size() == 2) {
                allDeliveredPromise.TrySet();
            }
        }));

    subscription->TrackChunks({
        MakeChunkId(TCellTag(1), 1),
        MakeChunkId(TCellTag(1), 2),
    });

    auto firstBatch = WaitForValue(firstBatchPromise.ToFuture());
    WaitForCompletion(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(100)));
    EXPECT_EQ(fetchCount.load(), 1);

    firstFetchPromise.TrySet(MakeSealSummaries(firstBatch));
    WaitForCompletion(allDeliveredPromise.ToFuture());
    EXPECT_EQ(fetchCount.load(), 2);
}

TEST_F(TDistributedChunkSessionSealMonitorTest, FetchesDifferentCellsConcurrently)
{
    auto firstCellChunkId = MakeChunkId(TCellTag(1), 1);
    auto secondCellChunkId = MakeChunkId(TCellTag(2), 1);
    auto firstCellFetchStartedPromise = NewPromise<void>();
    auto firstCellFetchPromise = NewPromise<std::vector<TDistributedChunkSessionSealSummary>>();
    auto secondCellDeliveredPromise = NewPromise<void>();

    auto monitor = CreateMonitor(BIND([&] (std::vector<TChunkId> chunkIds) {
        if (CellTagFromId(chunkIds.front()) == TCellTag(1)) {
            firstCellFetchStartedPromise.TrySet();
            return firstCellFetchPromise.ToFuture();
        }
        return MakeFuture(MakeSealSummaries(chunkIds));
    }));
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            for (const auto& summary : summaries) {
                if (summary.ChunkId == secondCellChunkId) {
                    secondCellDeliveredPromise.TrySet();
                }
            }
        }));

    subscription->TrackChunks({firstCellChunkId});
    WaitForCompletion(firstCellFetchStartedPromise.ToFuture());
    subscription->TrackChunks({secondCellChunkId});

    WaitForCompletion(secondCellDeliveredPromise.ToFuture());
    firstCellFetchPromise.TrySet(std::vector<TDistributedChunkSessionSealSummary>{});
}

TEST_F(TDistributedChunkSessionSealMonitorTest, BatchesAcrossSubscriptions)
{
    auto actionQueue = CreateSuspendableActionQueue("SealMonitorBatchingTest");
    auto invoker = actionQueue->GetInvoker();
    WaitFor(actionQueue->Suspend(/*immediately*/ false))
        .ThrowOnError();

    auto firstChunkId = MakeChunkId(TCellTag(1), 1);
    auto secondChunkId = MakeChunkId(TCellTag(1), 2);
    auto fetchedPromise = NewPromise<std::vector<TChunkId>>();
    auto firstDeliveredPromise = NewPromise<void>();
    auto secondDeliveredPromise = NewPromise<void>();

    auto config = CreateConfig();
    config->MaxChunksPerFetch = 2;
    auto monitor = CreateDistributedChunkSessionSealMonitor(
        config,
        BIND([&] (std::vector<TChunkId> chunkIds) {
            fetchedPromise.TrySet(chunkIds);
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        invoker);
    auto firstSubscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            ASSERT_EQ(summaries.size(), 1u);
            EXPECT_EQ(summaries[0].ChunkId, firstChunkId);
            firstDeliveredPromise.TrySet();
        }));
    auto secondSubscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            ASSERT_EQ(summaries.size(), 1u);
            EXPECT_EQ(summaries[0].ChunkId, secondChunkId);
            secondDeliveredPromise.TrySet();
        }));

    firstSubscription->TrackChunks({firstChunkId});
    secondSubscription->TrackChunks({secondChunkId});
    actionQueue->Resume();

    auto fetchedChunkIds = WaitForValue(fetchedPromise.ToFuture());
    EXPECT_EQ(THashSet<TChunkId>(fetchedChunkIds.begin(), fetchedChunkIds.end()),
        (THashSet<TChunkId>{firstChunkId, secondChunkId}));
    WaitForCompletion(firstDeliveredPromise.ToFuture());
    WaitForCompletion(secondDeliveredPromise.ToFuture());
}

TEST_F(TDistributedChunkSessionSealMonitorTest, UnsubscribeIgnoresInFlightResult)
{
    auto chunkId = MakeChunkId(TCellTag(1), 1);
    auto fetchStartedPromise = NewPromise<void>();
    auto fetchPromise = NewPromise<std::vector<TDistributedChunkSessionSealSummary>>();
    std::atomic<int> callbackCount = 0;

    auto monitor = CreateMonitor(BIND([&] (std::vector<TChunkId>) {
        fetchStartedPromise.TrySet();
        return fetchPromise.ToFuture();
    }));
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary>) {
            ++callbackCount;
        }));

    subscription->TrackChunks({chunkId});
    WaitForCompletion(fetchStartedPromise.ToFuture());
    subscription.reset();
    fetchPromise.TrySet(std::vector{MakeSealSummary(chunkId)});
    DrainActionQueue();

    EXPECT_EQ(callbackCount.load(), 0);
}

TEST_F(TDistributedChunkSessionSealMonitorTest, TracksMoreChunksAfterDelivery)
{
    auto firstChunkId = MakeChunkId(TCellTag(1), 1);
    auto secondChunkId = MakeChunkId(TCellTag(1), 2);
    auto firstDeliveredPromise = NewPromise<void>();
    auto secondDeliveredPromise = NewPromise<void>();
    std::atomic<int> callbackCount = 0;

    auto monitor = CreateMonitor(BIND([&] (std::vector<TChunkId> chunkIds) {
        return MakeFuture(MakeSealSummaries(chunkIds));
    }));
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary> summaries) {
            ASSERT_EQ(summaries.size(), 1u);
            if (++callbackCount == 1) {
                EXPECT_EQ(summaries[0].ChunkId, firstChunkId);
                firstDeliveredPromise.TrySet();
            } else {
                EXPECT_EQ(summaries[0].ChunkId, secondChunkId);
                secondDeliveredPromise.TrySet();
            }
        }));

    subscription->TrackChunks({firstChunkId});
    WaitForCompletion(firstDeliveredPromise.ToFuture());
    subscription->TrackChunks({secondChunkId});

    WaitForCompletion(secondDeliveredPromise.ToFuture());
    EXPECT_EQ(callbackCount.load(), 2);
}

TEST_F(TDistributedChunkSessionSealMonitorTest, RetriesSynchronousFetchException)
{
    auto config = CreateConfig();
    config->ErrorBackoff.MinBackoff = TDuration::MilliSeconds(10);
    config->ErrorBackoff.MaxBackoff = TDuration::MilliSeconds(10);
    config->ErrorBackoff.BackoffJitter = 0;

    auto chunkId = MakeChunkId(TCellTag(1), 1);
    auto deliveredPromise = NewPromise<void>();
    std::atomic<int> fetchCount = 0;

    auto monitor = CreateMonitor(
        BIND([&] (std::vector<TChunkId> chunkIds) {
            if (++fetchCount == 1) {
                throw std::runtime_error("Synchronous fetch failure");
            }
            return MakeFuture(MakeSealSummaries(chunkIds));
        }),
        config);
    auto subscription = monitor->Subscribe(BIND(
        [&] (std::vector<TDistributedChunkSessionSealSummary>) {
            deliveredPromise.TrySet();
        }));

    subscription->TrackChunks({chunkId});

    WaitForCompletion(deliveredPromise.ToFuture());
    EXPECT_EQ(fetchCount.load(), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NYT::NDistributedChunkSessionClient
