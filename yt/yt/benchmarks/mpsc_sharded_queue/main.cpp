#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/profiling/tscp.h>

#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/mpsc_sharded_queue.h>
#include <yt/yt/core/misc/mpsc_fair_share_queue.h>
#include <yt/yt/core/misc/relaxed_mpsc_queue.h>
#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <vector>
#include <thread>

using namespace NYT;
using namespace NYT::NProfiling;

//////////////////////////////////////////////////////////////////////////

constexpr int PoolCount = 4;
constexpr int BucketsCount = 1000;

using TTestMpscFairShareQueue = NYT::TMpscFairShareQueue<int, i64, int>;

void EnqueueMany(i64 count, auto& queue)
{
    for (i64 index = 0; index < count; ++index) {
        queue.Enqueue(std::move(index));
    }
}

void EnqueueMany(i64 count, moodycamel::ConcurrentQueue<i64>& queue)
{
    for (i64 index = 0; index < count; ++index) {
        queue.enqueue(index);
    }
}

void EnqueueMany(i64 count, TTestMpscFairShareQueue& queue)
{
    for (i64 index = 0; index < count; ++index) {
        queue.Enqueue({
            .Item = index + 1,
            .PoolWeight = static_cast<double>((index % PoolCount) + 1),
            .FairShareTag = static_cast<int>(index % BucketsCount),
        });
    }
}

void ConsumeAll(NYT::TMpscStack<i64>& queue, auto consumer)
{
    consumer(queue.DequeueAll());
}

void ConsumeAll(NYT::TRelaxedMpscQueue<i64>& queue, auto consumer)
{
    constexpr int BatchSize = 10000;
    std::vector<i64> batch;
    batch.reserve(BatchSize);

    i64 value = 0;
    while (queue.TryDequeue(&value)) {
        batch.push_back(value);
        if (batch.size() == BatchSize) {
            consumer(batch);
            batch.clear();
        }
    }

    consumer(batch);
}

void ConsumeAll(moodycamel::ConcurrentQueue<i64>& queue, auto consumer)
{
    std::vector<i64> batch(10000);
    int dequeued = 0;
    while (dequeued = queue.try_dequeue_bulk(batch.begin(), batch.size())) {
        batch.resize(dequeued);
        consumer(batch);

        batch.resize(batch.capacity());
    }
}

void ConsumeAll(TTestMpscFairShareQueue& queue, auto consumer)
{
    queue.PrepareDequeue();

    constexpr int BatchSize = 10000;
    std::vector<i64> batch;
    batch.reserve(BatchSize);

    i64 value = 0;
    while (value = queue.TryDequeue()) {
        batch.push_back(value);
        if (batch.size() == BatchSize) {
            consumer(batch);
            batch.clear();
        }

        queue.MarkFinished(value, 100);
    }

    consumer(batch);
}

void ConsumeAll(NYT::TMpscShardedQueue<i64>& queue, auto consumer)
{
    queue.ConsumeAll(consumer);
}

template<typename TQueue>
void Bench(const std::string& queueName, int producersCount)
{
    auto startTime = TInstant::Now();

    TQueue holder;

    std::vector<std::thread> threads;

    const i64 TasksCount = 200_MB;

    threads.reserve(producersCount);
    for (int i = 0; i < producersCount; ++i) {
        threads.push_back(std::thread([&] () {
            EnqueueMany(TasksCount / producersCount, holder);
        }));
    }

    std::atomic_bool stopped = false;
    i64 totalDequeued = 0;

    std::thread consumer([&] () {
        while (!stopped) {
            ConsumeAll(holder, [&] (const std::vector<i64>& batch) {
                totalDequeued += std::size(batch);
            });

            Sleep(TDuration::MicroSeconds(10));
        }
    });

    for (auto& t : threads) {
        t.join();
    }

    stopped = true;
    consumer.join();
    ConsumeAll(holder, [&] (const std::vector<i64>& batch) {
        totalDequeued += std::size(batch);
    });

    YT_VERIFY(totalDequeued == TasksCount);

    Cerr << "Queue: " << queueName << "\tThreadCount: " << producersCount << "\tTook: " << TInstant::Now() - startTime <<  Endl;
}

template<typename TQueue>
void BenchMany(const std::string& name)
{
    for (auto threadCount : {1, 4, 8, 16, 32, 64})
    {
        Bench<TQueue>(name, threadCount);
    }
    Cerr << "-----"<<  Endl;
}

int main()
{
    BenchMany<moodycamel::ConcurrentQueue<i64>>("moodycamel");
    BenchMany<NYT::TMpscStack<i64>>("TMpscStack");
    BenchMany<NYT::TRelaxedMpscQueue<i64>>("TRelaxedMpscQueue");
    BenchMany<NYT::TMpscShardedQueue<i64>>("TMpscShardedQueue");
    BenchMany<TTestMpscFairShareQueue>("MpscFairShareQueue");
    return 0;
}
