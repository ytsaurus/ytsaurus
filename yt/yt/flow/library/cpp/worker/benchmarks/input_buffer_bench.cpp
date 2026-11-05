#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/worker/input_buffer_detail.h>

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/core/ytree/convert.h>

#include <absl/container/btree_set.h>

#include <util/random/fast.h>

namespace NYT::NFlow::NWorker {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr int ConnectionCount = 8;
constexpr int BatchSize = 128;

const TStreamId StreamId("input_stream");

// Payload matching the per-element footprint of the old accounting cookie, so the btree
// element stays the same size as before for a fair size comparison against the heap.
struct TCookiePayload
{
    i64 MessageSize = 0;
    TStreamId StreamId;
};

// Replica of the pre-YTFLOW-682 container: absl btree ordered by
// (Message->AlignmentTimestamp, Message->MessageId), key behind a pointer.
struct TBtreeOrderedMessage
{
    TInputMessageConstPtr Message;
    TCookiePayload Cookie;

    bool operator<(const TBtreeOrderedMessage& right) const
    {
        if (Message->AlignmentTimestamp != right.Message->AlignmentTimestamp) {
            return Message->AlignmentTimestamp < right.Message->AlignmentTimestamp;
        }
        return Message->MessageId < right.Message->MessageId;
    }
};

TCookiePayload MakeCookie(const TInputMessageConstPtr& message)
{
    return {.MessageSize = message->ByteSize + 128, .StreamId = message->StreamId};
}

struct TBtreeQueue
{
    absl::btree_multiset<TBtreeOrderedMessage> Queue;

    void Push(const TInputMessageConstPtr& message, ui64 /*seqNo*/)
    {
        Queue.insert({.Message = message, .Cookie = MakeCookie(message)});
    }

    size_t Size() const
    {
        return Queue.size();
    }

    TBtreeOrderedMessage ExtractFront()
    {
        auto node = Queue.extract(Queue.begin());
        return std::move(node.value());
    }
};

struct THeapQueue
{
    TInputBuffer::TMessagesPriorityQueue Queue;

    void Push(const TInputMessageConstPtr& message, ui64 seqNo)
    {
        Queue.push({
            .AlignmentTimestamp = message->AlignmentTimestamp,
            .SeqNo = seqNo,
            .Message = message,
        });
    }

    size_t Size() const
    {
        return Queue.size();
    }

    TInputBuffer::TOrderedMessage ExtractFront()
    {
        return Queue.extract_front();
    }
};

////////////////////////////////////////////////////////////////////////////////

using TConnectionPools = std::vector<std::vector<TInputMessageConstPtr>>;

// |shuffledIds| models inherited message ids (multi-stage pipelines): their order
// does not correlate with arrival order, so btree inserts land at random positions.
const TConnectionPools& GetConnectionPools(bool spreadTimestamps, bool shuffledIds, int perConnection)
{
    static THashMap<std::tuple<bool, bool, int>, TConnectionPools> cache;
    auto [it, inserted] = cache.try_emplace(std::tuple(spreadTimestamps, shuffledIds, perConnection));
    if (!inserted) {
        return it->second;
    }

    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        "[{name=banner_id;type=uint64};{name=is_click;type=boolean}]")));
    constexpr ui64 baseTimestamp = 1700000000;
    TFastRng64 rng(42);
    ui64 globalSeq = 0;

    auto& pools = it->second;
    pools.resize(ConnectionCount);
    for (int connection = 0; connection < ConnectionCount; ++connection) {
        auto& pool = pools[connection];
        std::vector<ui64> timestamps(perConnection, baseTimestamp);
        if (spreadTimestamps) {
            for (auto& timestamp : timestamps) {
                timestamp += rng.GenRand() % 300;
            }
            std::sort(timestamps.begin(), timestamps.end());
        }

        pool.reserve(perConnection);
        for (int i = 0; i < perConnection; ++i) {
            TMessageBuilder builder(StreamId, schema);
            auto idSeqNo = shuffledIds ? rng.GenRand() : static_cast<ui64>(i);
            builder.SetMessageId(GenerateOrderedMessageId(
                TUniqueSeqNo(idSeqNo),
                StreamId,
                ToString(connection),
                ToString(1000000000 + i)));
            builder.SetSystemTimestamp(TSystemTimestamp(timestamps[i]));
            builder.SetAlignmentTimestamp(TSystemTimestamp(timestamps[i]));
            builder.SetEventTimestamp(TSystemTimestamp(timestamps[i]));
            builder.Payload().SetValue(MakeUnversionedUint64Value(globalSeq, 0));
            builder.Payload().SetValue(MakeUnversionedBooleanValue(true, 1));
            ++globalSeq;
            pool.push_back(New<TInputMessage>(builder.Finish(), TKey()));
        }
    }
    return pools;
}

template <class TQueue>
void RunBench(benchmark::State& state, bool spreadTimestamps, bool shuffledIds)
{
    const i64 steadyStateSize = state.range(0);
    const int perConnection = steadyStateSize * 4 / ConnectionCount;
    const auto& pools = GetConnectionPools(spreadTimestamps, shuffledIds, perConnection);

    TQueue queue;
    ui64 seqNo = 0;
    std::vector<int> cursors(ConnectionCount, 0);
    int connection = 0;
    auto pushBatch = [&] {
        auto& cursor = cursors[connection];
        const auto& pool = pools[connection];
        for (int i = 0; i < BatchSize; ++i) {
            queue.Push(pool[cursor++], seqNo++);
        }
        connection = (connection + 1) % ConnectionCount;
    };
    auto resetAndPrefill = [&] {
        queue = TQueue();
        std::fill(cursors.begin(), cursors.end(), 0);
        connection = 0;
        while (static_cast<i64>(queue.Size()) < steadyStateSize) {
            pushBatch();
        }
    };

    resetAndPrefill();

    i64 processed = 0;
    for (auto _ : state) {
        // Timestamps must keep advancing as they do in production: when the pool is
        // exhausted, rebuild the queue outside of the measured time instead of wrapping
        // around into the past.
        if (cursors[connection] + BatchSize > perConnection) {
            state.PauseTiming();
            resetAndPrefill();
            state.ResumeTiming();
        }
        pushBatch();
        for (int i = 0; i < BatchSize; ++i) {
            auto extracted = queue.ExtractFront();
            benchmark::DoNotOptimize(extracted);
        }
        processed += BatchSize;
    }
    state.SetItemsProcessed(processed);
}

void BM_BtreeSameSecondOrderedIds(benchmark::State& state)
{
    RunBench<TBtreeQueue>(state, /*spreadTimestamps*/ false, /*shuffledIds*/ false);
}

void BM_HeapSameSecondOrderedIds(benchmark::State& state)
{
    RunBench<THeapQueue>(state, /*spreadTimestamps*/ false, /*shuffledIds*/ false);
}

void BM_BtreeSameSecondShuffledIds(benchmark::State& state)
{
    RunBench<TBtreeQueue>(state, /*spreadTimestamps*/ false, /*shuffledIds*/ true);
}

void BM_HeapSameSecondShuffledIds(benchmark::State& state)
{
    RunBench<THeapQueue>(state, /*spreadTimestamps*/ false, /*shuffledIds*/ true);
}

void BM_BtreeSpreadTimestamps(benchmark::State& state)
{
    RunBench<TBtreeQueue>(state, /*spreadTimestamps*/ true, /*shuffledIds*/ false);
}

void BM_HeapSpreadTimestamps(benchmark::State& state)
{
    RunBench<THeapQueue>(state, /*spreadTimestamps*/ true, /*shuffledIds*/ false);
}

BENCHMARK(BM_BtreeSameSecondOrderedIds)->Arg(10'000)->Arg(100'000);
BENCHMARK(BM_HeapSameSecondOrderedIds)->Arg(10'000)->Arg(100'000);
BENCHMARK(BM_BtreeSameSecondShuffledIds)->Arg(10'000)->Arg(100'000);
BENCHMARK(BM_HeapSameSecondShuffledIds)->Arg(10'000)->Arg(100'000);
BENCHMARK(BM_BtreeSpreadTimestamps)->Arg(10'000)->Arg(100'000);
BENCHMARK(BM_HeapSpreadTimestamps)->Arg(10'000)->Arg(100'000);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
