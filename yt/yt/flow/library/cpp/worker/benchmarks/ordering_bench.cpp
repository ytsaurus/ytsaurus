#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/worker/input_buffer_detail.h>
#include <yt/yt/flow/library/cpp/worker/message_distributor_detail.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NWorker {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constexpr int MessagesPerStream = 10'000;
constexpr i64 MaxRowsPerBatch = 1'000;
const auto Now = TInstant::Seconds(1'800'000'000);
const auto BoostAge = TDuration::Hours(2);

// The first half of every stream is older than the boost age, the second half is fresh.
TSystemTimestamp GetTimestamp(int index)
{
    auto base = index < MessagesPerStream / 2
        ? Now - BoostAge - TDuration::Seconds(MessagesPerStream / 2)
        : Now - TDuration::Seconds(MessagesPerStream);
    return TSystemTimestamp(base.Seconds() + index);
}

TStreamId GetStreamId(int stream)
{
    return TStreamId(Format("stream-%v", stream));
}

std::vector<TInputMessageConstPtr> MakeInputMessages(int streamCount)
{
    auto schema = New<NTableClient::TTableSchema>();
    std::vector<TInputMessageConstPtr> messages;
    for (int stream = 0; stream < streamCount; ++stream) {
        for (int index = 0; index < MessagesPerStream; ++index) {
            TMessageBuilder builder(GetStreamId(stream), schema);
            builder.SetMessageId(TMessageId(Format("%v-%v", stream, index)));
            builder.SetSystemTimestamp(GetTimestamp(index));
            builder.SetAlignmentTimestamp(GetTimestamp(index));
            builder.SetEventTimestamp(GetTimestamp(index));
            messages.push_back(New<TInputMessage>(builder.Finish(), MakeKey(stream, index)));
        }
    }
    return messages;
}

// Arguments: stream count, boost (0 or 1). Reports the time per extracted message.
void BM_InputBufferExtractBatch(benchmark::State& state)
{
    const int streamCount = state.range(0);
    const auto messages = MakeInputMessages(streamCount);

    auto computationSpec = New<TComputationSpec>();
    THashSet<TStreamId> allowedStreams;
    for (int stream = 0; stream < streamCount; ++stream) {
        computationSpec->InputStreamIds.insert(GetStreamId(stream));
        allowedStreams.insert(GetStreamId(stream));
    }
    auto dynamicSpec = New<TDynamicComputationSpec>();
    dynamicSpec->BatchDuration = TDuration::Zero();
    dynamicSpec->MaxRowsPerBatch = TSize(MaxRowsPerBatch);
    dynamicSpec->MaxBytesPerBatch = TSize(1LL << 40);
    dynamicSpec->StaleInputBoostAge = state.range(1) ? BoostAge : TDuration::Zero();

    i64 extracted = 0;
    for (auto _ : state) {
        state.PauseTiming();
        NFlow::TStreamLimitUsageStateMap limits;
        for (int stream = 0; stream < streamCount; ++stream) {
            auto limit = New<NFlow::TStreamLimitUsageState>();
            limit->SetLimitBytes(1LL << 40);
            limits.emplace(GetStreamId(stream), std::move(limit));
        }
        auto buffer = New<TInputBuffer>(
            TJobId(TGuid::Create()),
            std::move(limits),
            /*epochCycleTracker*/ nullptr,
            THashMap<TStreamId, NFlow::TOfferedRateEstimatorPtr>{},
            computationSpec,
            TComputationId("computation"),
            dynamicSpec,
            GetSyncInvoker(),
            NProfiling::TProfiler{},
            [] {
                return Now;
            });
        Y_UNUSED(NConcurrency::WaitFor(buffer->AddMessages(TGuid::Create(), messages, /*onProcessed*/ {})));
        state.ResumeTiming();

        while (true) {
            auto batch = NConcurrency::WaitFor(buffer->GetInputBatch(allowedStreams)).ValueOrThrow();
            if (batch.empty()) {
                break;
            }
            extracted += std::ssize(batch);
        }

        state.PauseTiming();
        buffer.Reset();
        state.ResumeTiming();
    }
    if (extracted != std::ssize(messages) * state.iterations()) {
        state.SkipWithError("Not every message was extracted");
    }
    state.SetItemsProcessed(extracted);
}

BENCHMARK(BM_InputBufferExtractBatch)
    ->ArgsProduct({{1, 4, 16}, {0, 1}})
    ->ArgNames({"streams", "boost"})
    ->Unit(benchmark::kMillisecond);

////////////////////////////////////////////////////////////////////////////////

// Arguments: stream count, boost (0 or 1). One call selects every queued task; reports the time per task.
void BM_SelectTasksToSend(benchmark::State& state)
{
    const int streamCount = state.range(0);
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""([{name="data"; type="string";};])""")));

    std::vector<TRoutedTaskSet> taskSets(streamCount);
    THashMap<std::pair<TStreamId, TComputationId>, TStreamOrderingParams> orderingParams;
    for (int stream = 0; stream < streamCount; ++stream) {
        auto streamId = GetStreamId(stream);
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = schema;
        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
        specs[streamId][TStreamSpecId(1)] = streamSpec;
        auto storage = New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(specs),
            /*groupBySchema*/ New<NTableClient::TTableSchema>(),
            /*evaluatorCache*/ nullptr);
        for (int index = 0; index < MessagesPerStream; ++index) {
            TMessageBuilder builder(streamId, schema);
            builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue("X", 0));
            builder.SetMessageId(TMessageId(Format("%v-%v", stream, index)));
            builder.SetSystemTimestamp(GetTimestamp(index));
            builder.SetAlignmentTimestamp(GetTimestamp(index));
            builder.SetEventTimestamp(GetTimestamp(index));
            TTaskId taskId = static_cast<TTaskId>(index) * streamCount + stream;
            taskSets[stream].insert(TRoutedTask{.Task = {
                    .Id = taskId,
                    .AlignmentTimestamp = GetTimestamp(index),
                    .Message = New<TOutputMessage>(builder.Finish(), storage),
                                                }});
        }
        orderingParams[std::pair(streamId, TComputationId())] = TStreamOrderingParams{
            .StaleInputBoostAge = state.range(1) ? BoostAge : TDuration::Zero(),
        };
    }

    THashMap<std::pair<TJobId, TStreamId>, i64> limits;
    for (int stream = 0; stream < streamCount; ++stream) {
        limits[std::pair(TJobId(), GetStreamId(stream))] = 1LL << 40;
    }
    auto iterate = [&] (auto&& callback) {
        for (int stream = 0; stream < streamCount; ++stream) {
            callback(TJobId(), GetStreamId(stream), taskSets[stream]);
        }
    };

    i64 selected = 0;
    for (auto _ : state) {
        TWorkerConnection::TBatchLimiter batcher(1e9, 1LL << 40);
        SelectTasksToSend(iterate, orderingParams, Now, limits, batcher, [&] (const TJobId&, const TRoutedTask&) {
            ++selected;
        });
    }
    if (selected != static_cast<i64>(streamCount) * MessagesPerStream * state.iterations()) {
        state.SkipWithError("Not every task was selected");
    }
    state.SetItemsProcessed(selected);
}

BENCHMARK(BM_SelectTasksToSend)
    ->ArgsProduct({{1, 4, 16}, {0, 1}})
    ->ArgNames({"streams", "boost"})
    ->Unit(benchmark::kMillisecond);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
