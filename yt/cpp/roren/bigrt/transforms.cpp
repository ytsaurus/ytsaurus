#include "transforms.h"

#include <yt/cpp/roren/interface/transforms.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>
#include <bigrt/lib/utility/profiling/fiber_vcpu_time.h>

#include <logfeller/lib/chunk_splitter/chunk_splitter.h>

#include <library/cpp/logger/global/global.h>
#include <library/cpp/safe_stats/safe_stats.h>

#include <util/datetime/cputimer.h>
#include <util/string/escape.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/serialize.h>
#include <library/cpp/yt/small_containers/compact_vector.h>

#include <yt/yt/library/profiling/tag.h>

#include <util/ysaveload.h>

#include <type_traits>
#include <util/generic/xrange.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TParseLogfellerFn : public IDoFn<NBigRT::TMessageBatch, TLogfellerRecord>
{
public:
    TParseLogfellerFn() = default;

    explicit TParseLogfellerFn(TString chunkSplitterFormat, TParseLogFellerOptions options)
        : ChunkSplitterFormat_(std::move(chunkSplitterFormat))
        , Options_(std::move(options))
    { }

    void Start(TOutput<TLogfellerRecord>&) override
    {
        if (!ChunkSplitter_) {
            ChunkSplitter_ = NLogFeller::NChunkSplitter::CreateChunkSplitter(ChunkSplitterFormat_);
        }

        if (!Metrics_) {
            auto profiler = GetExecutionContext()->GetProfiler().WithTags(Options_.ProfilerTags);
            auto buildCounter = [&](const auto& sensor, TString defaultName) {
                if (sensor) {
                    return profiler
                        .WithTags(NYT::NProfiling::TTagSet{GetTagList(sensor->Tags)})
                        .Counter(sensor->Name);
                }
                return profiler.Counter(defaultName);
            };
            auto buildTimer = [&](const auto& sensor, TString defaultName) {
                if (sensor) {
                    return profiler
                        .WithTags(NYT::NProfiling::TTagSet{GetTagList(sensor->Tags)})
                        .TimeCounter(sensor->Name);
                }
                return profiler.TimeCounter(defaultName);
            };
            Metrics_ = TMetrics {
                buildCounter(Options_.InputCountSensor, ".input.rows_count"),
                buildCounter(Options_.InputCompressedBytesSensor, ".input.compressed_bytes"),
                buildCounter(Options_.UnpackOkCountSensor, ".input.chunks"),
                buildCounter(Options_.UnpackErrorCountSensor, ".input.broken_chunks"),
                buildCounter(Options_.UnpackedBytesSensor, ".input.uncompressed_bytes"),
                buildTimer(Options_.UnpackingTime, ".unpacking_vcpu_time"),
            };
        }
    }

    void Do(const NBigRT::TMessageBatch& messageBatch, TOutput<TLogfellerRecord>& output) override
    {
        for (auto message : messageBatch.Messages) {
            Metrics_->InputCount.Increment();
            Metrics_->InputCompressedBytes.Increment(message.Data.size());
            bool unpacked;
            {
                NBigRT::TFiberVCpuTimeGuard timer(Metrics_->UnpackingTime);
                unpacked = message.Unpack();
            }
            if (!unpacked) {
                // YT_LOG_DEBUG("Broken chunk");
                Metrics_->UnpackErrorCount.Increment();
                continue;
            }
            Metrics_->UnpackOkCount.Increment();
            Metrics_->UnpackedBytesSensor.Increment(message.Data.size());

            ParsedLogfeller_.CreateTime = message.CreateTimeMs;
            ParsedLogfeller_.WriteTime = message.WriteTimeMs;
            ParsedLogfeller_.DataMessageOffset = message.Offset;

            Y_ENSURE(message.Offset >= 0, "negative offset");

            TStringBuf chunk{message.Data};
            TStringBuf rawRecord, skip;
            NLogFeller::NChunkSplitter::TRecordContext recordContext;

            ui64 chunkIndex = 0;
            while (ChunkSplitter_->NextRecord(chunk, rawRecord, skip, ParsedLogfeller_.RecordContext)) {
                ParsedLogfeller_.Record.assign(rawRecord);
                ParsedLogfeller_.ChunkIndex = chunkIndex;
                output.Add(ParsedLogfeller_);

                ++chunkIndex;
            }
        }
    }

private:
    struct TMetrics
    {
        NYT::NProfiling::TCounter InputCount;
        NYT::NProfiling::TCounter InputCompressedBytes;

        NYT::NProfiling::TCounter UnpackOkCount;
        NYT::NProfiling::TCounter UnpackErrorCount;

        NYT::NProfiling::TCounter UnpackedBytesSensor;

        NYT::NProfiling::TTimeCounter UnpackingTime;
    };

private:
    TString ChunkSplitterFormat_;
    TParseLogFellerOptions Options_;

    Y_SAVELOAD_DEFINE_OVERRIDE(ChunkSplitterFormat_, Options_);

    NLogFeller::NChunkSplitter::TChunkSplitterPtr ChunkSplitter_;
    TLogfellerRecord ParsedLogfeller_;
    std::optional<TMetrics> Metrics_;
};

TParDoTransform<NBigRT::TMessageBatch, TLogfellerRecord> ParseLogfellerParDo(TString chunkSplitterFormat, TParseLogFellerOptions options)
{
    return MakeParDo<TParseLogfellerFn>(std::move(chunkSplitterFormat), std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class TUnpackMessagesFn : public IDoFn<NBigRT::TMessageBatch, TString>
{
public:
    TUnpackMessagesFn() = default;

    TUnpackMessagesFn(TUnpackMessagesOptions options)
        : Options_(std::move(options))
    {
    }

    void Start(TOutput<TString>&) override
    {
        Metrics_ = {};
    }

    void Do(const NBigRT::TMessageBatch& messageBatch, TOutput<TString>& output) override
    {
        for (auto messageData: messageBatch.Messages) {
            ++Metrics_.InputCount;
            if (!messageData.Unpack()) {
                ++Metrics_.UnpackErrorCount;
                continue;
            }

            ++Metrics_.UnpackOkCount;
            output.Add(messageData.Data);
        }
    }

    void Finish(TOutput<TString>&) override
    {
        TCounterUpdater updater(GetExecutionContext()->GetProfiler());
        updater.Update(Options_.InputCountSensor, Metrics_.InputCount);
        updater.Update(Options_.UnpackErrorCountSensor, Metrics_.UnpackErrorCount);
        updater.Update(Options_.UnpackOkCountSensor, Metrics_.UnpackOkCount);
    }

private:
    struct TMetrics {
        i64 InputCount = 0;
        i64 UnpackErrorCount = 0;
        i64 UnpackOkCount = 0;
    };

private:
    TUnpackMessagesOptions Options_;

    Y_SAVELOAD_DEFINE_OVERRIDE(Options_);

    TMetrics Metrics_;
};

TParDoTransform<NBigRT::TMessageBatch, TString> UnpackMessagesParDo(TUnpackMessagesOptions options)
{
    return MakeParDo<TUnpackMessagesFn>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
