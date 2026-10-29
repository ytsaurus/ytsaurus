#include "read_delay.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp GetReadDelayThreshold(
    const TWatermarkAlignmentSpecPtr& alignment,
    const TCallback<TSystemTimestamp(const TStreamId&)>& getWatermark)
{
    auto threshold = InfinitySystemTimestamp;
    if (alignment && alignment->ReadDelays) {
        for (const auto& [streamId, delay] : *alignment->ReadDelays) {
            auto watermark = getWatermark(streamId);
            threshold = std::min(threshold, TSystemTimestamp(std::max(watermark.Underlying(), delay.Seconds()) - delay.Seconds()));
        }
    }
    return threshold;
}

TSystemTimestamp GetReadDelayBatchTimestamp(const std::vector<TMessage>& messages)
{
    auto timestamp = ZeroSystemTimestamp;
    for (const auto& message : messages) {
        timestamp = std::max(timestamp, message.EventTimestamp);
    }
    return timestamp;
}

std::vector<TReadDelayInput> PrepareReadDelayInputs(const std::vector<ISource::TMessageBatch>& batches)
{
    std::vector<TReadDelayInput> inputs;
    inputs.reserve(batches.size());
    for (const auto& batch : batches) {
        YT_VERIFY(!batch.Messages.empty());
        auto statistics = batch.OriginalStatistics.value_or(TBatchStatistics{});
        if (!batch.OriginalStatistics) {
            statistics.Count = std::ssize(batch.Messages);
            for (const auto& message : batch.Messages) {
                statistics.ByteSize += message->ByteSize;
            }
        }
        inputs.push_back({
            .LastMessageId = batch.Messages.back()->MessageId,
            .SystemTime = TInstant::Seconds(batch.Messages.front()->SystemTimestamp.Underlying()),
            .Statistics = statistics,
        });
    }
    return inputs;
}

TSourceReadDelayObservation MakeReadDelayObservation(
    const TInflightMetricsPtr& metrics,
    TInstant now,
    TSystemTimestamp threshold)
{
    TSourceReadDelayObservation observation{
        .ObservationTime = now,
        .ReadDelayEventThreshold = TInstant::Seconds(threshold.Underlying()),
    };
    if (metrics->NewCountPerSec && metrics->NewBytesPerSec) {
        observation.NewRate = TSourceWork{*metrics->NewCountPerSec, *metrics->NewBytesPerSec};
    }
    if (metrics->ReadyCount && metrics->ReadyByteSize) {
        observation.Unread = TSourceWork{static_cast<double>(*metrics->ReadyCount), static_cast<double>(*metrics->ReadyByteSize)};
    }
    return observation;
}

TError GetReadDelayEstimateWarning(const TSourceReadDelayEstimate& estimate)
{
    if (!estimate.CountDensityMismatch && !estimate.BytesDensityMismatch && !estimate.EventOffsetDeviationSeconds) {
        return {};
    }
    return TError("Read-delay Offered and Ready estimates may be inaccurate: observed input density or batch timestamps are unstable")
        .With("count_density_mismatch", estimate.CountDensityMismatch)
        .With("bytes_density_mismatch", estimate.BytesDensityMismatch)
        .With("event_offset_deviation_seconds", estimate.EventOffsetDeviationSeconds);
}

TInflightStreamTraverseDataPtr BuildReadDelayInflight(
    const TInflightStreamTraverseDataPtr& inflight,
    const TSourceReadDelayEstimate& estimate)
{
    auto result = NYTree::CloneYsonStruct(inflight);
    auto& metrics = *result->InflightMetrics;
    metrics.OfferedCountPerSec = estimate.Offered ? std::optional(estimate.Offered->Count) : std::nullopt;
    metrics.OfferedBytesPerSec = estimate.Offered ? std::optional(estimate.Offered->Bytes) : std::nullopt;
    const auto bounded = [] (double estimated, i64 unread) {
        return estimated >= unread ? unread : static_cast<i64>(estimated);
    };
    metrics.ReadyCount = estimate.Ready && inflight->InflightMetrics->ReadyCount
        ? std::optional(bounded(estimate.Ready->Count, *inflight->InflightMetrics->ReadyCount))
        : std::nullopt;
    metrics.ReadyByteSize = estimate.Ready && inflight->InflightMetrics->ReadyByteSize
        ? std::optional(bounded(estimate.Ready->Bytes, *inflight->InflightMetrics->ReadyByteSize))
        : std::nullopt;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TReadDelayStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("held_count", &TThis::HeldCount)
        .Default(0);
    registrar.Parameter("held_byte_size", &TThis::HeldByteSize)
        .Default(0);
    registrar.Parameter("held_batch_count", &TThis::HeldBatchCount)
        .Default(0);
    registrar.Parameter("event_threshold", &TThis::EventThreshold)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("first_blocking_message_id", &TThis::FirstBlockingMessageId)
        .Default();
    registrar.Parameter("required_event_threshold", &TThis::RequiredEventThreshold)
        .Default();
    registrar.Parameter("draining", &TThis::Draining)
        .Default(false);
}

void TReadDelayQueue::Push(TReadDelayBatch batch)
{
    Held_ += batch.InputStatistics;
    Batches_.push_back(std::move(batch));
}

TReadDelayBatch TReadDelayQueue::Pop()
{
    YT_VERIFY(!Batches_.empty());
    auto batch = std::move(Batches_.front());
    Batches_.pop_front();
    Held_.Count -= batch.InputStatistics.Count;
    Held_.ByteSize -= batch.InputStatistics.ByteSize;
    return batch;
}

bool TReadDelayQueue::Empty() const
{
    return Batches_.empty();
}

TReadDelayPublication TReadDelayQueue::GetPublishablePrefix(
    TSystemTimestamp threshold,
    bool draining,
    const TIntrusivePtr<TOrderedMemory<TMessageId, TSystemTimestamp>>& timestampMemory) const
{
    TReadDelayPublication result;
    for (const auto& batch : Batches_) {
        const bool canPublish = draining
            ? timestampMemory->IsRegistered(batch.FirstMessageId)
            : batch.Timestamp <= threshold;
        if (!canPublish) {
            break;
        }
        ++result.BatchCount;
        result.CandidateOutputCount += batch.CandidateOutputMessages.size();
    }
    return result;
}

TReadDelayStatusPtr TReadDelayQueue::BuildStatus(TSystemTimestamp threshold, bool draining) const
{
    auto status = New<TReadDelayStatus>();
    status->HeldCount = Held_.Count;
    status->HeldByteSize = Held_.ByteSize;
    status->HeldBatchCount = std::ssize(Batches_);
    status->EventThreshold = threshold;
    status->Draining = draining;
    if (!Batches_.empty()) {
        status->FirstBlockingMessageId = Batches_.front().FirstMessageId;
        if (!draining) {
            status->RequiredEventThreshold = Batches_.front().Timestamp;
        }
    }
    return status;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
