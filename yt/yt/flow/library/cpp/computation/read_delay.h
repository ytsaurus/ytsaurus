#pragma once

#include "watermark_generator.h"

#include <yt/yt/flow/library/cpp/common/source.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/ordered_memory.h>

#include <yt/yt/flow/library/cpp/source_read_delay/estimator.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp GetReadDelayThreshold(
    const TWatermarkAlignmentSpecPtr& alignment,
    const TCallback<TSystemTimestamp(const TStreamId&)>& getWatermark);

TSystemTimestamp GetReadDelayBatchTimestamp(const std::vector<TMessage>& messages);

struct TReadDelayInput
{
    TMessageId LastMessageId;
    TInstant SystemTime;
    std::optional<TInstant> EventTime;
    TBatchStatistics Statistics;
};

std::vector<TReadDelayInput> PrepareReadDelayInputs(const std::vector<ISource::TMessageBatch>& batches);

TSourceReadDelayObservation MakeReadDelayObservation(
    const TInflightMetricsPtr& metrics,
    TInstant now,
    TSystemTimestamp threshold);

TError GetReadDelayEstimateWarning(const TSourceReadDelayEstimate& estimate);

TInflightStreamTraverseDataPtr BuildReadDelayInflight(
    const TInflightStreamTraverseDataPtr& inflight,
    const TSourceReadDelayEstimate& estimate);

////////////////////////////////////////////////////////////////////////////////

struct TReadDelayBatch
{
    TMessageId FirstMessageId;
    TSourceMessageBatchCookie BatchCookie;
    TBatchStatistics InputStatistics;
    std::vector<TMessage> CandidateOutputMessages;
    std::vector<bool> IsOutput;
    TSystemTimestamp Timestamp;
    TWatermarkGeneratorCookie WatermarkGeneratorCookie;
};

struct TReadDelayPublication
{
    size_t BatchCount = 0;
    size_t CandidateOutputCount = 0;
};

struct TReadDelayStatus
    : public NYTree::TYsonStruct
{
    i64 HeldCount{};
    i64 HeldByteSize{};
    i64 HeldBatchCount{};
    TSystemTimestamp EventThreshold;
    std::optional<TMessageId> FirstBlockingMessageId;
    std::optional<TSystemTimestamp> RequiredEventThreshold;
    bool Draining{};

    REGISTER_YSON_STRUCT(TReadDelayStatus);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReadDelayStatus);

//! Ordered publication admission and exact accounting of already-read original input.
class TReadDelayQueue
{
public:
    void Push(TReadDelayBatch batch);
    TReadDelayBatch Pop();
    bool Empty() const;

    TReadDelayPublication GetPublishablePrefix(
        TSystemTimestamp threshold,
        bool draining,
        const TIntrusivePtr<TOrderedMemory<TMessageId, TSystemTimestamp>>& timestampMemory) const;

    //! Reports the retained queue after publishing its admissible prefix.
    TReadDelayStatusPtr BuildStatus(TSystemTimestamp threshold, bool draining) const;

private:
    std::deque<TReadDelayBatch> Batches_;
    TBatchStatistics Held_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
