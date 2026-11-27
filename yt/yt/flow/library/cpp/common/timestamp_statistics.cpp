#include "timestamp_statistics.h"

#include "flow_view.h"
#include "message.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

constinit i64 MaxAdequateTotalBias = std::numeric_limits<i64>::max() / 4;

void ValidateTimestamp(TSystemTimestamp timestamp, const char* type)
{
    YT_VERIFY(timestamp.Underlying() <= MaxAdequateTimestamp.Underlying(),
        Format("%v timestamp is not adequate (Timestamp: %v)", type, TInstant::Seconds(timestamp.Underlying())));
}

void ValidateTotalBias(i64 totalBias)
{
    YT_VERIFY(std::abs(totalBias) < MaxAdequateTotalBias, Format("Total bias is not adequate (TotalBias: %v)", totalBias));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTimestampStatisticsRegistrationInfo TTimestampStatistics::ComputeRegistrationInfo(const TMessageMeta& messageMeta)
{
    ValidateTimestamp(messageMeta.EventTimestamp, "EventTimestamp");
    ValidateTimestamp(messageMeta.AlignmentTimestamp, "AlignmentTimestamp");
    i64 bias = static_cast<i64>(messageMeta.EventTimestamp.Underlying()) - static_cast<i64>(messageMeta.AlignmentTimestamp.Underlying());
    return TTimestampStatisticsRegistrationInfo(bias);
}

void TTimestampStatistics::RegisterMessage(TTimestampStatisticsRegistrationInfo info)
{
    AlignmentToEventTimestampBiasSum += info.Underlying();
    ValidateTotalBias(AlignmentToEventTimestampBiasSum);
    MessageCount += 1;
}

void TTimestampStatistics::UnregisterMessage(TTimestampStatisticsRegistrationInfo info)
{
    AlignmentToEventTimestampBiasSum -= info.Underlying();
    ValidateTotalBias(AlignmentToEventTimestampBiasSum);
    MessageCount -= 1;
    YT_VERIFY(MessageCount >= 0);
}

double TTimestampStatistics::EstimateAlignmentToEventTimestampBias() const
{
    if (MessageCount == 0) {
        return 0.0;
    }
    return static_cast<double>(AlignmentToEventTimestampBiasSum) / MessageCount;
}

TTimestampStatistics& TTimestampStatistics::operator+=(const TTimestampStatistics& other)
{
    AlignmentToEventTimestampBiasSum += other.AlignmentToEventTimestampBiasSum;
    ValidateTotalBias(AlignmentToEventTimestampBiasSum);
    MessageCount += other.MessageCount;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void TTimestampStatisticsSerializer::Register(TRegistrar registrar)
{
    registrar.ExternalBaseClassParameter("alignment_to_event_timestamp_bias_sum", &TTimestampStatistics::AlignmentToEventTimestampBiasSum);
    registrar.ExternalBaseClassParameter("message_count", &TTimestampStatistics::MessageCount);
}

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp GetStaleInputBoostBound(TInstant now, TDuration staleInputBoostAge)
{
    // A zero age would make every message stale.
    if (!staleInputBoostAge) {
        return ZeroSystemTimestamp;
    }

    // TInstant subtraction saturates at zero, so a huge age also disables the boost.
    return TSystemTimestamp((now - staleInputBoostAge).Seconds());
}

TSystemTimestamp ComputeThrottlerPriority(TSystemTimestamp inputKey, TSystemTimestamp sourceTimestamp)
{
    // Zero means the source has not read anything yet; keep it raw so the job gets top priority.
    // The read timestamp is not an in-flight message, so it is never stale.
    auto sourceKey = sourceTimestamp;
    if (sourceTimestamp != InfinitySystemTimestamp && sourceTimestamp != ZeroSystemTimestamp) {
        sourceKey = MakeOrderingKey(sourceTimestamp, sourceTimestamp, /*params*/ {}, /*staleBound*/ ZeroSystemTimestamp);
    }

    auto priority = std::min(inputKey, sourceKey);
    // InfinitySystemTimestamp means we don't have any information yet.
    // So it's better to have top priority to get at least some data to process.
    if (priority == InfinitySystemTimestamp) {
        priority = ZeroSystemTimestamp;
    }

    return priority;
}

double ComputeOrderingTimestampBias(
    const TStreamId& streamId,
    const TTimestampStatistics& timestampStatistics,
    const TInputOrderingSpecPtr& inputOrderingSpec)
{
    auto delay = GetOrDefault(inputOrderingSpec->StreamDelays, streamId);
    return timestampStatistics.EstimateAlignmentToEventTimestampBias() + delay.SecondsFloat();
}

THashMap<std::pair<TStreamId, TComputationId>, TStreamOrderingParams> ComputeStreamOrderingParams(
    const THashMap<TStreamId, TTimestampStatistics>& timestampStatistics,
    const TPipelineSpecPtr& pipelineSpec,
    const TDynamicPipelineSpecPtr& dynamicPipelineSpec)
{
    // The same default object a job gets when its computation has no dynamic spec entry.
    const auto defaultStaleInputBoostAge = New<TDynamicComputationSpec>()->StaleInputBoostAge;
    THashMap<std::pair<TStreamId, TComputationId>, TStreamOrderingParams> orderingParams;
    for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
        auto dynamicComputationSpecIt = dynamicPipelineSpec->Computations.find(computationId);
        auto staleInputBoostAge = dynamicComputationSpecIt == dynamicPipelineSpec->Computations.end()
            ? defaultStaleInputBoostAge
            : dynamicComputationSpecIt->second->StaleInputBoostAge;
        for (const auto& streamId : computationSpec->InputStreamIds) {
            auto statisticsIt = timestampStatistics.find(streamId);
            if (statisticsIt == timestampStatistics.end()) {
                continue;
            }
            orderingParams[std::pair(streamId, computationId)] = TStreamOrderingParams{
                .Bias = ComputeOrderingTimestampBias(
                    streamId,
                    statisticsIt->second,
                    computationSpec->InputOrdering),
                .Priority = GetOrDefault(computationSpec->InputOrdering->StreamPriorities, streamId),
                .StaleInputBoostAge = staleInputBoostAge,
            };
        }
    }
    return orderingParams;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
