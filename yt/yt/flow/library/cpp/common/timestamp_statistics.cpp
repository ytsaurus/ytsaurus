#include "timestamp_statistics.h"

#include "flow_view.h"
#include "message.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

constinit i64 MaxAdequateTotalBias = std::numeric_limits<i64>::max() / 4;

void ValidateTimestamp(TSystemTimestamp timestamp, const char* type)
{
    YT_VERIFY(timestamp.Underlying() < MaxAdequateTimestamp.Underlying(),
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

double ComputeOrderingTimestampBias(
    const TStreamId& streamId,
    const TTimestampStatistics& timestampStatistics,
    const TInputOrderingSpecPtr& inputOrderingSpec)
{
    auto delay = GetOrDefault(inputOrderingSpec->StreamDelays, streamId);
    return timestampStatistics.EstimateAlignmentToEventTimestampBias() + delay.SecondsFloat();
}

THashMap<std::pair<TStreamId, TComputationId>, double> ComputeOrderingTimestampBiases(
    const THashMap<TStreamId, TTimestampStatistics>& timestampStatistics,
    const TPipelineSpecPtr& pipelineSpec)
{
    THashMap<std::pair<TStreamId, TComputationId>, double> timestampBiases;
    for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
        for (const auto& streamId : computationSpec->InputStreamIds) {
            auto statisticsIt = timestampStatistics.find(streamId);
            if (statisticsIt == timestampStatistics.end()) {
                continue;
            }
            timestampBiases[std::pair(streamId, computationId)] = ComputeOrderingTimestampBias(
                streamId,
                statisticsIt->second,
                computationSpec->InputOrdering);
        }
    }
    return timestampBiases;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
