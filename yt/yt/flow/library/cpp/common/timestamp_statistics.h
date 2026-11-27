#pragma once

#include "public.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TTimestampStatisticsRegistrationInfo, i64);

//! Class for collecting, aggregating and distributing timestamp statistics across all workers.
struct TTimestampStatistics final
{
    // Use integer arithmetic because perfect precision and reversibility.
    i64 AlignmentToEventTimestampBiasSum = 0;
    i64 MessageCount = 0;

    static TTimestampStatisticsRegistrationInfo ComputeRegistrationInfo(const TMessageMeta& messageMeta);

    void RegisterMessage(TTimestampStatisticsRegistrationInfo info);
    void UnregisterMessage(TTimestampStatisticsRegistrationInfo info);
    double EstimateAlignmentToEventTimestampBias() const; // AvgAligmentTimestamp + bias = AvgEventTimestamp.

    TTimestampStatistics& operator+=(const TTimestampStatistics& other);
};

////////////////////////////////////////////////////////////////////////////////

struct TTimestampStatisticsSerializer
    : public virtual NYTree::TExternalizedYsonStruct
{
    REGISTER_EXTERNALIZED_YSON_STRUCT(TTimestampStatistics, TTimestampStatisticsSerializer);

    static void Register(TRegistrar registrar);
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTimestampStatistics, TTimestampStatisticsSerializer);

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxStreamPriority = 15;
constexpr auto MaxStreamDelay = TDuration::Days(36525);

struct TStreamOrderingParams
{
    double Bias = 0;
    int Priority = 0;
    //! Zero disables the stale input boost.
    TDuration StaleInputBoostAge;
};

//! Returns the bound below which a message SystemTimestamp is stale; zero when the boost is disabled.
TSystemTimestamp GetStaleInputBoostBound(TInstant now, TDuration staleInputBoostAge);

//! Cross-stream ordering key: the non-stale bit, then the priority, then the stabilized timestamp.
//! A message is stale when |systemTimestamp| is below |staleBound|. A smaller key is more important.
TSystemTimestamp MakeOrderingKey(
    TSystemTimestamp alignmentTimestamp,
    TSystemTimestamp systemTimestamp,
    const TStreamOrderingParams& params,
    TSystemTimestamp staleBound);

//! Throttler priority from the input ordering key and the raw source read timestamp.
TSystemTimestamp ComputeThrottlerPriority(TSystemTimestamp inputKey, TSystemTimestamp sourceTimestamp);

double ComputeOrderingTimestampBias(
    const TStreamId& streamId,
    const TTimestampStatistics& timestampStatistics,
    const TInputOrderingSpecPtr& inputOrderingSpec);

THashMap<std::pair<TStreamId, TComputationId>, TStreamOrderingParams> ComputeStreamOrderingParams(
    const THashMap<TStreamId, TTimestampStatistics>& timestampStatistics,
    const TPipelineSpecPtr& pipelineSpec,
    const TDynamicPipelineSpecPtr& dynamicPipelineSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define TIMESTAMP_STATISTICS_INL_H_
#include "timestamp_statistics-inl.h"
#undef TIMESTAMP_STATISTICS_INL_H_
