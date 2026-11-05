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

double ComputeOrderingTimestampBias(
    const TStreamId& streamId,
    const TTimestampStatistics& timestampStatistics,
    const TInputOrderingSpecPtr& inputOrderingSpec);

THashMap<std::pair<TStreamId, TComputationId>, double> ComputeOrderingTimestampBiases(
    const THashMap<TStreamId, TTimestampStatistics>& timestampStatistics,
    const TPipelineSpecPtr& pipelineSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
