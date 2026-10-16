#pragma once

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

//! All partition info collected in one struct.
struct TPartitionIntermediateDescription
    : public NYTree::TYsonStructLite
{
    TPartitionPtr Partition;
    TJobPtr Job;
    TPartitionJobStatusPtr PartitionJobStatus;
    TPartitionEphemeralStatePtr PartitionEphemeralState;

    REGISTER_YSON_STRUCT_LITE(TPartitionIntermediateDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

TPartitionIntermediateDescription GetPartitionIntermediateDescription(const TFlowViewPtr& flowView, const TPartitionId& partitionId);

TPartitionIntermediateDescription GetPartitionIntermediateDescription(const TFlowViewPtr& flowView, const TPartitionPtr& partition);

//! Get all partitions, grouped by worker. If #worker is set, only this worker will be present in result.
THashMap<std::string, std::vector<TPartitionIntermediateDescription>> GetWorkersPartitionIntermediateDescriptions(
    const TFlowViewPtr& flowView,
    const std::optional<std::string>& worker = std::nullopt);

//! Get all partitions, grouped by computation. If #computation is set, only this computation will be present in result.
THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>> GetComputationPartitionIntermediateDescriptions(
    const TFlowViewPtr& flowView,
    const std::optional<TComputationId>& computation = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
