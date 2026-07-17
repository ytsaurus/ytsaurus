#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/library/profiling/sensors_owner/sensors_owner.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp GetPartitionEventWatermark(
    const TNodeTraverseDataPtr& node,
    const TComputationSpecPtr& spec);

void HideEventWatermarkInplace(
    const TNodeTraverseDataPtr& node,
    const TSystemTimestamp& updateTime,
    const TComputationSpecPtr& spec);

std::vector<TNodeTraverseDataPtr> ApplyAvailabilityGroupsEventWatermarkComputeRule(
    const THashMap<std::string, std::vector<TNodeTraverseDataPtr>>& nodesByAvailabilityGroup,
    const TComputationSpecPtr& spec,
    const NProfiling::TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger);

std::vector<TNodeTraverseDataPtr> ApplyIdlePartitionsRule(
    const std::vector<TNodeTraverseDataPtr>& nodes,
    const TComputationSpecPtr& spec,
    const NProfiling::TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger,
    const IStatusErrorStatePtr& watermarkStallErrorState);

std::vector<TNodeTraverseDataPtr> ApplyLateDataPartitionsRule(
    const std::vector<TNodeTraverseDataPtr>& nodes,
    const TComputationSpecPtr& spec,
    const NProfiling::TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger);

std::vector<TNodeTraverseDataPtr> ApplyEventWatermarkComputeRule(
    const THashMap<std::string, std::vector<TNodeTraverseDataPtr>>& nodesByAvailabilityGroup,
    const TComputationSpecPtr& spec,
    const NProfiling::TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger,
    const IStatusErrorStatePtr& watermarkStallErrorState);

std::optional<TSystemTimestamp> GetPartitionLastIdleTimestamp(
    const TNodeTraverseDataPtr& traverseData,
    const TComputationSpecPtr& spec,
    // Return true even if emptiness is not stable.
    bool relaxed = false);

std::optional<TSystemTimestamp> GetPartitionLastUnavailableTimestamp(
    const TNodeTraverseDataPtr& traverseData,
    const TComputationSpecPtr& spec,
    const NLogging::TLogger& logger);

THashMap<TStreamId, TStreamTraverseDataMetricsPtr> ComputeStreamMetrics(
    const std::vector<TNodeTraverseDataPtr>& traverseData,
    const TComputationSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

class TComputationControllerBase
    : public IComputationController
{
private:
    struct TExtendedParameters
        : public virtual IComputationController::TDynamicParameters
    {
        double WeightMultiplier{};
        double InterruptingWeightMultiplier{};

        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedParameters);

    TComputationControllerBase(
        TComputationControllerContextPtr context,
        TDynamicComputationControllerContextPtr dynamicContext);

    TComputationControllerContextPtr GetContext() const;
    TDynamicComputationControllerContextPtr GetDynamicContext() const;
    TComputationSpecPtr GetSpec() const;
    TDynamicComputationSpecPtr GetDynamicSpec() const;

    const TComputationId& GetComputationId() const;

    void InterruptPartition(const TFlowViewPtr& flowView, const TPartitionId& partitionId);

    // Like #InterruptPartition, but retires the partition for good: its source-key state is erased
    // rather than preserved for a possible recreation.
    void CompletePartition(const TFlowViewPtr& flowView, const TPartitionId& partitionId);

    void CreateSourcePartition(
        const TFlowViewPtr& flowView,
        const TKey& sourceKey,
        const NYTree::IMapNodePtr& dynamicComputationPartitionSpec);

    void CreateRangePartition(
        const TFlowViewPtr& flowView,
        const TKey& lowerKey,
        const TKey& upperKey,
        const NYTree::IMapNodePtr& dynamicComputationPartitionSpec);

    void UpdateDynamicPartitionSpec(
        const TFlowViewPtr& flowView,
        const TPartitionId& partitionId,
        const NYTree::IMapNodePtr& dynamicComputationPartitionSpec);

    TProcessPartitionTraverseDataResultPtr ProcessPartitionTraverseData(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
        const TFlowViewPtr& flowView) override;

    double ComputePartitionWeight(const TPartitionId& partitionId, const TFlowViewPtr& flowView) override;

    void Init(IInitContextPtr initContext) override;
    void Sync() override;
    void Commit() override;

protected:
    const NLogging::TLogger Logger;

protected:
    virtual THashMap<std::string, std::vector<TNodeTraverseDataPtr>> GetNodesByAvailabilityGroup(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
        const TFlowViewPtr& flowView);
    virtual std::optional<TNodeTraverseDataPtr> GetFuturePartitionsNodeTraverseData(const TFlowViewPtr& flowView);

    IComputationController::TParametersPtr GetParametersBase() const final;
    IComputationController::TDynamicParametersPtr GetDynamicParametersBase() const final;

private:
    const TComputationControllerContextPtr Context_;
    const IComputationController::TParametersPtr Parameters_;
    const NProfiling::TSensorsOwner SensorsOwner_;
    //! Persistent error state raised while too many idle source partitions gate the watermark.
    const IStatusErrorStatePtr IdlePartitionsWatermarkStallErrorState_;
    TAtomicIntrusivePtr<TDynamicComputationControllerContext> DynamicContext_;
    TAtomicIntrusivePtr<IComputationController::TDynamicParameters> DynamicParameters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
