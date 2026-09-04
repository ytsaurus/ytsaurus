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

using TNodesByAvailabilityGroup = THashMap<std::string, std::vector<TNodeTraverseDataPtr>>;
using TNodesByAvailabilityGroupBySource = THashMap<TStreamId, TNodesByAvailabilityGroup>;
using TSuppressedAvailabilityGroupsBySource = THashMap<TStreamId, THashSet<std::string>>;

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp GetPartitionEventWatermark(
    const TNodeTraverseDataPtr& node,
    const TComputationSpecPtr& spec,
    const TStreamId& sourceStreamId);

void HideEventWatermarkInplace(
    const TNodeTraverseDataPtr& node,
    const TSystemTimestamp& updateTime,
    const TComputationSpecPtr& spec,
    const TStreamId& sourceStreamId);

//! |suppressedGroups|, when given, receives the groups this call suppressed within its source domain:
//! their watermark is hidden here, and callers may stop publishing their errors too. A group every
//! partition of which is unavailable is suppressed only while that domain's caps allow it, so the set
//! is empty whenever those groups still gate the pipeline.
std::vector<TNodeTraverseDataPtr> ApplyAvailabilityGroupsEventWatermarkComputeRule(
    const TNodesByAvailabilityGroup& nodesByAvailabilityGroup,
    const TStreamId& sourceStreamId,
    const TComputationSpecPtr& spec,
    const NProfiling::TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger,
    THashSet<std::string>* suppressedGroups = nullptr);

std::vector<TNodeTraverseDataPtr> ApplyEventWatermarkComputeRule(
    const TNodesByAvailabilityGroupBySource& nodesByAvailabilityGroupBySource,
    const TComputationSpecPtr& spec,
    const NProfiling::TSensorsOwner& sensorsOwner,
    const NLogging::TLogger& logger,
    const IStatusErrorStatePtr& watermarkStallErrorState,
    TSuppressedAvailabilityGroupsBySource* suppressedGroupsBySource = nullptr);

std::optional<TSystemTimestamp> GetPartitionLastIdleTimestamp(
    const TNodeTraverseDataPtr& traverseData,
    const TComputationSpecPtr& spec,
    const TStreamId& sourceStreamId,
    // Return true even if emptiness is not stable.
    bool relaxed = false);

std::optional<TSystemTimestamp> GetPartitionLastUnavailableTimestamp(
    const TNodeTraverseDataPtr& traverseData,
    const TStreamId& sourceStreamId);

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

    //! Replaces the computation-owned part of the partition's dynamic spec; the
    //! job-manager-owned fields (e.g. FinishAfterCurrentEpoch) are carried over.
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
    virtual TNodesByAvailabilityGroupBySource GetNodesByAvailabilityGroupBySource(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
        const TFlowViewPtr& flowView) = 0;
    virtual std::optional<TNodeTraverseDataPtr> GetFuturePartitionsNodeTraverseData(const TFlowViewPtr& flowView);

    //! Availability groups the last traverse suppressed: the pipeline neither waits for their watermark
    //! nor needs their errors. Not the same as being fully unavailable — the caps decide whether a fully
    //! unavailable group is suppressed at all.
    //! Null until this process has completed a traverse, which is not the same as "nothing is
    //! suppressed": a controller that has just taken over knows nothing yet.
    const std::optional<TSuppressedAvailabilityGroupsBySource>& GetSuppressedAvailabilityGroupsBySource() const;

    IComputationController::TParametersPtr GetParametersBase() const final;
    IComputationController::TDynamicParametersPtr GetDynamicParametersBase() const final;

private:
    const TComputationControllerContextPtr Context_;
    const IComputationController::TParametersPtr Parameters_;
    const NProfiling::TSensorsOwner SensorsOwner_;
    //! Persistent error state raised while too many idle source partitions gate the watermark.
    const IStatusErrorStatePtr IdlePartitionsWatermarkStallErrorState_;
    std::optional<TSuppressedAvailabilityGroupsBySource> SuppressedAvailabilityGroupsBySource_;
    TAtomicIntrusivePtr<TDynamicComputationControllerContext> DynamicContext_;
    TAtomicIntrusivePtr<IComputationController::TDynamicParameters> DynamicParameters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
