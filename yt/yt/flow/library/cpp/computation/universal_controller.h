#pragma once

#include "public.h"

#include "controller_base.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TKey MakeUniversalPartitionKey(const TStreamId& streamId, const TKey& sourceKey);
std::pair<TStreamId, TKey> SplitUniversalPartitionKey(const TKey& partitionKey);

////////////////////////////////////////////////////////////////////////////////

struct TAvailabilityGroupOrigin
{
    TStreamId StreamId;
    std::string Group;

    bool operator==(const TAvailabilityGroupOrigin&) const = default;
};

THashMap<TStreamId, THashSet<std::string>> MigrateLegacySuppressedAvailabilityGroups(
    const THashSet<std::string>& legacySuppressedAvailabilityGroups,
    const std::vector<TAvailabilityGroupOrigin>& currentOrigins);

////////////////////////////////////////////////////////////////////////////////

struct TUniversalComputationControllerState
    : public virtual NYTree::TYsonStruct
{
    THashMap<TStreamId, NYTree::INodePtr> Sources;
    THashMap<TSinkId, NYTree::INodePtr> Sinks;

    REGISTER_YSON_STRUCT(TUniversalComputationControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationControllerState);

////////////////////////////////////////////////////////////////////////////////

//! Persisted source-partitioning and sink-topology state.
struct TUniversalComputationControllerPartitioningState
    : public NYTree::TYsonStruct
{
    TIntrusivePtr<TVersionedValue<THashMap<TSinkId, i64>>> SinkChannelCounts;
    THashSet<std::string> SuppressedAvailabilityGroups;
    TSuppressedAvailabilityGroupsBySource SuppressedAvailabilityGroupsBySource;

    REGISTER_YSON_STRUCT(TUniversalComputationControllerPartitioningState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationControllerPartitioningState);

////////////////////////////////////////////////////////////////////////////////

class TUniversalComputationController
    : public TComputationControllerBase
{
private:
    struct TExtendedParameters
        : public virtual TComputationControllerBase::TParameters
    {
        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar registrar);
    };

    struct TExtendedDynamicParameters
        : public virtual TComputationControllerBase::TDynamicParameters
        , public virtual TPartitioningSpec
    {
        REGISTER_YSON_STRUCT(TExtendedDynamicParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedDynamicParameters);

    TUniversalComputationController(
        TComputationControllerContextPtr context,
        TDynamicComputationControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;

    void UpdateWatermarkState(TWatermarkStatePtr watermarkState) final;
    // thread safe
    TWatermarkStatePtr GetWatermarkState();

    TPartitioningTopology DescribePartitioningTopology() final;

    TPartitioningDescription DescribePartitioning(
        const TPartitioningStatus& status) final;

protected:
    //! Origin of a universal partition key's availability group, or null if its source stream no longer
    //! exists.
    std::optional<TAvailabilityGroupOrigin> GetAvailabilityGroupOrigin(const TKey& partitionKey) const;

    TNodesByAvailabilityGroupBySource GetNodesByAvailabilityGroupBySource(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
        const TFlowViewPtr& flowView) final;
    std::optional<TNodeTraverseDataPtr> GetFuturePartitionsNodeTraverseData(const TFlowViewPtr& flowView) final;

private:
    static THashMap<TStreamId, ISourceControllerPtr> CreateSources(
        const TComputationControllerContextPtr& context,
        const TComputationSpecPtr& spec,
        const TDynamicComputationSpecPtr& dynamicSpec);
    static THashMap<TSinkId, ISinkControllerPtr> CreateSinks(
        const TComputationControllerContextPtr& context,
        const TComputationSpecPtr& spec,
        const TDynamicComputationSpecPtr& dynamicSpec);

    void NotifySourcesAboutSuppressedGroups(
        const TSuppressedAvailabilityGroupsBySource& groupsByStream);
    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> GetSourcePartitionKeys() const;
    THashMap<TSinkId, i64> GetSinkChannelCounts() const;

    bool UsesRangePartitioning() const;

private:
    TAtomicIntrusivePtr<TWatermarkState> WatermarkState_;
    THashMap<TStreamId, ISourceControllerPtr> Sources_;
    THashMap<TSinkId, ISinkControllerPtr> Sinks_;
    TMutableStateClient<TUniversalComputationControllerPartitioningState> PartitioningState_;
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
