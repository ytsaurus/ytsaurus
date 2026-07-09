#pragma once

#include "graph_entity_id.h"
#include "intermediate_description.h"

#include <yt/yt/flow/library/cpp/common/describe_traits.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/misc/node_info.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <map>
#include <vector>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

//! Returns a map from vertex to its 0-based position in a deterministic
//! topological order of the DAG. Independent nodes are ordered lexicographically.
//! All vertices (including isolated ones) must be present as keys in #graph.
template <class TVertex>
std::map<TVertex, size_t> BuildDeterministicTopologicalIndex(
    const std::map<TVertex, std::vector<TVertex>>& graph);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPartitionDescribeState,
    (Executing)
    // Interrupting or Completing.
    (Transient)
    (Completed)
    (Interrupted)
    (Unknown)
);

DEFINE_ENUM(EPartitionJobState,
    (Unknown)
    (Recovering)
    (Working)
    (Stopped)
);

////////////////////////////////////////////////////////////////////////////////

struct TMessage
    : public NYTree::TYsonStructLite
{
    std::string Text;
    std::optional<NYson::TYsonString> Yson;
    std::optional<TError> Error;
    std::optional<std::string> MarkdownText;
    NLogging::ELogLevel Level{};

    REGISTER_YSON_STRUCT_LITE(TMessage);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TPartitionsStats
    : public NYTree::TYsonStructLite
{
    i64 Count{};
    THashMap<EPartitionDescribeState, i64> CountByState;

    REGISTER_YSON_STRUCT_LITE(TPartitionsStats);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TComputationDescription
    : public NYTree::TYsonStructLite
{
    TGraphEntityId Id; // Graph entity ID in the UI: ComputationPrefix + computationId, e.g. "cmp-Computation_3".
    std::string Name;
    std::string ClassName;
    std::string Description;
    NLogging::ELogLevel Status{};
    std::vector<TMessage> Messages;
    std::string GroupBySchemaStr;
    double EpochPerSecond{};
    TPartitionsStats PartitionsStats;
    TNodePerformanceMetricsPtr Metrics;
    double CpuUsage{};
    bool HighlightCpuUsage{};
    i64 MemoryUsage{};
    bool HighlightMemoryUsage{};

    REGISTER_YSON_STRUCT_LITE(TComputationDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TPerformanceMetricsBase
    : public NYTree::TYsonStructLite
{
    double CpuUsage{};
    i64 MemoryUsage{};
    double MessagesPerSecond{};
    double BytesPerSecond{};

    REGISTER_YSON_STRUCT_LITE(TPerformanceMetricsBase);

    static void Register(TRegistrar registrar);
};

#define YTFLOW_ITERATE_PERFORMANCE_METRICS_FIELDS(XX) \
    XX(CpuUsage)                                      \
    XX(MemoryUsage)                                   \
    XX(MessagesPerSecond)                             \
    XX(BytesPerSecond)

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDescription
    : public TPerformanceMetricsBase
{
    TPartitionId PartitionId;
    TComputationId ComputationId;

    std::optional<std::string> LowerKey;
    std::optional<std::string> UpperKey;
    std::optional<std::string> SourceKey;
    std::string KeyOrRange;
    std::string LexicographicallySerializedKeyOrRange;

    std::optional<TJobId> CurrentJobId;
    std::optional<std::string> CurrentWorkerAddress;
    std::optional<TIncarnationId> CurrentWorkerIncarnationId;
    EPartitionState State{};
    EPartitionJobState JobState{};

    NLogging::ELogLevel Status{};

    TInstant LastRetryableErrorInstant;
    TInstant PreviousJobFailInstant;
    TInstant PreviousRebalancingInstant;

    REGISTER_YSON_STRUCT_LITE(TPartitionDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TWorkerDescription
    : public TPerformanceMetricsBase
    , public TNodeInfoBase
{
    std::string Address; // Compatibility for UI.
    std::vector<TWorkerGroupId> Groups;
    TInstant RegisterTime;
    NLogging::ELogLevel Status{};

    // Heavy field. It is cleared in DescribeWorkers.
    std::vector<TPartitionDescription> Partitions;

    REGISTER_YSON_STRUCT_LITE(TWorkerDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Returns a UI-friendly copy of #parameters with the source-class-specific describe traits
//! (resolved from the registry by #sourceClassName) applied to turn references into clickable links.
//! Falls back to the generic traits for plain classes.
NYTree::INodePtr MakeSourceLinks(const NYTree::INodePtr& parameters, const std::string& sourceClassName, const TDescribeTraitsContext& context);

//! Same as #MakeSourceLinks, but for sink-class-specific describe traits.
NYTree::INodePtr MakeSinkLinks(const NYTree::INodePtr& parameters, const std::string& sinkClassName, const TDescribeTraitsContext& context);

//! Returns a UI-friendly copy of the computation spec: applies each entity's describe traits
//! (from the registry) over its parameters to turn references into clickable links.
NYTree::INodePtr MakePrettyForUIComputationSpec(const TComputationSpecPtr& computationSpec, const TDescribeTraitsContext& context);

//! Returns a UI-friendly copy of the computation's dynamic spec: applies the describe traits of the
//! computation and of each dynamic source/sink (resolved from the static #staticSpec by stream/sink id)
//! over their parameters to turn references into clickable links.
NYTree::INodePtr MakePrettyForUIDynamicComputationSpec(
    const TDynamicComputationSpecPtr& dynamicSpec,
    const TComputationSpecPtr& staticSpec,
    const TDescribeTraitsContext& context);

////////////////////////////////////////////////////////////////////////////////

void ValidateSpecs(TPipelineSpecPtr pipelineSpec, TDynamicPipelineSpecPtr dynamicPipelineSpec, std::vector<TMessage>& messages);

////////////////////////////////////////////////////////////////////////////////

THashSet<TComputationId> GetTopForHighlighting(const THashMap<TComputationId, double>& values);

void FillRetryableErrors(const THashMap<std::string, TError>& errors, std::vector<TMessage>& messages, NLogging::ELogLevel* status = nullptr);
void FillJobFailErrors(const THashMap<EJobFinishReason, TError>& errors, std::vector<TMessage>& messages, NLogging::ELogLevel* status = nullptr);

THashMap<TComputationId, TComputationDescription> MakeComputationDescriptions(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& intermediateDescriptions);

void FillPartitionDescription(
    const TPartitionIntermediateDescription& intermediateDescription,
    TPartitionDescription& description,
    std::vector<TMessage>& messages);

void FillTopHeavyHittersMessages(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& intermediateDescriptions,
    const TComputationId& computationId,
    TComputationDescription& computationDescription);

////////////////////////////////////////////////////////////////////////////////

void FillWorkerErrors(
    const TFlowViewPtr& flowView,
    const TWorkerPtr& worker,
    std::vector<TMessage>& messages,
    NLogging::ELogLevel* status = nullptr);

void FillWorkerDescription(
    const TFlowViewPtr& flowView,
    const TWorkerPtr& worker,
    const std::vector<TPartitionIntermediateDescription>& partitionDescriptions,
    TWorkerDescription& description,
    std::vector<TMessage>& messages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
