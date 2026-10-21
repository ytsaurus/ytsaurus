#pragma once

#include "spec.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! A process function running inside a graph; fields mirror those of #TComputationSpec.
struct TProcessFunctionGraphNodeSpec
    : public NYTree::TYsonStruct
{
    std::string ProcessingFunction;
    NYTree::IMapNodePtr ProcessingFunctionParameters;
    THashSet<TStreamId> InputStreamIds;
    THashSet<TStreamId> OutputStreamIds;
    THashMap<TStreamId, TTimerSpecPtr> TimerStreams;
    THashMap<TStreamId, THashSet<TStreamId>> StreamsDependency;

    REGISTER_YSON_STRUCT(TProcessFunctionGraphNodeSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessFunctionGraphNodeSpec)

//! Process functions of one computation connected by stream IDs.
struct TProcessFunctionGraphSpec
    : public NYTree::TYsonStruct
{
    THashMap<std::string, TProcessFunctionGraphNodeSpecPtr> Nodes;

    REGISTER_YSON_STRUCT(TProcessFunctionGraphSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessFunctionGraphSpec)

//! Static parameters of the graph process function generated from #TComputationSpec::ProcessFunctionGraph.
struct TProcessFunctionGraphParameters
    : public NYTree::TYsonStruct
{
    TProcessFunctionGraphSpecPtr Graph;
    //! Specs of all streams read or written by graph nodes.
    THashMap<TStreamId, TStreamSpecPtr> Streams;

    REGISTER_YSON_STRUCT(TProcessFunctionGraphParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessFunctionGraphParameters)

struct TProcessFunctionGraphDynamicParameters
    : public NYTree::TYsonStruct
{
    THashMap<std::string, NYTree::IMapNodePtr> Nodes;

    REGISTER_YSON_STRUCT(TProcessFunctionGraphDynamicParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessFunctionGraphDynamicParameters)

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf ProcessFunctionGraphFunctionName = "NYT::NFlow::TGraphProcessFunction";

//! Returns the computation timer stream ID of timer |timerId| of node |nodeId|.
TStreamId MakeProcessFunctionGraphTimerId(TStringBuf nodeId, const TStreamId& timerId);

//! Returns the local inputs and timers that output or timer |streamId| of |node| depends on.
THashSet<TStreamId> GetProcessFunctionGraphNodeDependencies(
    const TProcessFunctionGraphNodeSpecPtr& node,
    const TStreamId& streamId);

//! Graph structure bound to the input and output streams of its computation.
struct TProcessFunctionGraphTopology
{
    //! Node IDs in a deterministic topological order.
    std::vector<std::string> Order;
    //! Maps every node output stream to its node.
    THashMap<TStreamId, std::string> Producers;
    //! Maps every stream read by nodes to its readers in the order of their IDs.
    THashMap<TStreamId, std::vector<std::string>> Consumers;
    //! Streams written and read inside the graph that are not computation outputs.
    THashSet<TStreamId> InternalStreams;
    //! Maps each input, output and timer of each node to the computation input and timer streams it derives from.
    THashMap<std::string, THashMap<TStreamId, THashSet<TStreamId>>> Boundaries;
    //! Same as #Boundaries for system time, which does not wait for pending timers: messages emitted
    //! by timers get the current system timestamp. Input and output streams map only to computation inputs.
    THashMap<std::string, THashMap<TStreamId, THashSet<TStreamId>>> SystemBoundaries;
};

//! Validates |graph| against the computation stream IDs and computes its structure.
TProcessFunctionGraphTopology BuildProcessFunctionGraphTopology(
    const TProcessFunctionGraphSpecPtr& graph,
    const THashSet<TStreamId>& inputStreamIds,
    const THashSet<TStreamId>& outputStreamIds);

//! Rejects fields of a computation with #TComputationSpec::ProcessFunctionGraph that the graph generates.
void ValidateProcessFunctionGraphAuthoringSpec(const TComputationSpec* spec);

//! Replaces #TComputationSpec::ProcessFunctionGraph of all computations with the graph process function.
//! Internal graph streams move from pipeline streams into the graph parameters. On error the spec is unchanged.
void ResolveProcessFunctionGraphs(const TPipelineSpecPtr& spec);

//! Validates a resolved graph computation, including the fields generated from its graph.
void ValidateProcessFunctionGraphComputation(
    const TPipelineSpecPtr& pipeline,
    const TComputationId& computationId,
    const TComputationSpecPtr& computation);

//! Validates dynamic parameters of the nodes of all resolved graph computations of |spec|.
void ValidateProcessFunctionGraphDynamicSpec(
    const TPipelineSpecPtr& spec,
    const TDynamicPipelineSpecPtr& dynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
