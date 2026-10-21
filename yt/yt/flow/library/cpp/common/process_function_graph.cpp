#include "process_function_graph.h"

#include "registry.h"
#include "schema.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <algorithm>
#include <set>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TProcessFunctionGraphNodeSpec::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::ThrowRecursive);
    registrar.Parameter("processing_function", &TThis::ProcessingFunction)
        .NonEmpty();
    registrar.Parameter("processing_function_parameters", &TThis::ProcessingFunctionParameters)
        .Default();
    registrar.Parameter("input_stream_ids", &TThis::InputStreamIds)
        .NonEmpty();
    registrar.Parameter("output_stream_ids", &TThis::OutputStreamIds)
        .Default();
    registrar.Parameter("timer_streams", &TThis::TimerStreams)
        .Default();
    registrar.Parameter("streams_dependency", &TThis::StreamsDependency)
        .Default();
}

void TProcessFunctionGraphSpec::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::ThrowRecursive);
    registrar.Parameter("nodes", &TThis::Nodes)
        .NonEmpty();
}

void TProcessFunctionGraphParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("graph", &TThis::Graph);
    registrar.Parameter("streams", &TThis::Streams)
        .Default();
}

void TProcessFunctionGraphDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("nodes", &TThis::Nodes)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TContainer>
auto GetSortedIds(const TContainer& container)
{
    std::vector<typename TContainer::key_type> ids;
    ids.reserve(container.size());
    for (const auto& item : container) {
        if constexpr (requires { item.first; }) {
            ids.push_back(item.first);
        } else {
            ids.push_back(item);
        }
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

//! Unlike #CloneYsonStruct(), keeps unrecognized fields, as the pipeline spec does for other computations.
template <class T>
TIntrusivePtr<T> CloneKeepingUnrecognized(const TIntrusivePtr<T>& value)
{
    auto result = New<T>();
    result->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    result->Load(ConvertToNode(value));
    return result;
}

void ValidateNodeSpec(const TProcessFunctionGraphNodeSpecPtr& node)
{
    THROW_ERROR_EXCEPTION_UNLESS(node, "Node spec cannot be null");
    THROW_ERROR_EXCEPTION_IF(node->ProcessingFunction == ProcessFunctionGraphFunctionName,
        "Nested process function graphs are not supported");
    THROW_ERROR_EXCEPTION_IF(node->InputStreamIds.empty(), "Node has no input streams");
    for (const auto& streamId : GetSortedIds(node->InputStreamIds)) {
        THROW_ERROR_EXCEPTION_IF(node->OutputStreamIds.contains(streamId),
            "Stream %Qv is both an input and an output stream of the node",
            streamId);
    }
    for (const auto& timerId : GetSortedIds(node->TimerStreams)) {
        const auto& timerSpec = GetOrCrash(node->TimerStreams, timerId);
        ValidateUserDefinedEntityId("timer stream", timerId.Underlying());
        THROW_ERROR_EXCEPTION_UNLESS(timerSpec, "Timer stream %Qv has no spec", timerId);
        THROW_ERROR_EXCEPTION_IF(node->InputStreamIds.contains(timerId) || node->OutputStreamIds.contains(timerId),
            "Timer stream %Qv has the ID of a message stream of the node",
            timerId);
        auto validateWatched = [&] (const TStreamId& streamId) {
            THROW_ERROR_EXCEPTION_UNLESS(node->InputStreamIds.contains(streamId),
                "Timer stream %Qv watches stream %Qv, which is not an input stream of the node",
                timerId,
                streamId);
        };
        THROW_ERROR_EXCEPTION_IF(timerSpec->Streams && timerSpec->StreamsWithDelays,
            "Timer stream %Qv sets both \"streams\" and \"streams_with_delays\"",
            timerId);
        if (timerSpec->Streams) {
            for (const auto& streamId : *timerSpec->Streams) {
                validateWatched(streamId);
            }
        }
        if (timerSpec->StreamsWithDelays) {
            for (const auto& [streamId, _] : *timerSpec->StreamsWithDelays) {
                validateWatched(streamId);
            }
        }
    }
    for (const auto& streamId : GetSortedIds(node->StreamsDependency)) {
        const auto& dependencies = GetOrCrash(node->StreamsDependency, streamId);
        THROW_ERROR_EXCEPTION_UNLESS(node->OutputStreamIds.contains(streamId) || node->TimerStreams.contains(streamId),
            "Stream %Qv in \"streams_dependency\" is neither an output stream nor a timer stream of the node",
            streamId);
        THROW_ERROR_EXCEPTION_IF(dependencies.empty(),
            "Stream %Qv in \"streams_dependency\" must depend on at least one stream",
            streamId);
        for (const auto& dependency : GetSortedIds(dependencies)) {
            THROW_ERROR_EXCEPTION_UNLESS(node->InputStreamIds.contains(dependency) || node->TimerStreams.contains(dependency),
                "Stream %Qv depends on %Qv, which is neither an input stream nor a timer stream of the node",
                streamId,
                dependency);
        }
    }
}

void ValidateNodeFunction(
    const std::string& nodeId,
    const TProcessFunctionGraphNodeSpecPtr& node,
    const TComputationSpec& computation)
{
    THROW_ERROR_EXCEPTION_IF(TRegistry::Get()->IsSyncProcessFunction(node->ProcessingFunction),
        "Process function graph node %Qv uses processing function %Qv implementing ISyncProcessFunction; "
        "sync process functions are not supported inside a graph",
        nodeId,
        node->ProcessingFunction);
    if (computation.ComputationClassName == "NYT::NFlow::TProcessFunctionSwiftMapComputation") {
        for (const auto& streamId : GetSortedIds(node->OutputStreamIds)) {
            for (const auto& dependency : GetSortedIds(GetProcessFunctionGraphNodeDependencies(node, streamId))) {
                THROW_ERROR_EXCEPTION_IF(node->TimerStreams.contains(dependency),
                    "Output stream %Qv of Swift process function graph node %Qv must not depend on timer stream %Qv",
                    streamId,
                    nodeId,
                    dependency);
            }
        }
    }
}

void ValidateProcessFunctionGraphHost(const TComputationSpec& computation)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        computation.ComputationClassName == "NYT::NFlow::TProcessFunctionComputation" ||
            computation.ComputationClassName == "NYT::NFlow::TProcessFunctionSwiftMapComputation",
        "Process function graph requires computation class \"NYT::NFlow::TProcessFunctionComputation\" "
        "or \"NYT::NFlow::TProcessFunctionSwiftMapComputation\"");
    THROW_ERROR_EXCEPTION_UNLESS(computation.GroupBySchema && !computation.GroupBySchema->Columns().empty(),
        "Process function graph requires \"group_by_schema\"");
    std::vector<TStringBuf> unsupportedFields;
    if (!computation.SourceStreams.empty()) {
        unsupportedFields.push_back("source_streams");
    }
    if (!computation.KeyVisitorStreams.empty()) {
        unsupportedFields.push_back("key_visitor_streams");
    }
    if (!computation.ExternalStateManagers.empty()) {
        unsupportedFields.push_back("external_state_managers");
    }
    if (!computation.ExternalStateJoiners.empty()) {
        unsupportedFields.push_back("external_state_joiners");
    }
    if (!computation.StateJoiners.empty()) {
        unsupportedFields.push_back("state_joiners");
    }
    THROW_ERROR_EXCEPTION_UNLESS(unsupportedFields.empty(),
        "Process function graph computation cannot set %v",
        unsupportedFields);
    // Internal messages inherit event timestamps from their parents, so an assigner would apply only to exported copies.
    THROW_ERROR_EXCEPTION_IF(
        computation.WatermarkStrategy &&
            computation.WatermarkStrategy->EventTimestampAssigner &&
            computation.WatermarkStrategy->EventTimestampAssigner->Column,
        "Process function graph computation cannot set \"watermark_strategy/event_timestamp_assigner\"; "
        "set event timestamps of messages in the nodes that emit them");
    THROW_ERROR_EXCEPTION_UNLESS(
        computation.WatermarkStrategy &&
            computation.WatermarkStrategy->WatermarkPercentile &&
            computation.WatermarkStrategy->WatermarkPercentile->Value == PreciseWatermarkPercentile,
        "Process function graph requires precise watermarks: \"watermark_strategy/watermark_percentile/value\" "
        "must be 100, which is the default");
}

void ValidateGraphInputGroupable(const TStreamId& streamId, const TStreamSpecPtr& streamSpec, const TComputationSpec& computation)
{
    try {
        ValidateIsGroupable(*streamSpec->Schema, *computation.GroupBySchema);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Input stream %Qv cannot be grouped by the \"group_by_schema\" of the computation", streamId).With(ex);
    }
}

struct TGeneratedComputationFields
{
    THashMap<TStreamId, TTimerSpecPtr> TimerStreams;
    THashMap<TStreamId, THashSet<TStreamId>> StreamsDependency;
};

TGeneratedComputationFields BuildGeneratedComputationFields(
    const TComputationId& computationId,
    const TComputationSpecPtr& computation,
    const TProcessFunctionGraphSpecPtr& graph,
    const TProcessFunctionGraphTopology& topology)
{
    TGeneratedComputationFields fields;
    for (const auto& nodeId : topology.Order) {
        const auto& node = GetOrCrash(graph->Nodes, nodeId);
        const auto& local = GetOrCrash(topology.Boundaries, nodeId);
        for (const auto& streamId : node->OutputStreamIds) {
            if (computation->OutputStreamIds.contains(streamId)) {
                fields.StreamsDependency[streamId] = GetOrCrash(local, streamId);
            }
        }
        for (const auto& [timerId, timerSpec] : node->TimerStreams) {
            const auto& watchedBoundaries = timerSpec->TimeType == ETimeType::SystemTime
                ? GetOrCrash(topology.SystemBoundaries, nodeId)
                : local;
            auto physicalSpec = CloneYsonStruct(timerSpec);
            if (timerSpec->StreamsWithDelays) {
                THashMap<TStreamId, TDuration> watched;
                for (const auto& [streamId, delay] : *timerSpec->StreamsWithDelays) {
                    for (const auto& boundary : GetOrCrash(watchedBoundaries, streamId)) {
                        auto [it, inserted] = watched.emplace(MakeGlobalStreamId(computationId, boundary, computation), delay);
                        if (!inserted) {
                            it->second = std::max(it->second, delay);
                        }
                    }
                }
                physicalSpec->StreamsWithDelays = std::move(watched);
            } else {
                THashSet<TStreamId> watched;
                const auto& watchedInputs = timerSpec->Streams ? *timerSpec->Streams : node->InputStreamIds;
                for (const auto& streamId : watchedInputs) {
                    for (const auto& boundary : GetOrCrash(watchedBoundaries, streamId)) {
                        watched.insert(MakeGlobalStreamId(computationId, boundary, computation));
                    }
                }
                physicalSpec->Streams = std::move(watched);
            }
            THROW_ERROR_EXCEPTION_IF(
                (!physicalSpec->Streams || physicalSpec->Streams->empty()) &&
                    (!physicalSpec->StreamsWithDelays || physicalSpec->StreamsWithDelays->empty()),
                "Timer stream %Qv of process function graph node %Qv watches no streams",
                timerId,
                nodeId);

            auto physicalId = MakeProcessFunctionGraphTimerId(nodeId, timerId);
            fields.TimerStreams.emplace(physicalId, std::move(physicalSpec));
            auto& dependencies = fields.StreamsDependency[physicalId];
            for (const auto& dependency : GetProcessFunctionGraphNodeDependencies(node, timerId)) {
                const auto& boundaries = GetOrCrash(local, dependency);
                dependencies.insert(boundaries.begin(), boundaries.end());
            }
        }
    }
    return fields;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStreamId MakeProcessFunctionGraphTimerId(TStringBuf nodeId, const TStreamId& timerId)
{
    return TStreamId(Format("pf_%v_%v_%v_%v",
        nodeId.size(),
        nodeId,
        timerId.Underlying().size(),
        timerId));
}

THashSet<TStreamId> GetProcessFunctionGraphNodeDependencies(
    const TProcessFunctionGraphNodeSpecPtr& node,
    const TStreamId& streamId)
{
    if (auto it = node->StreamsDependency.find(streamId); it != node->StreamsDependency.end()) {
        return it->second;
    }
    // By default an output depends on all inputs and timers, and a timer on all inputs and itself.
    THashSet<TStreamId> dependencies(node->InputStreamIds.begin(), node->InputStreamIds.end());
    if (node->TimerStreams.contains(streamId)) {
        dependencies.insert(streamId);
    } else {
        for (const auto& [timerId, _] : node->TimerStreams) {
            dependencies.insert(timerId);
        }
    }
    return dependencies;
}

TProcessFunctionGraphTopology BuildProcessFunctionGraphTopology(
    const TProcessFunctionGraphSpecPtr& graph,
    const THashSet<TStreamId>& inputStreamIds,
    const THashSet<TStreamId>& outputStreamIds)
{
    THROW_ERROR_EXCEPTION_UNLESS(graph && !graph->Nodes.empty(), "Process function graph has no nodes");

    TProcessFunctionGraphTopology topology;
    auto nodeIds = GetSortedIds(graph->Nodes);
    for (const auto& nodeId : nodeIds) {
        const auto& node = GetOrCrash(graph->Nodes, nodeId);
        try {
            ValidateUserDefinedEntityId("process function graph node", nodeId);
            ValidateNodeSpec(node);
            for (const auto& streamId : GetSortedIds(node->OutputStreamIds)) {
                THROW_ERROR_EXCEPTION_IF(inputStreamIds.contains(streamId),
                    "Output stream %Qv is an input stream of the computation",
                    streamId);
                auto [it, inserted] = topology.Producers.emplace(streamId, nodeId);
                THROW_ERROR_EXCEPTION_UNLESS(inserted,
                    "Output stream %Qv is also written by node %Qv",
                    streamId,
                    it->second);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Invalid process function graph node %Qv", nodeId).With(ex);
        }
    }

    for (const auto& nodeId : nodeIds) {
        for (const auto& streamId : GetSortedIds(GetOrCrash(graph->Nodes, nodeId)->InputStreamIds)) {
            THROW_ERROR_EXCEPTION_UNLESS(inputStreamIds.contains(streamId) || topology.Producers.contains(streamId),
                "Input stream %Qv of process function graph node %Qv is neither an input stream of the computation "
                "nor an output stream of another node",
                streamId,
                nodeId);
            topology.Consumers[streamId].push_back(nodeId);
        }
    }
    for (const auto& streamId : GetSortedIds(inputStreamIds)) {
        THROW_ERROR_EXCEPTION_UNLESS(topology.Consumers.contains(streamId),
            "Input stream %Qv of the computation is not read by any process function graph node",
            streamId);
    }
    for (const auto& streamId : GetSortedIds(outputStreamIds)) {
        THROW_ERROR_EXCEPTION_UNLESS(topology.Producers.contains(streamId),
            "Output stream %Qv of the computation is not written by any process function graph node",
            streamId);
    }
    for (const auto& streamId : GetSortedIds(topology.Producers)) {
        bool exported = outputStreamIds.contains(streamId);
        THROW_ERROR_EXCEPTION_UNLESS(exported || topology.Consumers.contains(streamId),
            "Output stream %Qv of process function graph node %Qv is neither read by another node "
            "nor listed in \"output_stream_ids\" of the computation",
            streamId,
            GetOrCrash(topology.Producers, streamId));
        if (!exported) {
            // Pipeline stream IDs are validated with the pipeline spec, which internal streams leave on resolution.
            ValidateUserDefinedEntityId("stream", streamId.Underlying());
            topology.InternalStreams.insert(streamId);
        }
    }

    THashMap<std::string, int> indegree;
    THashMap<std::string, THashSet<std::string>> successors;
    for (const auto& nodeId : nodeIds) {
        indegree.emplace(nodeId, 0);
    }
    for (const auto& nodeId : nodeIds) {
        for (const auto& streamId : GetOrCrash(graph->Nodes, nodeId)->InputStreamIds) {
            if (auto it = topology.Producers.find(streamId); it != topology.Producers.end()) {
                if (successors[it->second].insert(nodeId).second) {
                    ++indegree[nodeId];
                }
            }
        }
    }
    std::set<std::string> ready;
    for (const auto& [nodeId, degree] : indegree) {
        if (degree == 0) {
            ready.insert(nodeId);
        }
    }
    while (!ready.empty()) {
        auto nodeId = *ready.begin();
        ready.erase(ready.begin());
        topology.Order.push_back(nodeId);
        for (const auto& successor : successors[nodeId]) {
            if (--indegree[successor] == 0) {
                ready.insert(successor);
            }
        }
    }
    if (topology.Order.size() < nodeIds.size()) {
        std::vector<std::string> cycle;
        for (const auto& nodeId : nodeIds) {
            if (indegree[nodeId] > 0) {
                cycle.push_back(nodeId);
            }
        }
        THROW_ERROR_EXCEPTION("Process function graph contains a cycle through nodes %v", cycle);
    }

    for (const auto& nodeId : topology.Order) {
        const auto& node = GetOrCrash(graph->Nodes, nodeId);
        auto& local = topology.Boundaries[nodeId];
        auto& system = topology.SystemBoundaries[nodeId];
        THashSet<TStreamId> inputSystemBoundaries;
        for (const auto& streamId : node->InputStreamIds) {
            if (inputStreamIds.contains(streamId)) {
                local[streamId] = {streamId};
                system[streamId] = {streamId};
            } else {
                const auto& producer = GetOrCrash(topology.Producers, streamId);
                local[streamId] = GetOrCrash(GetOrCrash(topology.Boundaries, producer), streamId);
                system[streamId] = GetOrCrash(GetOrCrash(topology.SystemBoundaries, producer), streamId);
            }
            const auto& inputSystem = GetOrCrash(system, streamId);
            inputSystemBoundaries.insert(inputSystem.begin(), inputSystem.end());
        }
        for (const auto& [timerId, _] : node->TimerStreams) {
            local[timerId] = {MakeProcessFunctionGraphTimerId(nodeId, timerId)};
            system[timerId] = {MakeProcessFunctionGraphTimerId(nodeId, timerId)};
        }
        for (const auto& streamId : node->OutputStreamIds) {
            THashSet<TStreamId> boundaries;
            THashSet<TStreamId> systemBoundaries;
            for (const auto& dependency : GetProcessFunctionGraphNodeDependencies(node, streamId)) {
                const auto& dependencyBoundaries = GetOrCrash(local, dependency);
                boundaries.insert(dependencyBoundaries.begin(), dependencyBoundaries.end());
                const auto& dependencySystemBoundaries = node->TimerStreams.contains(dependency)
                    ? inputSystemBoundaries
                    : GetOrCrash(system, dependency);
                systemBoundaries.insert(dependencySystemBoundaries.begin(), dependencySystemBoundaries.end());
            }
            local[streamId] = std::move(boundaries);
            system[streamId] = std::move(systemBoundaries);
        }
    }
    return topology;
}

void ValidateProcessFunctionGraphAuthoringSpec(const TComputationSpec* spec)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        !spec->ProcessingFunction &&
            !spec->ProcessingFunctionParameters &&
            spec->StreamsDependency.empty() &&
            spec->TimerStreams.empty() &&
            !spec->AllowTimerSelfDependency,
        "\"process_function_graph\" cannot be combined with \"processing_function\", "
        "\"processing_function_parameters\", \"timer_streams\", \"streams_dependency\" or "
        "\"allow_timer_self_dependency\"; the graph generates them, declare timers and dependencies on its nodes");
}

void ResolveProcessFunctionGraphs(const TPipelineSpecPtr& spec)
{
    THashMap<TComputationId, TComputationSpecPtr> resolved;
    THashMap<TStreamId, TComputationId> internalStreams;
    for (const auto& computationId : GetSortedIds(spec->Computations)) {
        const auto& computation = GetOrCrash(spec->Computations, computationId);
        if (!computation->ProcessFunctionGraph) {
            continue;
        }
        try {
            ValidateProcessFunctionGraphAuthoringSpec(computation.Get());
            ValidateProcessFunctionGraphHost(*computation);

            auto parameters = New<TProcessFunctionGraphParameters>();
            parameters->Graph = CloneYsonStruct(computation->ProcessFunctionGraph);
            auto topology = BuildProcessFunctionGraphTopology(
                parameters->Graph,
                computation->InputStreamIds,
                computation->OutputStreamIds);

            for (const auto& nodeId : topology.Order) {
                const auto& node = GetOrCrash(parameters->Graph->Nodes, nodeId);
                try {
                    ValidateNodeFunction(nodeId, node, *computation);
                    TRegistry::Get()->ValidateStaticProcessFunctionParameters(
                        node->ProcessingFunction,
                        node->ProcessingFunctionParameters);
                    auto addStream = [&] (const TStreamId& streamId) -> const TStreamSpecPtr& {
                        if (auto it = parameters->Streams.find(streamId); it != parameters->Streams.end()) {
                            return it->second;
                        }
                        auto it = spec->Streams.find(streamId);
                        THROW_ERROR_EXCEPTION_UNLESS(it != spec->Streams.end(),
                            "Stream %Qv is not registered in pipeline \"streams\"",
                            streamId);
                        return parameters->Streams.emplace(streamId, CloneKeepingUnrecognized(it->second)).first->second;
                    };
                    for (const auto& streamId : GetSortedIds(node->InputStreamIds)) {
                        ValidateGraphInputGroupable(streamId, addStream(streamId), *computation);
                    }
                    for (const auto& streamId : GetSortedIds(node->OutputStreamIds)) {
                        addStream(streamId);
                    }
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Invalid process function graph node %Qv", nodeId).With(ex);
                }
            }

            for (const auto& streamId : GetSortedIds(topology.InternalStreams)) {
                for (const auto& [otherId, other] : spec->Computations) {
                    THROW_ERROR_EXCEPTION_IF(
                        otherId != computationId &&
                            (other->InputStreamIds.contains(streamId) || other->OutputStreamIds.contains(streamId)),
                        "Internal stream %Qv of the process function graph is also used by computation %Qv; "
                        "add it to \"output_stream_ids\" to export it",
                        streamId,
                        otherId);
                }
                auto [it, inserted] = internalStreams.emplace(streamId, computationId);
                THROW_ERROR_EXCEPTION_UNLESS(inserted,
                    "Internal stream %Qv is also internal to the process function graph of computation %Qv",
                    streamId,
                    it->second);
            }

            auto result = CloneKeepingUnrecognized(computation);
            auto fields = BuildGeneratedComputationFields(computationId, result, parameters->Graph, topology);
            result->ProcessFunctionGraph.Reset();
            result->ProcessingFunction = std::string(ProcessFunctionGraphFunctionName);
            result->ProcessingFunctionParameters = ConvertTo<IMapNodePtr>(parameters);
            result->TimerStreams = std::move(fields.TimerStreams);
            result->StreamsDependency = std::move(fields.StreamsDependency);
            result->AllowTimerSelfDependency = !result->TimerStreams.empty();
            resolved.emplace(computationId, std::move(result));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Cannot resolve process function graph of computation %Qv", computationId).With(ex);
        }
    }

    for (auto& [computationId, computation] : resolved) {
        spec->Computations[computationId] = std::move(computation);
    }
    for (const auto& [streamId, _] : internalStreams) {
        spec->Streams.erase(streamId);
    }
}

void ValidateProcessFunctionGraphComputation(
    const TPipelineSpecPtr& pipeline,
    const TComputationId& computationId,
    const TComputationSpecPtr& computation)
{
    try {
        ValidateProcessFunctionGraphHost(*computation);
        auto parameters = ConvertTo<TProcessFunctionGraphParametersPtr>(computation->ProcessingFunctionParameters);
        THROW_ERROR_EXCEPTION_UNLESS(parameters->Graph, "Process function graph parameters have no graph");
        auto topology = BuildProcessFunctionGraphTopology(
            parameters->Graph,
            computation->InputStreamIds,
            computation->OutputStreamIds);

        // Internal streams are not in pipeline streams, so they are validated here like any other stream.
        for (const auto& streamId : GetSortedIds(parameters->Streams)) {
            const auto& streamSpec = GetOrCrash(parameters->Streams, streamId);
            try {
                THROW_ERROR_EXCEPTION_UNLESS(streamSpec && streamSpec->Schema, "Stream spec has no schema");
                ValidateStreamSchema(*streamSpec->Schema);
                TRegistry::Get()->ValidateStreamSpec(streamSpec);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Invalid spec of process function graph stream %Qv", streamId).With(ex);
            }
        }

        for (const auto& nodeId : topology.Order) {
            const auto& node = GetOrCrash(parameters->Graph->Nodes, nodeId);
            ValidateNodeFunction(nodeId, node, *computation);
            auto validateStream = [&] (const TStreamId& streamId) -> const TStreamSpecPtr& {
                auto it = parameters->Streams.find(streamId);
                THROW_ERROR_EXCEPTION_UNLESS(it != parameters->Streams.end(),
                    "Stream %Qv of process function graph node %Qv has no spec in the graph parameters",
                    streamId,
                    nodeId);
                auto pipelineIt = pipeline->Streams.find(streamId);
                if (topology.InternalStreams.contains(streamId)) {
                    THROW_ERROR_EXCEPTION_IF(pipelineIt != pipeline->Streams.end(),
                        "Internal stream %Qv of the process function graph must not be registered in pipeline \"streams\"",
                        streamId);
                } else {
                    THROW_ERROR_EXCEPTION_UNLESS(pipelineIt != pipeline->Streams.end() && *pipelineIt->second == *it->second,
                        "Spec of stream %Qv in the process function graph parameters differs from its pipeline spec",
                        streamId);
                }
                return it->second;
            };
            for (const auto& streamId : GetSortedIds(node->InputStreamIds)) {
                ValidateGraphInputGroupable(streamId, validateStream(streamId), *computation);
            }
            for (const auto& streamId : GetSortedIds(node->OutputStreamIds)) {
                validateStream(streamId);
            }
        }

        auto expected = BuildGeneratedComputationFields(computationId, computation, parameters->Graph, topology);
        THROW_ERROR_EXCEPTION_UNLESS(AreNodesEqual(ConvertToNode(expected.TimerStreams), ConvertToNode(computation->TimerStreams)),
            "\"timer_streams\" of the computation do not match its process function graph")
            .With("expected_timer_streams", expected.TimerStreams);
        THROW_ERROR_EXCEPTION_UNLESS(AreNodesEqual(ConvertToNode(expected.StreamsDependency), ConvertToNode(computation->StreamsDependency)),
            "\"streams_dependency\" of the computation does not match its process function graph")
            .With("expected_streams_dependency", expected.StreamsDependency);
        THROW_ERROR_EXCEPTION_UNLESS(computation->AllowTimerSelfDependency == !expected.TimerStreams.empty(),
            "\"allow_timer_self_dependency\" of the computation does not match its process function graph");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Invalid process function graph of computation %Qv", computationId).With(ex);
    }
}

void ValidateProcessFunctionGraphDynamicSpec(
    const TPipelineSpecPtr& spec,
    const TDynamicPipelineSpecPtr& dynamicSpec)
{
    for (const auto& computationId : GetSortedIds(spec->Computations)) {
        const auto& computation = GetOrCrash(spec->Computations, computationId);
        if (computation->ProcessingFunction != ProcessFunctionGraphFunctionName) {
            continue;
        }
        auto it = dynamicSpec->Computations.find(computationId);
        try {
            TRegistry::Get()->ValidateProcessFunctionGraphDynamicParameters(
                computation,
                it == dynamicSpec->Computations.end() ? nullptr : it->second->ProcessingFunctionParameters);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Invalid dynamic parameters of process function graph computation %Qv", computationId).With(ex);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
