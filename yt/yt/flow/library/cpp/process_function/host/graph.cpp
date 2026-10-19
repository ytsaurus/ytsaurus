#include "computation_runtime_context.h"

#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>

#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/process_function_graph.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <util/digest/city.h>

#include <util/string/hex.h>

#include <algorithm>
#include <memory>

namespace NYT::NFlow {

using namespace NYTree;

DECLARE_REFCOUNTED_CLASS(TGraphNodeRuntimeContext)
DECLARE_REFCOUNTED_CLASS(TGraphProcessFunction)

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsSwiftGraph(const TComputationSpecPtr& computation)
{
    THROW_ERROR_EXCEPTION_UNLESS(computation, "Process function graph requires a hosting computation spec");
    return computation->ComputationClassName == "NYT::NFlow::TProcessFunctionSwiftMapComputation";
}

TMessageId GenerateGraphParentsId(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers)
{
    std::vector<TMessageId> ids;
    ids.reserve(messages.size() + timers.size());
    for (const auto& message : messages) {
        ids.push_back(message->MessageId);
    }
    for (const auto& timer : timers) {
        ids.push_back(timer->MessageId);
    }
    YT_VERIFY(!ids.empty());
    if (ids.size() == 1) {
        return ids.front();
    }
    std::sort(ids.begin(), ids.end());
    uint128 digest{0, 0};
    for (const auto& id : ids) {
        const auto& view = id.Underlying();
        digest = CityHash128WithSeed(view.data(), view.size(), digest);
    }
    const ui64 parts[] = {Uint128Low64(digest), Uint128High64(digest)};
    return TMessageId(Format("%v-%v", ids.front(), HexEncode(parts, sizeof(parts))));
}

class TGraphInitContext
    : public IRuntimeInitContext
{
public:
    TGraphInitContext(
        IRuntimeInitContextPtr underlying,
        IMapNodePtr parametersNode,
        TYsonStructPtr parametersObject,
        std::string profilerPrefix)
        : Underlying_(std::move(underlying))
        , ParametersNode_(parametersNode ? std::move(parametersNode) : GetEphemeralNodeFactory()->CreateMap())
        , ParametersObject_(std::move(parametersObject))
        , ProfilerPrefix_(std::move(profilerPrefix))
    { }

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> ctor) const override
    {
        return Underlying_->CreateMutableStateKeyProvider(std::move(ctor));
    }

    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> /*ctor*/) const override
    {
        return MakeFuture<IJoinedStateKeyProviderPtr>(
            TError("Joined state clients are not supported inside a process function graph"));
    }

    IInitContextPtr AsPartition() const override
    {
        return Underlying_->AsPartition();
    }

    IInitContextPtr AsKey(TKey key) const override
    {
        return Underlying_->AsKey(std::move(key));
    }

    IRuntimeInitContextPtr WithPrefix(TStringBuf prefix) const override
    {
        return New<TGraphInitContext>(
            Underlying_->WithPrefix(prefix),
            ParametersNode_,
            ParametersObject_,
            ProfilerPrefix_);
    }

    const std::string& GetPrefix() const override
    {
        return Underlying_->GetPrefix();
    }

    IMapNodePtr GetParametersNode() const override
    {
        return ParametersNode_;
    }

    TYsonStructPtr GetParametersObject() const override
    {
        return ParametersObject_;
    }

    IResourcePtr GetStaticResource(const TResourceId& resourceId) const override
    {
        return Underlying_->GetStaticResource(resourceId);
    }

    NProfiling::TProfiler GetProfiler() const override
    {
        return Underlying_->GetProfiler().WithPrefix(ProfilerPrefix_);
    }

    NHttp::IClientPtr GetHttpClient() const override
    {
        return Underlying_->GetHttpClient();
    }

    NHttp::IClientPtr GetHttpsClient() const override
    {
        return Underlying_->GetHttpsClient();
    }

    TComputationId GetComputationId() const override
    {
        return Underlying_->GetComputationId();
    }

    TPartitionId GetPartitionId() const override
    {
        return Underlying_->GetPartitionId();
    }

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& /*name*/) const override
    {
        THROW_ERROR_EXCEPTION("External state managers are not supported inside a process function graph");
    }

    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& /*name*/) const override
    {
        THROW_ERROR_EXCEPTION("External state joiners are not supported inside a process function graph");
    }

private:
    const IRuntimeInitContextPtr Underlying_;
    const IMapNodePtr ParametersNode_;
    const TYsonStructPtr ParametersObject_;
    const std::string ProfilerPrefix_;
};

template <class TContainer>
std::vector<TStreamId> GetSortedStreamIds(const TContainer& container)
{
    std::vector<TStreamId> ids;
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

TComputationStreamSpecStoragePtr BuildGraphStreams(
    const THashMap<TStreamId, TStreamSpecPtr>& streams,
    const NTableClient::TTableSchemaPtr& keySchema,
    const IPayloadConverterCachePtr& converterCache)
{
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    i64 nextSpecId = 0;
    for (const auto& streamId : GetSortedStreamIds(streams)) {
        const auto& streamSpec = GetOrCrash(streams, streamId);
        THROW_ERROR_EXCEPTION_UNLESS(streamSpec && streamSpec->Schema,
            "Process function graph stream %Qv has no schema",
            streamId);
        specs[streamId].emplace(TStreamSpecId(nextSpecId++), CloneYsonStruct(streamSpec));
    }
    return New<TComputationStreamSpecStorage>(New<TStreamSpecs>(specs), keySchema, converterCache);
}

TComputationSpecPtr BuildNodeView(
    const TProcessFunctionGraphNodeSpecPtr& node,
    const TComputationSpecPtr& computation)
{
    auto view = New<TComputationSpec>();
    view->ComputationClassName = computation->ComputationClassName;
    view->Parameters = computation->Parameters;
    view->ProcessingFunction = node->ProcessingFunction;
    view->ProcessingFunctionParameters = node->ProcessingFunctionParameters;
    view->GroupBySchema = computation->GroupBySchema;
    view->InputStreamIds = node->InputStreamIds;
    view->OutputStreamIds = node->OutputStreamIds;
    view->TimerStreams = node->TimerStreams;
    for (const auto& streamId : node->OutputStreamIds) {
        view->StreamsDependency[streamId] = GetProcessFunctionGraphNodeDependencies(node, streamId);
    }
    for (const auto& [timerId, _] : node->TimerStreams) {
        view->StreamsDependency[timerId] = GetProcessFunctionGraphNodeDependencies(node, timerId);
    }
    view->AllowTimerSelfDependency = !node->TimerStreams.empty();
    return view;
}

TTimer CopyTimer(const TInputTimerConstPtr& timer, const TStreamId& streamId)
{
    auto copy = TTimer(*timer);
    copy.StreamId = streamId;
    return copy;
}

bool AreSchemasEqual(const NTableClient::TTableSchemaPtr& lhs, const NTableClient::TTableSchemaPtr& rhs)
{
    return lhs == rhs || (lhs && rhs && *lhs == *rhs);
}

//! Returns the input and output streams of the node |spec| that are also streams of the computation |outerSpec|.
THashSet<TStreamId> GetBoundaryStreams(const TComputationSpecPtr& spec, const TComputationSpecPtr& outerSpec)
{
    THashSet<TStreamId> result;
    for (const auto* streams : {&spec->InputStreamIds, &spec->OutputStreamIds}) {
        for (const auto& streamId : *streams) {
            if (outerSpec->InputStreamIds.contains(streamId) || outerSpec->OutputStreamIds.contains(streamId)) {
                result.insert(streamId);
            }
        }
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TGraphNodeRuntimeContext
    : public TComputationRuntimeContext
{
public:
    TGraphNodeRuntimeContext(
        TComputationSpecPtr spec,
        TComputationStreamSpecStoragePtr streams,
        IPayloadConverterCachePtr converterCache,
        THashMap<TStreamId, THashSet<TStreamId>> boundaries,
        THashMap<TStreamId, THashSet<TStreamId>> systemBoundaries,
        TComputationId computationId,
        TComputationSpecPtr outerSpec)
        : TComputationRuntimeContext(
            spec,
            std::move(streams),
            spec->GroupBySchema,
            std::move(converterCache),
            /*throttlerFactory*/ nullptr)
        , Boundaries_(std::move(boundaries))
        , SystemBoundaries_(std::move(systemBoundaries))
        , ComputationId_(std::move(computationId))
        , OuterSpec_(std::move(outerSpec))
        , BoundaryStreams_(GetBoundaryStreams(spec, OuterSpec_))
    { }

    void Refresh(const IRuntimeContextPtr& outer, const IMapNodePtr& dynamicParameters)
    {
        Outer_ = outer;
        const auto& outerState = outer->GetEpochWatermarkState();
        // A node sees the watermarks of the whole pipeline; only its internal streams and local
        // timer IDs are derived from the computation boundary streams they depend on.
        auto localState = New<TWatermarkState>();
        localState->CurrentTimestamp = outerState->CurrentTimestamp;
        localState->Streams = outerState->Streams;
        localState->AlignmentGroups = outerState->AlignmentGroups;
        auto getMinWatermark = [&] (const THashSet<TStreamId>& boundaries, ETimeType timeType) {
            if (boundaries.empty()) {
                return ZeroSystemTimestamp;
            }
            auto watermark = InfinitySystemTimestamp;
            for (const auto& boundary : boundaries) {
                watermark = std::min(watermark, outerState->GetWatermark(MakeGlobalStreamId(ComputationId_, boundary, OuterSpec_), timeType));
            }
            return watermark;
        };
        for (const auto& [localId, boundaries] : Boundaries_) {
            if (BoundaryStreams_.contains(localId)) {
                continue;
            }
            auto watermarks = New<TWatermarks>();
            watermarks->EventWatermark = getMinWatermark(boundaries, ETimeType::EventTime);
            watermarks->SystemWatermark = getMinWatermark(GetOrCrash(SystemBoundaries_, localId), ETimeType::SystemTime);
            localState->Streams[localId] = std::move(watermarks);
        }
        RefreshEpochState(
            localState,
            dynamicParameters,
            IsSwiftGraph(OuterSpec_) ? std::nullopt : std::optional(outer->GetEpochUniqueSeqNo()));
    }

    NConcurrency::IThroughputThrottlerPtr GetThrottlerOrThrow(const TThrottlerId& throttlerId) override
    {
        return Outer_->GetThrottlerOrThrow(throttlerId);
    }

    NConcurrency::IThroughputThrottlerPtr TryGetThrottler(const TThrottlerId& throttlerId) override
    {
        return Outer_->TryGetThrottler(throttlerId);
    }

private:
    const THashMap<TStreamId, THashSet<TStreamId>> Boundaries_;
    const THashMap<TStreamId, THashSet<TStreamId>> SystemBoundaries_;
    const TComputationId ComputationId_;
    const TComputationSpecPtr OuterSpec_;
    //! Only these keep outer watermarks: timer IDs never coincide with the message streams of their node.
    const THashSet<TStreamId> BoundaryStreams_;
    IRuntimeContextPtr Outer_;
};

DEFINE_REFCOUNTED_TYPE(TGraphNodeRuntimeContext)

////////////////////////////////////////////////////////////////////////////////

// The qualified class name is part of the pipeline spec.
class TGraphProcessFunction
    : public IBatchProcessFunction
{
public:
    explicit TGraphProcessFunction(TProcessFunctionContextPtr context);

    void Init(const IRuntimeInitContextPtr& initContext) override;
    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    class TOutputCollector;

    //! Computation input messages and timers passed to the host as parents of graph outputs.
    struct TOrigins
    {
        std::vector<TInputMessageConstPtr> Messages;
        std::vector<TInputTimerConstPtr> Timers;
        //! Stable identity of the boundary parents, independent of their collectors.
        TMessageId MessageId;
        //! Swift timestamp aggregates of the immutable original message parents.
        TSystemTimestamp SystemTimestamp = ZeroSystemTimestamp;
        TSystemTimestamp AlignmentTimestamp = InfinitySystemTimestamp;
    };

    using TOriginsPtr = std::shared_ptr<const TOrigins>;

    struct TNode
    {
        std::string Id;
        TProcessFunctionGraphNodeSpecPtr Spec;
        //! Dependencies of outputs and timers that do not accept every parent of the node.
        THashMap<TStreamId, THashSet<TStreamId>> ParentFilters;
        IBatchProcessFunctionPtr Function;
        TGraphNodeRuntimeContextPtr Runtime;
        std::vector<TInputMessageConstPtr> Messages;
        std::vector<TInputTimerConstPtr> Timers;
    };

    //! Parents of the outputs of one collector to one stream or timer.
    struct TResolvedParents
    {
        //! Immediate parents supplied by the node, filtered by its stream dependencies.
        std::vector<TInputMessageConstPtr> Messages;
        std::vector<TInputTimerConstPtr> Timers;
        //! The computation inputs these parents derive from, used when exporting to the host.
        TOriginsPtr Origins;
        //! The key of the first parent.
        TKey Key;
        bool HasMixedKeys = false;
        TSystemTimestamp MinEventTimestamp = InfinitySystemTimestamp;
        //! Cached scope of explicit suffixes; computed only when such an output is emitted.
        std::optional<std::string> MessageIdSuffixScope;
    };

    const TProcessFunctionContextPtr Context_;
    const bool IsSwift_;
    const bool AllowMerging_;
    TComputationStreamSpecStoragePtr StreamSpecs_;
    //! Nodes in topological order.
    std::vector<TNode> Nodes_;
    THashMap<TStreamId, std::vector<TNode*>> Consumers_;
    THashSet<TStreamId> ExternalOutputs_;
    THashMap<TStreamId, std::pair<TNode*, TStreamId>> TimerRoutes_;

    THashMap<TMessageId, TOriginsPtr> Origins_;
    //! Equal boundary parent sets share the outer collector and its message counters.
    THashMap<TMessageId, IOutputCollectorPtr> RootOutputs_;
    THashMap<std::pair<TMessageId, TStreamId>, i64> MessageIndices_;
    IOutputCollectorPtr RootOutput_;
    IRuntimeContextPtr OuterRuntime_;
    ui64 InvocationId_ = 0;
    bool Active_ = false;
    //! The first error raised by graph outputs in the current epoch, kept even if a function catches it.
    std::optional<TError> OutputError_;

    //! Schemas of external output streams checked against the graph in the current epoch.
    THashMap<TStreamId, NTableClient::TTableSchemaPtr> ExternalOutputSchemas_;

    const TOriginsPtr& GetOrigins(const TNode& node, const TMessageId& parentId) const;

    TResolvedParents ResolveParents(
        const TNode& node,
        const TStreamId& streamId,
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers) const;

    const IOutputCollectorPtr& GetRootOutput(const TOriginsPtr& origins);
    const NTableClient::TTableSchemaPtr& GetExternalOutputSchema(const TStreamId& streamId);

    void FillMessageMeta(const TNode& node, TMessage& message, const TResolvedParents& parents);
    void ExportMessage(TMessage&& message, const TOriginsPtr& origins, const TAddMessageOptions& options);

    void AddMessage(const TNode& node, TMessage&& message, const TAddMessageOptions& options, TOutputCollector* collector);
    void AddTimer(const TNode& node, TTimer&& timer, TOutputCollector* collector);

    void RecordOutputError(const std::exception& ex);
};

DEFINE_REFCOUNTED_TYPE(TGraphProcessFunction)

////////////////////////////////////////////////////////////////////////////////

class TGraphProcessFunction::TOutputCollector
    : public IOutputCollector
{
public:
    TOutputCollector(
        TWeakPtr<TGraphProcessFunction> graph,
        const TNode* node,
        ui64 invocationId,
        std::vector<TInputMessageConstPtr> messages,
        std::vector<TInputTimerConstPtr> timers)
        : Graph_(std::move(graph))
        , Node_(node)
        , InvocationId_(invocationId)
        , Messages_(std::move(messages))
        , Timers_(std::move(timers))
    { }

    IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits) override
    {
        auto graph = LockGraph();
        ValidateInvocation(*graph);
        THROW_ERROR_EXCEPTION_UNLESS(visits.empty(), "Visits are not supported inside a process function graph");
        return New<TOutputCollector>(Graph_, Node_, InvocationId_, messages, timers);
    }

    void AddTimer(TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp) override
    {
        TTimer timer;
        timer.TriggerTimestamp = triggerTimestamp;
        if (eventTimestamp) {
            timer.EventTimestamp = *eventTimestamp;
        }
        AddTimer(std::move(timer));
    }

    void AddTimer(
        const TStreamId& streamId,
        TSystemTimestamp triggerTimestamp,
        std::optional<TSystemTimestamp> eventTimestamp) override
    {
        TTimer timer;
        timer.StreamId = streamId;
        timer.TriggerTimestamp = triggerTimestamp;
        if (eventTimestamp) {
            timer.EventTimestamp = *eventTimestamp;
        }
        AddTimer(std::move(timer));
    }

    void AddTimer(TTimer&& timer) override
    {
        auto graph = LockGraph();
        try {
            ValidateInvocation(*graph);
            graph->AddTimer(*Node_, std::move(timer), this);
        } catch (const std::exception& ex) {
            graph->RecordOutputError(ex);
            throw;
        }
    }

    TResolvedParents& GetParents(const TGraphProcessFunction& graph, const TStreamId& streamId)
    {
        auto it = Parents_.find(streamId);
        if (it == Parents_.end()) {
            it = Parents_.emplace(streamId, graph.ResolveParents(*Node_, streamId, Messages_, Timers_)).first;
        }
        return it->second;
    }

private:
    //! Weak, because the graph owns the functions, and a function may keep its collector.
    const TWeakPtr<TGraphProcessFunction> Graph_;
    //! Owned by the graph.
    const TNode* const Node_;
    const ui64 InvocationId_;
    const std::vector<TInputMessageConstPtr> Messages_;
    const std::vector<TInputTimerConstPtr> Timers_;
    //! Outputs of one collector to one stream share their parents and origins.
    THashMap<TStreamId, TResolvedParents> Parents_;

    TGraphProcessFunctionPtr LockGraph() const
    {
        auto graph = Graph_.Lock();
        THROW_ERROR_EXCEPTION_UNLESS(graph, "Process function graph output was used after the graph was destroyed");
        return graph;
    }

    void ValidateInvocation(const TGraphProcessFunction& graph) const
    {
        THROW_ERROR_EXCEPTION_UNLESS(graph.Active_ && InvocationId_ == graph.InvocationId_,
            "Process function graph output collector was used outside its Process invocation");
    }

    void DoAddMessage(TMessage&& message, TAddMessageOptions options) override
    {
        auto graph = LockGraph();
        try {
            ValidateInvocation(*graph);
            graph->AddMessage(*Node_, std::move(message), options, this);
        } catch (const std::exception& ex) {
            graph->RecordOutputError(ex);
            throw;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TGraphProcessFunction::TGraphProcessFunction(TProcessFunctionContextPtr context)
    : Context_(std::move(context))
    , IsSwift_(IsSwiftGraph(Context_->ComputationSpec))
    , AllowMerging_(!IsSwift_ || ConvertTo<TSwiftMapComputation::TParametersPtr>(Context_->ComputationSpec->Parameters)->AllowBatchingWithRelaxedGuarantees)
{ }

void TGraphProcessFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    auto parameters = initContext->GetParameters<TProcessFunctionGraphParameters>();
    const auto& graph = parameters->Graph;
    const auto& computation = Context_->ComputationSpec;
    THROW_ERROR_EXCEPTION_UNLESS(computation, "Process function graph requires a hosting computation spec");
    auto topology = BuildProcessFunctionGraphTopology(graph, computation->InputStreamIds, computation->OutputStreamIds);
    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    StreamSpecs_ = BuildGraphStreams(parameters->Streams, computation->GroupBySchema, converterCache);

    THashMap<std::string, TNode*> nodeById;
    Nodes_.reserve(topology.Order.size());
    for (const auto& nodeId : topology.Order) {
        auto& node = Nodes_.emplace_back();
        node.Id = nodeId;
        node.Spec = GetOrCrash(graph->Nodes, nodeId);
        auto parentCount = node.Spec->InputStreamIds.size() + node.Spec->TimerStreams.size();
        auto addFilter = [&] (const TStreamId& streamId) {
            auto dependencies = GetProcessFunctionGraphNodeDependencies(node.Spec, streamId);
            if (dependencies.size() < parentCount) {
                node.ParentFilters.emplace(streamId, std::move(dependencies));
            }
        };
        for (const auto& streamId : node.Spec->OutputStreamIds) {
            addFilter(streamId);
        }
        for (const auto& [timerId, _] : node.Spec->TimerStreams) {
            addFilter(timerId);
            TimerRoutes_.emplace(MakeProcessFunctionGraphTimerId(nodeId, timerId), std::pair(&node, timerId));
        }
        nodeById.emplace(nodeId, &node);
    }
    for (const auto& [streamId, consumers] : topology.Consumers) {
        auto& nodes = Consumers_[streamId];
        for (const auto& nodeId : consumers) {
            nodes.push_back(GetOrCrash(nodeById, nodeId));
        }
    }
    auto validateStreamSpec = [&] (const TStreamId& streamId) {
        THROW_ERROR_EXCEPTION_UNLESS(StreamSpecs_->GetStreamSpecs()->HasStream(streamId),
            "Process function graph stream %Qv has no spec",
            streamId);
    };
    for (const auto& streamId : GetSortedStreamIds(topology.Consumers)) {
        validateStreamSpec(streamId);
    }
    for (const auto& streamId : GetSortedStreamIds(topology.Producers)) {
        validateStreamSpec(streamId);
    }
    ExternalOutputs_ = computation->OutputStreamIds;

    for (auto& node : Nodes_) {
        try {
            auto view = BuildNodeView(node.Spec, computation);
            auto childInit = New<TGraphInitContext>(
                initContext->WithPrefix("functions")->WithPrefix(node.Id),
                node.Spec->ProcessingFunctionParameters,
                TRegistry::Get()->ParseProcessFunctionParameters(view),
                Format("/functions/%v", node.Id));

            auto processContext = New<TProcessFunctionContext>();
            processContext->InitContext = childInit;
            processContext->ComputationSpec = view;
            processContext->ClientsCache = Context_->ClientsCache;
            processContext->Invoker = Context_->Invoker;
            processContext->RetryableClient = Context_->RetryableClient->WithErrorComponent(Format("/functions/%v", node.Id));
            processContext->Logger = Context_->Logger.WithTag("ProcessFunctionNode", node.Id);
            processContext->StatusProfiler = Context_->StatusProfiler->WithPrefix(Format("/functions/%v", node.Id));

            auto function = TRegistry::Get()->CreateProcessFunction(node.Spec->ProcessingFunction, processContext);
            THROW_ERROR_EXCEPTION_IF(
                TRegistry::Get()->ViewProcessFunctionAsSync(node.Spec->ProcessingFunction, function),
                "Sync process functions are not supported inside a process function graph");
            node.Function = WrapAsBatch(function);
            node.Runtime = New<TGraphNodeRuntimeContext>(
                std::move(view),
                StreamSpecs_,
                converterCache,
                GetOrCrash(topology.Boundaries, node.Id),
                GetOrCrash(topology.SystemBoundaries, node.Id),
                childInit->GetComputationId(),
                computation);
            function->Init(childInit);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Cannot initialize process function graph node %Qv", node.Id)
                .With("processing_function", node.Spec->ProcessingFunction)
                .With(ex);
        }
    }
}

const TGraphProcessFunction::TOriginsPtr& TGraphProcessFunction::GetOrigins(
    const TNode& node,
    const TMessageId& parentId) const
{
    auto it = Origins_.find(parentId);
    THROW_ERROR_EXCEPTION_IF(it == Origins_.end(),
        "Parent %v of an output of process function graph node %Qv is not an input of the node in this epoch",
        parentId,
        node.Id);
    return it->second;
}

TGraphProcessFunction::TResolvedParents TGraphProcessFunction::ResolveParents(
    const TNode& node,
    const TStreamId& streamId,
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers) const
{
    TResolvedParents result;
    if (auto it = node.ParentFilters.find(streamId); it == node.ParentFilters.end()) {
        result.Messages = messages;
        result.Timers = timers;
        THROW_ERROR_EXCEPTION_IF(result.Messages.empty() && result.Timers.empty(),
            "Output of process function graph node %Qv to stream %Qv has no parents; emit it while processing "
            "an input message or timer, or set its parents with output->SetParents(...)",
            node.Id,
            streamId);
    } else {
        for (const auto& message : messages) {
            if (it->second.contains(message->StreamId)) {
                result.Messages.push_back(message);
            }
        }
        for (const auto& timer : timers) {
            if (it->second.contains(timer->StreamId)) {
                result.Timers.push_back(timer);
            }
        }
        THROW_ERROR_EXCEPTION_IF(result.Messages.empty() && result.Timers.empty(),
            "No parent of stream %Qv of process function graph node %Qv is allowed by its dependencies %v; "
            "declare the dependencies in \"streams_dependency\" of the node",
            streamId,
            node.Id,
            GetSortedStreamIds(it->second));
    }

    // Parents often share origins, e.g. the outputs of one batch, so each origins object is traversed once.
    std::vector<TOriginsPtr> distinctOrigins;
    THashSet<const TOrigins*> visitedOrigins;
    auto visit = [&] (const TMessageId& parentId) {
        const auto& parentOrigins = GetOrigins(node, parentId);
        if (visitedOrigins.insert(parentOrigins.get()).second) {
            distinctOrigins.push_back(parentOrigins);
        }
    };
    for (const auto& message : result.Messages) {
        visit(message->MessageId);
    }
    for (const auto& timer : result.Timers) {
        visit(timer->MessageId);
    }
    if (distinctOrigins.size() == 1) {
        // Outputs share the origins of their parents instead of copying them.
        result.Origins = std::move(distinctOrigins.front());
    } else {
        auto origins = std::make_shared<TOrigins>();
        THashSet<TMessageId> seen;
        for (const auto& parentOrigins : distinctOrigins) {
            origins->SystemTimestamp = std::max(origins->SystemTimestamp, parentOrigins->SystemTimestamp);
            origins->AlignmentTimestamp = std::min(origins->AlignmentTimestamp, parentOrigins->AlignmentTimestamp);
            for (const auto& message : parentOrigins->Messages) {
                if (seen.insert(message->MessageId).second) {
                    origins->Messages.push_back(message);
                }
            }
            for (const auto& timer : parentOrigins->Timers) {
                if (seen.insert(timer->MessageId).second) {
                    origins->Timers.push_back(timer);
                }
            }
        }
        auto byId = [] (const auto& lhs, const auto& rhs) {
            return lhs->MessageId < rhs->MessageId;
        };
        std::sort(origins->Messages.begin(), origins->Messages.end(), byId);
        std::sort(origins->Timers.begin(), origins->Timers.end(), byId);
        origins->MessageId = GenerateGraphParentsId(origins->Messages, origins->Timers);
        result.Origins = std::move(origins);
    }

    // Parents are not empty here.
    result.Key = result.Messages.empty() ? result.Timers.front()->Key : result.Messages.front()->Key;
    auto accept = [&] (const TKey& key, TSystemTimestamp eventTimestamp) {
        result.HasMixedKeys |= key != result.Key;
        result.MinEventTimestamp = std::min(result.MinEventTimestamp, eventTimestamp);
    };
    for (const auto& message : result.Messages) {
        accept(message->Key, message->EventTimestamp);
    }
    for (const auto& timer : result.Timers) {
        accept(timer->Key, timer->EventTimestamp);
    }
    return result;
}

const IOutputCollectorPtr& TGraphProcessFunction::GetRootOutput(const TOriginsPtr& origins)
{
    auto it = RootOutputs_.find(origins->MessageId);
    if (it == RootOutputs_.end()) {
        auto rootOutput = RootOutput_->SetParents(origins->Messages, origins->Timers, {});
        it = RootOutputs_.emplace(origins->MessageId, std::move(rootOutput)).first;
    }
    return it->second;
}

const NTableClient::TTableSchemaPtr& TGraphProcessFunction::GetExternalOutputSchema(const TStreamId& streamId)
{
    auto it = ExternalOutputSchemas_.find(streamId);
    if (it == ExternalOutputSchemas_.end()) {
        auto schema = OuterRuntime_->GetStreamSpecs()->GetSchema(streamId);
        THROW_ERROR_EXCEPTION_UNLESS(AreSchemasEqual(schema, StreamSpecs_->GetSchema(streamId)),
            "Output stream %Qv of the computation has a different schema than in its process function graph",
            streamId);
        it = ExternalOutputSchemas_.emplace(streamId, std::move(schema)).first;
    }
    return it->second;
}

void TGraphProcessFunction::Process(
    const IInputContextPtr& input,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    THROW_ERROR_EXCEPTION_IF(Active_, "Process function graph has overlapping epochs");
    THROW_ERROR_EXCEPTION_UNLESS(input->GetVisits().empty(), "Visits are not supported inside a process function graph");
    auto dynamicParameters = context->GetDynamicParameters<TProcessFunctionGraphDynamicParameters>();
    ++InvocationId_;
    Active_ = true;
    RootOutput_ = output;
    OuterRuntime_ = context;
    auto clearGuard = Finally([&] {
        for (auto& node : Nodes_) {
            node.Messages.clear();
            node.Timers.clear();
        }
        Origins_.clear();
        RootOutputs_.clear();
        MessageIndices_.clear();
        ExternalOutputSchemas_.clear();
        OutputError_.reset();
        RootOutput_.Reset();
        OuterRuntime_.Reset();
        Active_ = false;
    });

    // Maps each input stream to the last payload schema already compared with the graph.
    THashMap<TStreamId, NTableClient::TTableSchemaPtr> checkedInputSchemas;
    for (const auto& message : input->GetMessages()) {
        auto origins = std::make_shared<TOrigins>();
        origins->Messages.push_back(message);
        origins->MessageId = message->MessageId;
        origins->SystemTimestamp = message->SystemTimestamp;
        origins->AlignmentTimestamp = message->AlignmentTimestamp;
        Origins_[message->MessageId] = std::move(origins);

        auto it = Consumers_.find(message->StreamId);
        THROW_ERROR_EXCEPTION_IF(it == Consumers_.end(),
            "Process function graph received a message of stream %Qv, which no node reads",
            message->StreamId);
        auto schema = StreamSpecs_->GetSchema(message->StreamId);
        auto& checkedSchema = checkedInputSchemas[message->StreamId];
        if (checkedSchema != message->PayloadSchema) {
            THROW_ERROR_EXCEPTION_UNLESS(AreSchemasEqual(message->PayloadSchema, schema),
                "Input message of stream %Qv has an unexpected schema",
                message->StreamId);
            checkedSchema = message->PayloadSchema;
        }
        auto copy = TMessage(*message);
        copy.PayloadSchema = std::move(schema);
        auto delivered = New<TInputMessage>(std::move(copy), message->Key);
        for (auto* node : it->second) {
            node->Messages.push_back(delivered);
        }
    }
    for (const auto& timer : input->GetTimers()) {
        auto origins = std::make_shared<TOrigins>();
        origins->Timers.push_back(timer);
        origins->MessageId = timer->MessageId;
        Origins_[timer->MessageId] = std::move(origins);

        auto it = TimerRoutes_.find(timer->StreamId);
        THROW_ERROR_EXCEPTION_IF(it == TimerRoutes_.end(),
            "Process function graph received a timer of unknown stream %Qv",
            timer->StreamId);
        auto [node, timerId] = it->second;
        node->Timers.push_back(New<TInputTimer>(CopyTimer(timer, timerId), context->GetKeySchema()));
    }

    for (auto& node : Nodes_) {
        auto dynamicIt = dynamicParameters->Nodes.find(node.Id);
        try {
            node.Runtime->Refresh(context, dynamicIt == dynamicParameters->Nodes.end() ? nullptr : dynamicIt->second);
            auto collector = New<TOutputCollector>(
                MakeWeak(this),
                &node,
                InvocationId_,
                node.Messages,
                node.Timers);
            node.Function->Process(
                New<TInputContext>(node.Messages, node.Timers),
                collector,
                node.Runtime);
            // An output error fails the epoch even if the function caught it.
            if (OutputError_) {
                OutputError_->ThrowOnError();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Process function graph node %Qv failed", node.Id)
                .With("processing_function", node.Spec->ProcessingFunction)
                .With(ex);
        }
    }
}

void TGraphProcessFunction::RecordOutputError(const std::exception& ex)
{
    if (!OutputError_) {
        OutputError_ = TError(ex);
    }
}

void TGraphProcessFunction::AddMessage(
    const TNode& node,
    TMessage&& message,
    const TAddMessageOptions& options,
    TOutputCollector* collector)
{
    THROW_ERROR_EXCEPTION_UNLESS(Active_, "Process function graph output was used outside Process");
    if (!options.Distribute) {
        return;
    }
    const auto& outputs = node.Spec->OutputStreamIds;
    if (message.StreamId.Underlying().empty()) {
        THROW_ERROR_EXCEPTION_IF(outputs.empty(),
            "Process function graph node %Qv emitted a message, but it has no output streams",
            node.Id);
        THROW_ERROR_EXCEPTION_UNLESS(outputs.size() == 1,
            "Process function graph node %Qv has several output streams %v; set the stream ID of the message",
            node.Id,
            GetSortedStreamIds(outputs));
        message.StreamId = *outputs.begin();
    }
    const auto streamId = message.StreamId;
    THROW_ERROR_EXCEPTION_UNLESS(outputs.contains(streamId),
        "Process function graph node %Qv emitted a message to stream %Qv, which is not one of its output streams %v",
        node.Id,
        streamId,
        GetSortedStreamIds(outputs));
    auto schema = StreamSpecs_->GetSchema(streamId);
    THROW_ERROR_EXCEPTION_UNLESS(AreSchemasEqual(message.PayloadSchema, schema),
        "Message of process function graph node %Qv has an unexpected schema for stream %Qv",
        node.Id,
        streamId);
    message.PayloadSchema = std::move(schema);

    auto& parents = collector->GetParents(*this, streamId);
    auto resolvedOptions = options;
    if (options.MessageIdSuffix.GetMode() != TOutputMessageIdSuffix::EMode::SequenceNumber) {
        // A per-message function's suffix is scoped to its immediate input, even when several
        // inputs descend from the same boundary parent. Preserve that scope on export too.
        if (!parents.MessageIdSuffixScope) {
            auto parentId = GenerateGraphParentsId(parents.Messages, parents.Timers);
            const auto& parentIdView = parentId.Underlying();
            auto digest = CityHash128(parentIdView.data(), parentIdView.size());
            const ui64 parts[] = {Uint128Low64(digest), Uint128High64(digest)};
            parents.MessageIdSuffixScope = HexEncode(parts, sizeof(parts));
        }
        auto suffix = Format("%v/%v", *parents.MessageIdSuffixScope, options.MessageIdSuffix.Resolve(message, /*sequenceNumber*/ 0));
        resolvedOptions.MessageIdSuffix = TOutputMessageIdSuffix::FromUserDefined(std::move(suffix));
    }
    FillMessageMeta(node, message, parents);

    // Check every destination before delivering the message to any of them.
    TInputMessageConstPtr delivered;
    const std::vector<TNode*>* consumers = nullptr;
    if (auto it = Consumers_.find(streamId); it != Consumers_.end()) {
        consumers = &it->second;
        auto key = StreamSpecs_->ComputeKey(message);
        THROW_ERROR_EXCEPTION_IF(parents.HasMixedKeys,
            "Parents of a message on stream %Qv of process function graph node %Qv have different keys; "
            "a batch function must set the parents of each output with output->SetParents(...)",
            streamId,
            node.Id);
        THROW_ERROR_EXCEPTION_IF(parents.Key != key,
            "Process function graph node %Qv changed the key of a message on stream %Qv read by other nodes; "
            "a key change requires a separate computation",
            node.Id,
            streamId)
            .With("key", key)
            .With("parent_key", parents.Key);
        auto& index = MessageIndices_[std::pair(parents.Origins->MessageId, streamId)];
        message.MessageId = GenerateInheritedMessageId(
            parents.Origins->MessageId,
            streamId,
            resolvedOptions.MessageIdSuffix.Resolve(message, index++));
        delivered = New<TInputMessage>(TMessage(message), std::move(key));
        Origins_[message.MessageId] = parents.Origins;
    }

    if (ExternalOutputs_.contains(streamId)) {
        ExportMessage(std::move(message), parents.Origins, resolvedOptions);
    }
    if (consumers) {
        for (auto* consumer : *consumers) {
            consumer->Messages.push_back(delivered);
        }
    }
}

void TGraphProcessFunction::FillMessageMeta(
    const TNode& node,
    TMessage& message,
    const TResolvedParents& parents)
{
    const auto& origins = parents.Origins;
    if (IsSwift_) {
        THROW_ERROR_EXCEPTION_UNLESS(origins->Timers.empty(),
            "Output of Swift process function graph node %Qv has parent timers",
            node.Id);
        THROW_ERROR_EXCEPTION_UNLESS(AllowMerging_ || origins->Messages.size() == 1,
            "Output of Swift process function graph node %Qv has %v boundary parents; "
            "merging requires allow_batching_with_relaxed_guarantees=true",
            node.Id,
            origins->Messages.size());
    }

    if (IsSwift_) {
        message.SystemTimestamp = origins->SystemTimestamp;
        message.AlignmentTimestamp = origins->AlignmentTimestamp;
    } else {
        message.SystemTimestamp = OuterRuntime_->GetCurrentTimestamp();
        message.AlignmentTimestamp = message.SystemTimestamp;
    }
    if (message.EventTimestamp == ZeroSystemTimestamp) {
        message.EventTimestamp = parents.MinEventTimestamp;
    }
}

void TGraphProcessFunction::ExportMessage(
    TMessage&& message,
    const TOriginsPtr& origins,
    const TAddMessageOptions& options)
{
    message.PayloadSchema = GetExternalOutputSchema(message.StreamId);
    auto exportOptions = options;
    if (!IsSwift_) {
        // Transform assigns a fresh external identity independently of the internal suffix.
        exportOptions.MessageIdSuffix = TOutputMessageIdSuffix::FromSequenceNumber();
    }
    GetRootOutput(origins)->AddMessage(std::move(message), exportOptions);
}

void TGraphProcessFunction::AddTimer(
    const TNode& node,
    TTimer&& timer,
    TOutputCollector* collector)
{
    THROW_ERROR_EXCEPTION_UNLESS(Active_, "Process function graph output was used outside Process");
    const auto& timers = node.Spec->TimerStreams;
    if (timer.StreamId.Underlying().empty()) {
        THROW_ERROR_EXCEPTION_IF(timers.empty(),
            "Process function graph node %Qv scheduled a timer, but it has no timer streams",
            node.Id);
        THROW_ERROR_EXCEPTION_UNLESS(timers.size() == 1,
            "Process function graph node %Qv has several timer streams %v; set the stream ID of the timer",
            node.Id,
            GetSortedStreamIds(timers));
        timer.StreamId = timers.begin()->first;
    }
    THROW_ERROR_EXCEPTION_UNLESS(timers.contains(timer.StreamId),
        "Process function graph node %Qv scheduled a timer on stream %Qv, which is not one of its timer streams %v",
        node.Id,
        timer.StreamId,
        GetSortedStreamIds(timers));

    auto& parents = collector->GetParents(*this, timer.StreamId);
    if (timer.Key.Underlying().GetCount() == 0) {
        THROW_ERROR_EXCEPTION_IF(parents.HasMixedKeys,
            "Parents of a timer of process function graph node %Qv have different keys; "
            "set the key of the timer or its parents with output->SetParents(...)",
            node.Id);
        timer.Key = parents.Key;
    }
    timer.KeySchema = OuterRuntime_->GetKeySchema();
    if (timer.EventTimestamp == ZeroSystemTimestamp) {
        timer.EventTimestamp = parents.MinEventTimestamp;
    }
    timer.StreamId = MakeProcessFunctionGraphTimerId(node.Id, timer.StreamId);
    GetRootOutput(parents.Origins)->AddTimer(std::move(timer));
}

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_PROCESS_FUNCTION(
    TGraphProcessFunction,
    TProcessFunctionGraphParameters,
    TProcessFunctionGraphDynamicParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
