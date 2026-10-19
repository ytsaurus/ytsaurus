#include "process_function_graph_test_harness.h"

#include "test_runtime_context.h"

#include <yt/yt/flow/library/cpp/computation/computation_base.h>
#include <yt/yt/flow/library/cpp/computation/event_timestamp_assigner.h>
#include <yt/yt/flow/library/cpp/computation/meta_setter.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NTesting {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TComputationSpecPtr ParseGraphComputation(TStringBuf yson)
{
    auto node = ConvertToNode(NYson::TYsonStringBuf(yson))->AsMap();
    if (!node->FindChild("computation_class_name")) {
        node->AddChild("computation_class_name", ConvertToNode("NYT::NFlow::TProcessFunctionComputation"));
    }
    return ConvertTo<TComputationSpecPtr>(node);
}

std::vector<TMessageId> GetParentIds(const TMessageParentsConstPtr& parents)
{
    std::vector<TMessageId> result;
    if (!parents) {
        return result;
    }
    for (const auto& message : parents->ParentMessages) {
        result.push_back(message->MessageId);
    }
    for (const auto& timer : parents->ParentTimers) {
        result.push_back(timer->MessageId);
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProcessFunctionGraphTestHarness::TProcessFunctionGraphTestHarness(
    TTestStateEnvironment& environment,
    TStringBuf computationYson,
    NTableClient::TTableSchemaPtr keySchema)
    : TProcessFunctionGraphTestHarness(environment, ParseGraphComputation(computationYson), std::move(keySchema))
{ }

TProcessFunctionGraphTestHarness::TProcessFunctionGraphTestHarness(
    TTestStateEnvironment& environment,
    TComputationSpecPtr computation,
    NTableClient::TTableSchemaPtr keySchema)
    : Environment_(environment)
    , KeySchema_(std::move(keySchema))
    , Computation_(CloneYsonStruct(computation))
    , DynamicParameters_(New<TProcessFunctionGraphDynamicParameters>())
{
    THROW_ERROR_EXCEPTION_UNLESS(Computation_->ProcessFunctionGraph,
        "Computation spec of a process function graph test has no \"process_function_graph\"");
    // An omitted `group_by_schema` parses as an empty schema.
    if (!Computation_->GroupBySchema || Computation_->GroupBySchema->Columns().empty()) {
        Computation_->GroupBySchema = KeySchema_;
    } else {
        THROW_ERROR_EXCEPTION_UNLESS(*Computation_->GroupBySchema == *KeySchema_,
            "\"group_by_schema\" of the computation differs from the key schema of the harness; "
            "pass the same schema to the harness and to its state environment")
            .With("group_by_schema", *Computation_->GroupBySchema)
            .With("key_schema", *KeySchema_);
    }
}

TProcessFunctionGraphTestHarness::~TProcessFunctionGraphTestHarness() = default;

TProcessFunctionGraphTestHarness& TProcessFunctionGraphTestHarness::RegisterStream(
    const TStreamId& streamId,
    NTableClient::TTableSchemaPtr schema)
{
    auto spec = New<TStreamSpec>();
    spec->Schema = std::move(schema);
    return RegisterStream(streamId, std::move(spec));
}

TProcessFunctionGraphTestHarness& TProcessFunctionGraphTestHarness::RegisterStream(
    const TStreamId& streamId,
    TStreamSpecPtr spec)
{
    THROW_ERROR_EXCEPTION_IF(InitializationStarted_, "Register streams before the first epoch");
    auto [_, inserted] = Streams_.emplace(streamId, std::move(spec));
    THROW_ERROR_EXCEPTION_UNLESS(inserted, "Stream %Qv is already registered", streamId);
    return *this;
}

TProcessFunctionGraphTestHarness& TProcessFunctionGraphTestHarness::SetNodeDynamicParameters(
    const std::string& nodeId,
    const TYsonStructPtr& parameters)
{
    if (parameters) {
        DynamicParameters_->Nodes[nodeId] = ConvertTo<IMapNodePtr>(parameters);
    } else {
        DynamicParameters_->Nodes.erase(nodeId);
    }
    return *this;
}

TProcessFunctionGraphTestHarness& TProcessFunctionGraphTestHarness::SetCurrentTimestamp(TSystemTimestamp value)
{
    CurrentTimestamp_ = value;
    return *this;
}

TProcessFunctionGraphTestHarness& TProcessFunctionGraphTestHarness::SetWatermark(
    const TStreamId& streamId,
    TSystemTimestamp value)
{
    Watermarks_[streamId] = value;
    return *this;
}

TInputMessageConstPtr TProcessFunctionGraphTestHarness::MakeMessage(
    const TStreamId& streamId,
    const TKey& key,
    const TMessageBuilder::TInitFunction& init) const
{
    auto it = Streams_.find(streamId);
    THROW_ERROR_EXCEPTION_IF(it == Streams_.end(), "Stream %Qv is not registered", streamId);
    return MakeTestMessage(streamId, key, it->second->Schema, init);
}

void TProcessFunctionGraphTestHarness::RunEpoch(
    std::vector<TInputMessageConstPtr> messages,
    std::vector<TInputTimerConstPtr> timers)
{
    EnsureInitialized();
    Messages_.clear();
    Timers_.clear();
    TRegistry::Get()->ValidateProcessFunctionGraphDynamicParameters(Computation_, ConvertTo<IMapNodePtr>(DynamicParameters_));

    auto seqNo = TUniqueSeqNo(NextEpochSeqNo_++);
    const bool swift = Computation_->ComputationClassName == "NYT::NFlow::TProcessFunctionSwiftMapComputation";
    TTestRuntimeContextBuilder builder;
    for (const auto& streamId : Computation_->InputStreamIds) {
        builder.RegisterStream(streamId, GetOrCrash(Streams_, streamId)->Schema);
    }
    for (const auto& streamId : Computation_->OutputStreamIds) {
        builder.RegisterStream(streamId, GetOrCrash(Streams_, streamId)->Schema);
    }
    for (const auto& [streamId, watermark] : Watermarks_) {
        builder.SetWatermark(streamId, watermark);
    }
    if (!swift) {
        builder.SetEpochUniqueSeqNo(seqNo);
    }
    auto runtime = builder
        // The builder completes a spec without outputs in place, so it gets a copy.
        .SetSpec(CloneYsonStruct(Computation_))
        .SetKeySchema(KeySchema_)
        .SetCurrentTimestamp(CurrentTimestamp_)
        .SetDynamicParameters(CloneYsonStruct(DynamicParameters_))
        .Build();

    auto input = New<TInputContext>(std::move(messages), std::move(timers));
    auto assigner = CreateEventTimestampAssigner(/*spec*/ nullptr);
    auto setter = swift
        ? (ConvertTo<TSwiftMapComputation::TParametersPtr>(Computation_->Parameters)->AllowBatchingWithRelaxedGuarantees
                ? CreateSwiftMergeMetaSetter(Computation_, seqNo, assigner)
                : CreateDeterministicMetaSetter(Computation_, assigner))
        : CreateUniqueMetaSetter(Computation_, seqNo, CurrentTimestamp_, assigner);
    auto output = New<TRootOutputCollector>(Computation_, setter);
    Environment_.PreloadEpoch(input);
    Function_->Process(input, output->SetParents(input->GetMessages(), input->GetTimers(), {}), runtime);
    Environment_.CommitEpoch();

    auto result = output->CollectResult();
    for (int index = 0; index < std::ssize(result.OutputMessages); ++index) {
        Messages_.push_back({
            .Message = std::move(result.OutputMessages[index]),
            .ParentIds = GetParentIds(result.OutputMessagesParentMessageIds[index]),
        });
    }
    for (int index = 0; index < std::ssize(result.OutputTimers); ++index) {
        auto& timer = result.OutputTimers[index];
        const auto& [nodeId, timerStreamId] = GetOrCrash(TimerNodes_, timer.StreamId);
        Timers_.push_back({
            .Timer = std::move(timer),
            .NodeId = nodeId,
            .TimerStreamId = timerStreamId,
            .ParentIds = GetParentIds(result.OutputTimersParentMessageIds[index]),
        });
    }
}

const std::vector<TProcessFunctionGraphTestHarness::TOutputMessage>& TProcessFunctionGraphTestHarness::GetMessages() const
{
    return Messages_;
}

const std::vector<TProcessFunctionGraphTestHarness::TOutputTimer>& TProcessFunctionGraphTestHarness::GetTimers() const
{
    return Timers_;
}

std::vector<TInputTimerConstPtr> TProcessFunctionGraphTestHarness::MakeFiringTimers() const
{
    std::vector<TInputTimerConstPtr> timers;
    for (const auto& timer : Timers_) {
        timers.push_back(New<TInputTimer>(TTimer(timer.Timer), KeySchema_));
    }
    return timers;
}

const TComputationSpecPtr& TProcessFunctionGraphTestHarness::GetComputationSpec() const
{
    return Computation_;
}

void TProcessFunctionGraphTestHarness::EnsureInitialized()
{
    if (Function_) {
        return;
    }
    // A failed initialization leaves the environment and the functions partly initialized.
    THROW_ERROR_EXCEPTION_IF(InitializationStarted_,
        "Process function graph failed to initialize in an earlier epoch; create a new harness");
    InitializationStarted_ = true;

    // Resolve the graph exactly as the runner does.
    auto computationId = Environment_.GetComputationId();
    auto pipeline = New<TPipelineSpec>();
    for (const auto& [streamId, spec] : Streams_) {
        pipeline->Streams[streamId] = CloneYsonStruct(spec);
    }
    pipeline->Computations[computationId] = Computation_;
    ResolveProcessFunctionGraphs(pipeline);
    Computation_ = GetOrCrash(pipeline->Computations, computationId);
    ValidateProcessFunctionGraphComputation(pipeline, computationId, Computation_);

    auto parameters = ConvertTo<TProcessFunctionGraphParametersPtr>(Computation_->ProcessingFunctionParameters);
    for (const auto& [nodeId, node] : parameters->Graph->Nodes) {
        for (const auto& [timerStreamId, _] : node->TimerStreams) {
            TimerNodes_.emplace(MakeProcessFunctionGraphTimerId(nodeId, timerStreamId), std::pair(nodeId, timerStreamId));
        }
    }

    Environment_.SetStaticParameters(parameters);
    auto context = Environment_.CreateProcessFunctionContext();
    context->ComputationSpec = Computation_;
    auto function = WrapAsBatch(TRegistry::Get()->CreateProcessFunction(
        std::string(ProcessFunctionGraphFunctionName),
        context));
    Environment_.InitProcessFunction(function, context);
    Function_ = std::move(function);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
