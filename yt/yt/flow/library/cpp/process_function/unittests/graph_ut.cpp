#include <yt/yt/flow/library/cpp/computation/computation_base.h>
#include <yt/yt/flow/library/cpp/computation/event_timestamp_assigner.h>
#include <yt/yt/flow/library/cpp/computation/meta_setter.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>

#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_graph_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/process_function_graph.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/timers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <util/generic/size_literals.h>

#include <util/string/subst.h>

#include <util/system/type_name.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NTesting;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TGraphCounterParameters
    : public TYsonStruct
{
    i64 Increment = 1;

    REGISTER_YSON_STRUCT(TGraphCounterParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("increment", &TThis::Increment)
            .Default(1);
    }
};

struct TGraphCounterDynamicParameters
    : public TYsonStruct
{
    i64 Multiplier = 1;

    REGISTER_YSON_STRUCT(TGraphCounterDynamicParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("multiplier", &TThis::Multiplier)
            .Default(1);
    }
};

class TGraphCounterFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& context) override
    {
        Increment_ = context->GetParameters<TGraphCounterParameters>()->Increment;
        context->InitClient<i64>(Counter_, "counter");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        EXPECT_EQ(
            context->GetStreamSpecs()->GetStreamSpecs()->GetStreamSpecId(message->PayloadSchema),
            context->GetStreamSpecs()->GetLastSpecId(message->StreamId));
        auto state = Counter_.GetState(message->Key);
        *state = *state + Increment_ * context->GetDynamicParameters<TGraphCounterDynamicParameters>()->Multiplier;

        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
        output->AddMessage(builder.Finish());
        if (!context->GetSpec()->TimerStreams.empty()) {
            output->AddTimer(TSystemTimestamp(10));
        }
    }

    void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(*ExtractUintFromKey(timer->Key), "key");
        output->AddMessage(builder.Finish());
    }

private:
    TMutableStateKeyClient<i64> Counter_;
    i64 Increment_ = 1;
};

//! Emits one message with every input of the batch as its parent.
class TGraphBatchFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        if (input->GetMessages().empty()) {
            return;
        }
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(GetColumnValue<ui64>(input->GetMessages().front(), "key"), "key");
        output->AddMessage(builder.Finish());
    }
};

class TGraphDiscardFunction
    : public IProcessFunction
{ };

class TGraphRelayFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
        output->AddMessage(builder.Finish());
    }
};

class TGraphChangingKeyFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(GetColumnValue<ui64>(message, "key") + 1, "key");
        output->AddMessage(builder.Finish());
    }
};

//! Batches work while preserving the individual parents of its results.
class TGraphBatchMapFunction
    : public IBatchProcessFunction
{
public:
    static std::vector<size_t>& GetBatchSizes()
    {
        static std::vector<size_t> Sizes;
        return Sizes;
    }

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        GetBatchSizes().push_back(input->GetMessages().size());
        for (const auto& message : input->GetMessages()) {
            for (int index = 0; index < 2; ++index) {
                auto builder = context->MakeOutputMessageBuilder();
                builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
                output->SetParents({message}, {}, {})->AddMessage(builder.Finish());
            }
        }
    }
};

//! Recreates the same multi-parent collector for each result.
class TGraphRepeatedBatchFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        if (input->GetMessages().empty()) {
            return;
        }
        for (int index = 0; index < 3; ++index) {
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set(GetColumnValue<ui64>(input->GetMessages().front(), "key"), "key");
            output->SetParents(input->GetMessages(), {}, {})->AddMessage(builder.Finish());
        }
    }
};

class TGraphMetaProbeFunction
    : public IProcessFunction
{
public:
    static std::vector<TMessage>& GetObserved()
    {
        static std::vector<TMessage> Messages;
        return Messages;
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        GetObserved().push_back(TMessage(*message));
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
        output->AddMessage(builder.Finish());
    }
};

class TGraphSuffixRelayFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        for (const auto& suffix : {"first", "second"}) {
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
            TAddMessageOptions options;
            options.MessageIdSuffix = TOutputMessageIdSuffix::FromUserDefined(suffix);
            output->AddMessage(builder.Finish(), options);
        }
    }
};

class TGraphBatchSuffixFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        if (input->GetMessages().empty()) {
            return;
        }
        auto singleParentOutput = output->SetParents({input->GetMessages().front()}, {}, {});
        for (const auto& collector : {output, singleParentOutput}) {
            for (const auto& suffix : {"first", "second"}) {
                auto builder = context->MakeOutputMessageBuilder();
                builder.Payload().Set(GetColumnValue<ui64>(input->GetMessages().front(), "key"), "key");
                TAddMessageOptions options;
                options.MessageIdSuffix = TOutputMessageIdSuffix::FromUserDefined(suffix);
                collector->AddMessage(builder.Finish(), options);
            }
        }
    }
};

class TGraphChainedTimersFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& /*context*/) override
    {
        output->AddTimer("first", TSystemTimestamp(10), message->EventTimestamp);
    }

    void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        output->AddTimer("second", TSystemTimestamp(20), timer->EventTimestamp);
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(*ExtractUintFromKey(timer->Key), "key");
        output->AddMessage(builder.Finish());
    }
};

//! Forwards each input message to its own input stream, which is not an output of the node.
class TGraphWrongStreamFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto builder = context->MakeOutputMessageBuilder(message->StreamId);
        builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
        try {
            output->AddMessage(builder.Finish());
        } catch (const std::exception&) {
            // Output errors fail the epoch even if the function ignores them.
        }
    }
};

struct TGraphTimerKeyParameters
    : public TYsonStruct
{
    std::optional<ui64> TimerKey;

    REGISTER_YSON_STRUCT(TGraphTimerKeyParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("timer_key", &TThis::TimerKey)
            .Default();
    }
};

//! Schedules one timer per batch, with the configured key if any, and emits a message for each fired timer.
class TGraphTimerKeyFunction
    : public IBatchProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& context) override
    {
        TimerKey_ = context->GetParameters<TGraphTimerKeyParameters>()->TimerKey;
    }

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        for (const auto& timer : input->GetTimers()) {
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set(*ExtractUintFromKey(timer->Key), "key");
            output->SetParents({}, {timer}, {})->AddMessage(builder.Finish());
        }
        if (input->GetMessages().empty()) {
            return;
        }
        TTimer timer;
        timer.TriggerTimestamp = TSystemTimestamp(10);
        if (TimerKey_) {
            timer.Key = MakeKey<ui64>(ui64{*TimerKey_});
        }
        output->AddTimer(std::move(timer));
    }

private:
    std::optional<ui64> TimerKey_;
};

//! Records the watermarks visible to the node and relays each message.
class TGraphWatermarkProbeFunction
    : public IProcessFunction
{
public:
    static THashMap<std::string, TSystemTimestamp>& GetObserved()
    {
        static THashMap<std::string, TSystemTimestamp> Watermarks;
        return Watermarks;
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto& observed = GetObserved();
        observed["input"] = context->GetInputEventWatermark();
        for (const auto& streamId : {"a_out", "left", "timer", "unrelated"}) {
            observed[streamId] = context->GetWatermark(TStreamId(streamId));
        }
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
        output->AddMessage(builder.Finish());
    }
};

//! Emits one message without parents for every batch, including empty ones.
class TGraphParentlessFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& /*input*/,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set<ui64>(9, "key");
        builder.SetEventTimestamp(TSystemTimestamp(1));
        output->SetParents({}, {}, {})->AddMessage(builder.Finish());
    }
};

//! Emits twelve messages with every input of the batch as their parents.
class TGraphFanOutFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        if (input->GetMessages().empty()) {
            return;
        }
        for (int index = 0; index < 12; ++index) {
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set(GetColumnValue<ui64>(input->GetMessages().front(), "key"), "key");
            output->AddMessage(builder.Finish());
        }
    }
};

//! Records the IDs of the input messages in delivery order and relays each message.
class TGraphMessageIdProbeFunction
    : public IBatchProcessFunction
{
public:
    static std::vector<TMessageId>& GetObserved()
    {
        static std::vector<TMessageId> MessageIds;
        return MessageIds;
    }

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        for (const auto& message : input->GetMessages()) {
            GetObserved().push_back(message->MessageId);
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
            output->SetParents({message}, {}, {})->AddMessage(builder.Finish());
        }
    }
};

class TGraphSyncFunction
    : public IProcessFunction
    , public ISyncProcessFunction
{
public:
    void Sync(const IRetryableTransactionPtr& /*transaction*/, const IRuntimeContextPtr& /*context*/) override
    {
        ADD_FAILURE() << "A sync process function must not run inside a graph";
    }
};

struct TGraphSavedCollectorParameters
    : public TYsonStruct
{
    bool PrimeParents = false;
    std::string Operation;

    REGISTER_YSON_STRUCT(TGraphSavedCollectorParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("prime_parents", &TThis::PrimeParents)
            .Default(false);
        registrar.Parameter("operation", &TThis::Operation)
            .Default("message");
    }
};

//! Attempts to use a collector retained from the previous invocation.
class TGraphSavedCollectorFunction
    : public IBatchProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& context) override
    {
        Parameters_ = context->GetParameters<TGraphSavedCollectorParameters>();
    }

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        if (!SavedOutput_) {
            SavedOutput_ = output->SetParents(input->GetMessages(), input->GetTimers(), {});
            if (Parameters_->PrimeParents) {
                SavedOutput_->AddMessage(context->ConvertToOutputMessage(*input->GetMessages().front()));
                SavedOutput_->AddTimer(TSystemTimestamp(10));
            }
            return;
        }
        if (Parameters_->Operation == "parents") {
            auto child = SavedOutput_->SetParents(input->GetMessages(), input->GetTimers(), {});
            child->AddMessage(context->ConvertToOutputMessage(*input->GetMessages().front()));
        } else if (Parameters_->Operation == "timer") {
            SavedOutput_->AddTimer(TSystemTimestamp(10));
        } else {
            SavedOutput_->AddMessage(context->ConvertToOutputMessage(*input->GetMessages().front()));
        }
    }

private:
    TIntrusivePtr<TGraphSavedCollectorParameters> Parameters_;
    IOutputCollectorPtr SavedOutput_;
};

//! Keeps the collector of its last batch to exercise its ownership.
class TGraphCollectorKeeperFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& /*input*/,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& /*context*/) override
    {
        Output_ = output;
    }

private:
    IOutputCollectorPtr Output_;
};

class TGraphFailingInitFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& /*context*/) override
    {
        THROW_ERROR_EXCEPTION("Init failed");
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(
    TGraphCounterFunction,
    TGraphCounterParameters,
    TGraphCounterDynamicParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphBatchFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphDiscardFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphRelayFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphChangingKeyFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphBatchMapFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphRepeatedBatchFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphMetaProbeFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphSuffixRelayFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphBatchSuffixFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphChainedTimersFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphWrongStreamFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphWatermarkProbeFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphTimerKeyFunction, TGraphTimerKeyParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphParentlessFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphFanOutFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphMessageIdProbeFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphSyncFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphSavedCollectorFunction, TGraphSavedCollectorParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphCollectorKeeperFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphFailingInitFunction);

////////////////////////////////////////////////////////////////////////////////

//! Payload schema of test streams: a stream schema must not be sorted, unlike the key schema.
TTableSchemaPtr MakeTestPayloadSchema()
{
    return ConvertTo<TTableSchemaPtr>(NYson::TYsonStringBuf("[{name=key;type=uint64;}]"));
}

TStreamSpecPtr MakeStreamSpec(const TTableSchemaPtr& schema = MakeTestPayloadSchema())
{
    auto spec = New<TStreamSpec>();
    spec->Schema = schema;
    return spec;
}

//! Replaces `$name` placeholders with the registered names of the test functions.
std::string SubstituteFunctions(TStringBuf yson)
{
    auto result = std::string(yson);
    SubstGlobal(result, "$counter", TypeName<TGraphCounterFunction>());
    SubstGlobal(result, "$batch_map", TypeName<TGraphBatchMapFunction>());
    SubstGlobal(result, "$batch", TypeName<TGraphBatchFunction>());
    SubstGlobal(result, "$discard", TypeName<TGraphDiscardFunction>());
    SubstGlobal(result, "$relay", TypeName<TGraphRelayFunction>());
    SubstGlobal(result, "$changing_key", TypeName<TGraphChangingKeyFunction>());
    SubstGlobal(result, "$repeated_batch", TypeName<TGraphRepeatedBatchFunction>());
    SubstGlobal(result, "$meta_probe", TypeName<TGraphMetaProbeFunction>());
    SubstGlobal(result, "$suffix_relay", TypeName<TGraphSuffixRelayFunction>());
    SubstGlobal(result, "$suffix_batch", TypeName<TGraphBatchSuffixFunction>());
    SubstGlobal(result, "$timers", TypeName<TGraphChainedTimersFunction>());
    SubstGlobal(result, "$wrong_stream", TypeName<TGraphWrongStreamFunction>());
    SubstGlobal(result, "$watermark_probe", TypeName<TGraphWatermarkProbeFunction>());
    SubstGlobal(result, "$timer_key", TypeName<TGraphTimerKeyFunction>());
    SubstGlobal(result, "$parentless", TypeName<TGraphParentlessFunction>());
    SubstGlobal(result, "$fan_out", TypeName<TGraphFanOutFunction>());
    SubstGlobal(result, "$id_probe", TypeName<TGraphMessageIdProbeFunction>());
    SubstGlobal(result, "$sync", TypeName<TGraphSyncFunction>());
    SubstGlobal(result, "$saved_collector", TypeName<TGraphSavedCollectorFunction>());
    SubstGlobal(result, "$collector_keeper", TypeName<TGraphCollectorKeeperFunction>());
    SubstGlobal(result, "$failing_init", TypeName<TGraphFailingInitFunction>());
    return result;
}

INodePtr ParseYson(TStringBuf yson)
{
    auto text = SubstituteFunctions(yson);
    return ConvertToNode(NYson::TYsonStringBuf(TStringBuf(text)));
}

TComputationSpecPtr ParseComputation(const INodePtr& node)
{
    auto computation = ConvertTo<TComputationSpecPtr>(node);
    computation->GroupBySchema = DefaultTestKeySchema();
    return computation;
}

TComputationSpecPtr ParseComputation(TStringBuf yson)
{
    return ParseComputation(ParseYson(yson));
}

TPipelineSpecPtr MakePipeline(const TComputationId& computationId, TStringBuf computationYson, const std::vector<TStringBuf>& streams)
{
    auto pipeline = New<TPipelineSpec>();
    for (const auto& streamId : streams) {
        pipeline->Streams[TStreamId(streamId)] = MakeStreamSpec();
    }
    pipeline->Computations[computationId] = ParseComputation(computationYson);
    return pipeline;
}

// Counters `a` and `b` with timers read one input stream each; `c` merges their outputs.
constexpr TStringBuf CountersComputation = R"({
    computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
    input_stream_ids = [left; right];
    output_stream_ids = [merged];
    process_function_graph = {
        nodes = {
            a = {
                processing_function = "$counter";
                processing_function_parameters = {increment = 2;};
                input_stream_ids = [left];
                output_stream_ids = [a_out];
                timer_streams = {timer = {};};
            };
            b = {
                processing_function = "$counter";
                processing_function_parameters = {increment = 3;};
                input_stream_ids = [right];
                output_stream_ids = [b_out];
                timer_streams = {timer = {};};
            };
            c = {
                processing_function = "$counter";
                input_stream_ids = [a_out; b_out];
                output_stream_ids = [merged];
            };
        };
    };
})";

const std::vector<TStringBuf> CountersStreams = {"left", "right", "a_out", "b_out", "merged"};

TPipelineSpecPtr MakeCountersPipeline()
{
    return MakePipeline(TComputationId("graph"), CountersComputation, CountersStreams);
}

//! Resolves a graph computation and runs its process function in a test environment.
class TGraphHarness
{
public:
    TGraphHarness(TStringBuf computationYson, const std::vector<TStringBuf>& streams)
        : Environment_(DefaultTestKeySchema())
        , Pipeline_(MakeResolvedPipeline(Environment_.GetComputationId(), computationYson, streams))
        , Computation_(Pipeline_->Computations.at(Environment_.GetComputationId()))
        , Function_(CreateFunction())
    { }

    TTestStateEnvironment& GetEnvironment()
    {
        return Environment_;
    }

    const TComputationSpecPtr& GetComputation() const
    {
        return Computation_;
    }

    const IBatchProcessFunctionPtr& GetFunction() const
    {
        return Function_;
    }

    IRuntimeContextPtr MakeRuntime(
        const TProcessFunctionGraphDynamicParametersPtr& dynamicParameters = New<TProcessFunctionGraphDynamicParameters>(),
        TSystemTimestamp currentTimestamp = TSystemTimestamp(2),
        std::optional<TUniqueSeqNo> epochSeqNo = TUniqueSeqNo(42)) const
    {
        TTestRuntimeContextBuilder builder;
        for (const auto& [streamId, streamSpec] : Pipeline_->Streams) {
            builder.RegisterStream(streamId, CloneYsonStruct(streamSpec->Schema));
        }
        if (epochSeqNo) {
            builder.SetEpochUniqueSeqNo(*epochSeqNo);
        }
        return builder
            .SetSpec(Computation_)
            .SetKeySchema(DefaultTestKeySchema())
            .SetCurrentTimestamp(currentTimestamp)
            .SetDynamicParameters(dynamicParameters)
            .Build();
    }

    TIntrusivePtr<TRecordingOutputCollector> Run(const std::vector<TInputMessageConstPtr>& messages)
    {
        auto input = New<TInputContext>(messages, std::vector<TInputTimerConstPtr>{});
        auto output = New<TRecordingOutputCollector>();
        Function_->Process(input, output, MakeRuntime());
        return output;
    }

private:
    TTestStateEnvironment Environment_;
    const TPipelineSpecPtr Pipeline_;
    const TComputationSpecPtr Computation_;
    const IBatchProcessFunctionPtr Function_;

    static TPipelineSpecPtr MakeResolvedPipeline(
        const TComputationId& computationId,
        TStringBuf computationYson,
        const std::vector<TStringBuf>& streams)
    {
        auto pipeline = MakePipeline(computationId, computationYson, streams);
        ResolveProcessFunctionGraphs(pipeline);
        return pipeline;
    }

    IBatchProcessFunctionPtr CreateFunction()
    {
        Environment_.SetStaticParameters(ConvertTo<TProcessFunctionGraphParametersPtr>(Computation_->ProcessingFunctionParameters));
        auto context = Environment_.CreateProcessFunctionContext();
        context->ComputationSpec = Computation_;
        auto function = WrapAsBatch(TRegistry::Get()->CreateProcessFunction(
            std::string(ProcessFunctionGraphFunctionName),
            context));
        function->Init(context->InitContext);
        return function;
    }
};

TInputMessageConstPtr MakeKeyMessage(TStringBuf streamId, ui64 key, std::optional<TSystemTimestamp> eventTimestamp = {})
{
    return MakeTestMessage(TStreamId(streamId), MakeKey<ui64>(ui64{key}), MakeTestPayloadSchema(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set<ui64>(key, "key");
        if (eventTimestamp) {
            builder.SetEventTimestamp(*eventTimestamp);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProcessFunctionGraphSpecTest, ResolvesGraphIntoComputationFields)
{
    auto pipeline = MakeCountersPipeline();
    auto authored = ConvertToNode(pipeline->Computations.at("graph")->ProcessFunctionGraph);
    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateComputationSpec(pipeline->Computations.at("graph")),
        R"(Computation with "process_function_graph" must be resolved first; the pipeline runner resolves it)");

    ResolveProcessFunctionGraphs(pipeline);
    const auto& computation = pipeline->Computations.at("graph");
    EXPECT_FALSE(computation->ProcessFunctionGraph);
    EXPECT_EQ(*computation->ProcessingFunction, std::string(ProcessFunctionGraphFunctionName));
    EXPECT_EQ(computation->InputStreamIds, THashSet<TStreamId>({"left", "right"}));
    EXPECT_EQ(computation->OutputStreamIds, THashSet<TStreamId>({"merged"}));
    EXPECT_TRUE(computation->AllowTimerSelfDependency);

    auto timerA = MakeProcessFunctionGraphTimerId("a", "timer");
    auto timerB = MakeProcessFunctionGraphTimerId("b", "timer");
    EXPECT_EQ(computation->StreamsDependency.at("merged"), THashSet<TStreamId>({"left", "right", timerA, timerB}));
    EXPECT_EQ(computation->StreamsDependency.at(timerA), THashSet<TStreamId>({"left", timerA}));
    EXPECT_EQ(*computation->TimerStreams.at(timerA)->Streams, THashSet<TStreamId>({"left"}));
    EXPECT_EQ(*computation->TimerStreams.at(timerB)->Streams, THashSet<TStreamId>({"right"}));

    // Parameters are kept as written; internal streams move into the graph parameters.
    auto parameters = ConvertTo<TProcessFunctionGraphParametersPtr>(computation->ProcessingFunctionParameters);
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(parameters->Graph), authored));
    EXPECT_FALSE(parameters->Graph->Nodes.at("c")->ProcessingFunctionParameters);
    auto graphStreams = GetKeys(parameters->Streams);
    std::sort(graphStreams.begin(), graphStreams.end());
    EXPECT_EQ(graphStreams, std::vector<TStreamId>({"a_out", "b_out", "left", "merged", "right"}));
    auto pipelineStreams = GetKeys(pipeline->Streams);
    std::sort(pipelineStreams.begin(), pipelineStreams.end());
    EXPECT_EQ(pipelineStreams, std::vector<TStreamId>({"left", "merged", "right"}));

    EXPECT_NO_THROW(ValidateProcessFunctionGraphComputation(pipeline, TComputationId("graph"), computation));
    for (const auto& error : TRegistry::Get()->ValidatePipelineSpecParseability(ConvertToNode(pipeline)->AsMap())) {
        ADD_FAILURE() << ToString(error);
    }

    auto snapshot = ConvertToNode(pipeline);
    ResolveProcessFunctionGraphs(pipeline);
    EXPECT_TRUE(AreNodesEqual(snapshot, ConvertToNode(pipeline)));
}

TEST(TProcessFunctionGraphSpecTest, DownstreamTimerWatchesUpstreamTimersButNotItself)
{
    auto pipeline = MakeCountersPipeline();
    pipeline->Computations.at("graph")->ProcessFunctionGraph->Nodes.at("c")->TimerStreams["timer"] = New<TTimerSpec>();
    ResolveProcessFunctionGraphs(pipeline);
    const auto& computation = pipeline->Computations.at("graph");

    auto localTimerId = MakeProcessFunctionGraphTimerId("c", "timer");
    auto globalId = [&] (TStringBuf nodeId) {
        return MakeGlobalStreamId(TComputationId("graph"), MakeProcessFunctionGraphTimerId(nodeId, "timer"), computation);
    };
    const auto& watched = *computation->TimerStreams.at(localTimerId)->Streams;
    EXPECT_EQ(watched, THashSet<TStreamId>({"left", "right", globalId("a"), globalId("b")}));
    EXPECT_TRUE(computation->StreamsDependency.at(localTimerId).contains(localTimerId));
}

TEST(TProcessFunctionGraphSpecTest, SystemTimeTimersDoNotWatchUpstreamTimers)
{
    auto pipeline = MakeCountersPipeline();
    auto& nodes = pipeline->Computations.at("graph")->ProcessFunctionGraph->Nodes;
    nodes.at("c")->TimerStreams["timer"] = ConvertTo<TTimerSpecPtr>(NYson::TYsonStringBuf("{time_type = system_time;}"));
    // An output that depends only on a timer derives its system time from the inputs of its node.
    nodes.at("b")->StreamsDependency["b_out"] = {"timer"};
    ResolveProcessFunctionGraphs(pipeline);
    const auto& computation = pipeline->Computations.at("graph");
    EXPECT_EQ(
        *computation->TimerStreams.at(MakeProcessFunctionGraphTimerId("c", "timer"))->Streams,
        THashSet<TStreamId>({"left", "right"}));
    EXPECT_NO_THROW(ValidateProcessFunctionGraphComputation(pipeline, TComputationId("graph"), computation));
}

TEST(TProcessFunctionGraphSpecTest, MapsTimerWatchedStreamsToBoundaries)
{
    // Explicit `streams` on an internal input watch its external inputs and upstream timers.
    auto pipeline = MakeCountersPipeline();
    auto timer = ConvertTo<TTimerSpecPtr>(NYson::TYsonStringBuf("{streams = [a_out];}"));
    pipeline->Computations.at("graph")->ProcessFunctionGraph->Nodes.at("c")->TimerStreams["timer"] = timer;
    ResolveProcessFunctionGraphs(pipeline);
    const auto& computation = pipeline->Computations.at("graph");
    auto upstreamTimer = MakeGlobalStreamId(TComputationId("graph"), MakeProcessFunctionGraphTimerId("a", "timer"), computation);
    EXPECT_EQ(
        *computation->TimerStreams.at(MakeProcessFunctionGraphTimerId("c", "timer"))->Streams,
        THashSet<TStreamId>({"left", upstreamTimer}));

    // Delays of inputs that derive from one boundary stream merge to the largest one.
    auto delayed = MakePipeline(TComputationId("graph"), R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
        input_stream_ids = [left];
        output_stream_ids = [merged];
        process_function_graph = {
            nodes = {
                a = {processing_function = "$relay"; input_stream_ids = [left]; output_stream_ids = [a_out];};
                b = {processing_function = "$relay"; input_stream_ids = [left]; output_stream_ids = [b_out];};
                c = {
                    processing_function = "$counter";
                    input_stream_ids = [a_out; b_out];
                    output_stream_ids = [merged];
                    timer_streams = {timer = {streams_with_delays = {a_out = 100; b_out = 200;};};};
                };
            };
        };
    })",
        {"left", "a_out", "b_out", "merged"});
    ResolveProcessFunctionGraphs(delayed);
    const auto& delayedTimer = delayed->Computations.at("graph")->TimerStreams.at(MakeProcessFunctionGraphTimerId("c", "timer"));
    EXPECT_FALSE(delayedTimer->Streams);
    EXPECT_EQ(*delayedTimer->StreamsWithDelays, (THashMap<TStreamId, TDuration>{{"left", TDuration::MilliSeconds(200)}}));
}

TEST(TProcessFunctionGraphSpecTest, RejectsInvalidGraphsAtomically)
{
    struct TCase
    {
        std::vector<std::pair<TStringBuf, TStringBuf>> Patches;
        TStringBuf Error;
    };

    const std::vector<TCase> cases = {
        {{{"/process_function_graph/nodes/a/processing_function", "unknown"}}, "Unknown processing function"},
        {{{"/process_function_graph/nodes/a/processing_function", R"("NYT::NFlow::TGraphProcessFunction")"}}, "Nested process function graphs"},
        {{{"/process_function_graph/nodes/a/processing_function_parameters/typo", "1"}}, R"(Invalid process function graph node "a")"},
        {{{"/process_function_graph/nodes/a/processing_function_parameters/increment", "wrong_type"}}, R"(Invalid process function graph node "a")"},
        {{{"/process_function_graph/nodes/c/input_stream_ids", "[a_out; b_out; missing]"}},
            R"("missing" of process function graph node "c" is neither an input stream of the computation nor an output stream of another node)"},
        {{{"/process_function_graph/nodes/a/output_stream_ids", "[a_out; b_out]"}}, R"("b_out" is also written by node "a")"},
        {{{"/process_function_graph/nodes/c/output_stream_ids", "[merged; a_out]"}}, "is both an input and an output stream of the node"},
        {{{"/process_function_graph/nodes/a/output_stream_ids", "[a_out; right]"}}, R"("right" is an input stream of the computation)"},
        {{{"/process_function_graph/nodes/c/input_stream_ids", "[a_out]"}}, R"("b_out" of process function graph node "b" is neither read by another node)"},
        {{{"/input_stream_ids", "[left]"}}, R"("right" of process function graph node "b" is neither an input stream of the computation)"},
        {{{"/input_stream_ids", "[left; right; loop]"}}, R"(Input stream "loop" of the computation is not read by any process function graph node)"},
        {{{"/output_stream_ids", "[merged; loop]"}}, R"(Output stream "loop" of the computation is not written by any process function graph node)"},
        {{{"/process_function_graph/nodes/a/input_stream_ids", "[left; loop]"}, {"/process_function_graph/nodes/c/output_stream_ids", "[merged; loop]"}},
            "cycle through nodes [a, c]"},
        {{{"/process_function_graph/nodes/a/timer_streams/timer/streams", "[right]"}}, "which is not an input stream of the node"},
        {{{"/process_function_graph/nodes/a/timer_streams/a_out", "{}"}}, "has the ID of a message stream of the node"},
        {{{"/process_function_graph/nodes/a/timer_streams/timer", "{streams = [left]; streams_with_delays = {left = 100;};}"}},
            R"(sets both "streams" and "streams_with_delays")"},
        {{{"/process_function_graph/nodes/a/streams_dependency", "{missing = [left];}"}}, "is neither an output stream nor a timer stream"},
        {{{"/process_function_graph/nodes/a/streams_dependency", "{a_out = [right];}"}}, "is neither an input stream nor a timer stream"},
        {{{"/process_function_graph/nodes/a/streams_dependency", "{a_out = [];}"}}, "must depend on at least one stream"},
        {{{"/process_function_graph/nodes/bad.id", R"({processing_function = "$discard"; input_stream_ids = [left];})"}}, "Invalid process function graph node ID"},
        {{{"/timer_streams", "{timer = {};}"}}, "cannot be combined with"},
        {{{"/streams_dependency", "{merged = [left];}"}}, "cannot be combined with"},
        {{{"/processing_function", R"("$counter")"}}, "cannot be combined with"},
        {{{"/processing_function_parameters", "{}"}}, "cannot be combined with"},
        {{{"/allow_timer_self_dependency", "%true"}}, "cannot be combined with"},
        {{{"/computation_class_name", R"("NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation")"}}, "requires computation class"},
        {{{"/watermark_strategy/event_timestamp_assigner", "{column = key;}"}}, R"(cannot set "watermark_strategy/event_timestamp_assigner")"},
        {{{"/watermark_strategy/watermark_percentile/value", "50"}}, R"("watermark_strategy/watermark_percentile/value" must be 100)"},
        {{{"/process_function_graph/nodes/a/output_stream_ids", R"(["a/out"])"}, {"/process_function_graph/nodes/c/input_stream_ids", R"(["a/out"; b_out])"}},
            R"(Invalid stream ID "a/out")"},
        {{{"/process_function_graph/nodes/a/input_bindings", "{}"}}, ""},
        {{{"/process_function_graph/nodes/a/input_stream_ids", "[]"}}, ""},
        {{{"/process_function_graph/nodes", "{}"}}, ""},
    };
    for (const auto& [patches, error] : cases) {
        SCOPED_TRACE(Format("%v", patches));
        auto resolve = [&] {
            auto node = ParseYson(CountersComputation);
            for (const auto& [path, value] : patches) {
                SetNodeByYPath(node, NYPath::TYPath(path), ParseYson(value), /*force*/ true);
            }
            auto pipeline = MakeCountersPipeline();
            pipeline->Streams["loop"] = MakeStreamSpec();
            pipeline->Computations["graph"] = ParseComputation(node);

            // A valid graph resolved before the invalid one must stay unresolved.
            auto valid = MakeCountersPipeline()->Computations.at("graph");
            auto rename = [] (THashSet<TStreamId>* streams) {
                THashSet<TStreamId> renamed;
                for (const auto& streamId : *streams) {
                    renamed.insert(TStreamId(Format("valid_%v", streamId)));
                }
                *streams = std::move(renamed);
            };
            for (const auto& [_, nodeSpec] : valid->ProcessFunctionGraph->Nodes) {
                rename(&nodeSpec->InputStreamIds);
                rename(&nodeSpec->OutputStreamIds);
            }
            rename(&valid->InputStreamIds);
            rename(&valid->OutputStreamIds);
            for (const auto& streamId : CountersStreams) {
                pipeline->Streams[TStreamId(Format("valid_%v", streamId))] = MakeStreamSpec();
            }
            pipeline->Computations["a_valid"] = valid;

            auto snapshot = ConvertToNode(pipeline);
            try {
                ResolveProcessFunctionGraphs(pipeline);
            } catch (const std::exception&) {
                EXPECT_TRUE(AreNodesEqual(snapshot, ConvertToNode(pipeline)));
                throw;
            }
        };
        if (error.empty()) {
            EXPECT_THROW(resolve(), std::exception);
        } else {
            EXPECT_THROW_WITH_SUBSTRING(resolve(), error);
        }
    }
}

TEST(TProcessFunctionGraphSpecTest, RejectsInvalidStreams)
{
    {
        auto pipeline = MakeCountersPipeline();
        pipeline->Streams.erase("a_out");
        EXPECT_THROW_WITH_SUBSTRING(ResolveProcessFunctionGraphs(pipeline), R"("a_out" is not registered in pipeline "streams")");
    }
    {
        auto pipeline = MakeCountersPipeline();
        auto other = New<TComputationSpec>();
        other->ComputationClassName = "NYT::NFlow::TProcessFunctionComputation";
        other->InputStreamIds = {"a_out"};
        pipeline->Computations["other"] = other;
        EXPECT_THROW_WITH_SUBSTRING(ResolveProcessFunctionGraphs(pipeline), R"(is also used by computation "other")");
    }
    {
        auto pipeline = MakeCountersPipeline();
        pipeline->Streams["a_out"] = MakeStreamSpec(New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("value", EValueType::Uint64),
        }));
        EXPECT_THROW_WITH_SUBSTRING(ResolveProcessFunctionGraphs(pipeline), R"("a_out" cannot be grouped)");
    }
    {
        auto pipeline = MakeCountersPipeline();
        pipeline->Computations.at("graph")->GroupBySchema = New<TTableSchema>();
        EXPECT_THROW_WITH_SUBSTRING(ResolveProcessFunctionGraphs(pipeline), R"(requires "group_by_schema")");
    }
}

TEST(TProcessFunctionGraphSpecTest, ValidatesResolvedGraph)
{
    auto makeResolved = [] {
        auto pipeline = MakeCountersPipeline();
        ResolveProcessFunctionGraphs(pipeline);
        return pipeline;
    };
    auto validate = [] (const TPipelineSpecPtr& pipeline) {
        ValidateProcessFunctionGraphComputation(pipeline, TComputationId("graph"), pipeline->Computations.at("graph"));
    };
    EXPECT_NO_THROW(validate(makeResolved()));
    {
        auto pipeline = makeResolved();
        pipeline->Computations.at("graph")->StreamsDependency.at("merged").erase("left");
        EXPECT_THROW_WITH_SUBSTRING(validate(pipeline), R"("streams_dependency" of the computation does not match)");
    }
    {
        auto pipeline = makeResolved();
        pipeline->Computations.at("graph")->TimerStreams.clear();
        EXPECT_THROW_WITH_SUBSTRING(validate(pipeline), R"("timer_streams" of the computation do not match)");
    }
    {
        auto pipeline = makeResolved();
        pipeline->Streams["a_out"] = MakeStreamSpec();
        EXPECT_THROW_WITH_SUBSTRING(validate(pipeline), R"(must not be registered in pipeline "streams")");
    }
    {
        auto pipeline = makeResolved();
        pipeline->Streams["left"]->ClassName = "Other";
        EXPECT_THROW_WITH_SUBSTRING(validate(pipeline), "differs from its pipeline spec");
    }
    for (bool sorted : {false, true}) {
        SCOPED_TRACE(sorted);
        auto pipeline = MakeCountersPipeline();
        if (sorted) {
            pipeline->Streams["a_out"] = MakeStreamSpec(DefaultTestKeySchema());
        } else {
            pipeline->Streams["a_out"]->ClassName = "NYT::NFlow::TUnknownMessage";
        }
        ResolveProcessFunctionGraphs(pipeline);
        EXPECT_THROW_WITH_SUBSTRING(validate(pipeline), R"(Invalid spec of process function graph stream "a_out")");
    }
    {
        auto pipeline = makeResolved();
        pipeline->Computations.at("graph")->ComputationClassName = "NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation";
        EXPECT_THROW_WITH_SUBSTRING(validate(pipeline), "requires computation class");
    }
    {
        auto pipeline = makeResolved();
        pipeline->Computations.at("graph")->OutputStreamIds.insert("left_copy");
        EXPECT_THROW_WITH_SUBSTRING(validate(pipeline), "is not written by any process function graph node");
    }
}

TEST(TProcessFunctionGraphSpecTest, ReportsUnrecognizedNodeParameters)
{
    auto pipeline = MakeCountersPipeline();
    ResolveProcessFunctionGraphs(pipeline);
    const auto& computation = pipeline->Computations.at("graph");
    auto parameters = ConvertTo<TProcessFunctionGraphParametersPtr>(computation->ProcessingFunctionParameters);
    parameters->Graph->Nodes.at("a")->ProcessingFunctionParameters = ConvertTo<IMapNodePtr>(NYson::TYsonStringBuf("{typo=1;}"));
    computation->ProcessingFunctionParameters = ConvertTo<IMapNodePtr>(parameters);
    EXPECT_FALSE(TRegistry::Get()->ValidatePipelineSpecParseability(ConvertToNode(pipeline)->AsMap()).empty());
}

TEST(TProcessFunctionGraphSpecTest, KeepsUnrecognizedFieldsOnResolution)
{
    // Like other computations, a graph computation keeps unrecognized fields of the loaded pipeline spec.
    auto node = ConvertToNode(MakeCountersPipeline());
    SetNodeByYPath(node, "/computations/graph/worker_grup", ConvertToNode("heavy"));
    SetNodeByYPath(node, "/streams/a_out/clas_name", ConvertToNode("Other"));
    auto pipeline = ConvertTo<TPipelineSpecPtr>(node);
    ResolveProcessFunctionGraphs(pipeline);

    auto resolved = ConvertToNode(pipeline);
    EXPECT_EQ(ConvertTo<std::string>(GetNodeByYPath(resolved, "/computations/graph/worker_grup")), "heavy");
    EXPECT_EQ(
        ConvertTo<std::string>(GetNodeByYPath(resolved, "/computations/graph/processing_function_parameters/streams/a_out/clas_name")),
        "Other");
}

TEST(TProcessFunctionGraphSpecTest, RejectsInvalidDynamicParameters)
{
    auto pipeline = MakeCountersPipeline();
    ResolveProcessFunctionGraphs(pipeline);
    auto dynamicSpec = New<TDynamicPipelineSpec>();
    auto dynamicComputation = New<TDynamicComputationSpec>();
    dynamicSpec->Computations["graph"] = dynamicComputation;
    for (const auto& value : {"{nodes={missing={};};}", "{nodes={a={multiplier=wrong_type;};};}", "{nodes={a={typo=1;};};}", "{typo=1;}"}) {
        SCOPED_TRACE(value);
        dynamicComputation->ProcessingFunctionParameters = ConvertTo<IMapNodePtr>(NYson::TYsonStringBuf(value));
        EXPECT_THROW_WITH_SUBSTRING(
            ValidateProcessFunctionGraphDynamicSpec(pipeline, dynamicSpec),
            R"(Invalid dynamic parameters of process function graph computation "graph")");
    }
    dynamicComputation->ProcessingFunctionParameters = ConvertTo<IMapNodePtr>(NYson::TYsonStringBuf("{nodes={a={multiplier=2;};};}"));
    EXPECT_NO_THROW(ValidateProcessFunctionGraphDynamicSpec(pipeline, dynamicSpec));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProcessFunctionGraphTest, MergesBranchesAndIsolatesStateAndTimers)
{
    TGraphHarness harness(CountersComputation, CountersStreams);
    auto dynamicParameters = New<TProcessFunctionGraphDynamicParameters>();
    auto aDynamicParameters = New<TGraphCounterDynamicParameters>();
    aDynamicParameters->Multiplier = 4;
    dynamicParameters->Nodes["a"] = ConvertToNode(aDynamicParameters)->AsMap();
    auto runtime = harness.MakeRuntime(dynamicParameters);

    auto key = MakeKey<ui64>(7);
    auto input = New<TInputContext>(
        std::vector<TInputMessageConstPtr>{MakeKeyMessage("left", 7), MakeKeyMessage("right", 7)},
        std::vector<TInputTimerConstPtr>{});
    auto output = New<TRecordingOutputCollector>();
    auto& environment = harness.GetEnvironment();
    environment.PreloadEpoch(input);
    harness.GetFunction()->Process(input, output, runtime);
    environment.CommitEpoch();

    ASSERT_EQ(output->GetMessages().size(), 2u);
    EXPECT_EQ(output->GetMessages()[0].Message.StreamId, TStreamId("merged"));
    EXPECT_EQ(output->GetMessages()[0].Message.PayloadSchema, runtime->GetStreamSpecs()->GetSchema("merged"));
    EXPECT_EQ(output->GetMessages()[0].ParentIds.size(), 1u);
    EXPECT_EQ(output->GetMessages()[1].ParentIds.size(), 1u);
    EXPECT_EQ(output->GetTimers().size(), 2u);
    EXPECT_EQ(environment.ReadKeyState<i64>("functions/a/counter", key), 8);
    EXPECT_EQ(environment.ReadKeyState<i64>("functions/b/counter", key), 3);
    EXPECT_EQ(environment.ReadKeyState<i64>("functions/c/counter", key), 2);

    auto timer = MakeTestTimer(
        key,
        TSystemTimestamp(10),
        DefaultTestKeySchema(),
        MakeProcessFunctionGraphTimerId("a", "timer"));
    auto timerInput = New<TInputContext>(
        std::vector<TInputMessageConstPtr>{},
        std::vector<TInputTimerConstPtr>{timer});
    auto timerOutput = New<TRecordingOutputCollector>();
    environment.PreloadEpoch(timerInput);
    harness.GetFunction()->Process(timerInput, timerOutput, runtime);
    environment.CommitEpoch();

    ASSERT_EQ(timerOutput->GetMessages().size(), 1u);
    EXPECT_EQ(timerOutput->GetMessages()[0].ParentIds, std::vector<TMessageId>{timer->MessageId});
    EXPECT_EQ(environment.ReadKeyState<i64>("functions/c/counter", key), 3);
}

TEST(TProcessFunctionGraphTest, TimerDependenciesAllowChainingAndRearming)
{
    TGraphHarness harness(R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                timers = {
                    processing_function = "$timers";
                    input_stream_ids = [source];
                    output_stream_ids = [result];
                    timer_streams = {first = {time_type = current_time;}; second = {time_type = current_time;};};
                    streams_dependency = {first = [source]; second = [first; second];};
                };
            };
        };
    })",
        {"source", "result"});
    const auto& computation = harness.GetComputation();
    auto firstId = MakeProcessFunctionGraphTimerId("timers", "first");
    auto secondId = MakeProcessFunctionGraphTimerId("timers", "second");
    EXPECT_EQ(computation->StreamsDependency.at(firstId), THashSet<TStreamId>({"source"}));
    EXPECT_EQ(computation->StreamsDependency.at(secondId), THashSet<TStreamId>({firstId, secondId}));

    auto runtime = harness.MakeRuntime();
    auto key = MakeKey<ui64>(7);
    std::vector<TInputMessageConstPtr> messages{MakeKeyMessage("source", 7)};
    std::vector<TInputTimerConstPtr> timers;
    for (const auto& expectedId : {firstId, secondId, secondId}) {
        auto rootOutput = New<TRootOutputCollector>(
            computation,
            CreateUniqueMetaSetter(computation, TUniqueSeqNo(42), TSystemTimestamp(2), CreateEventTimestampAssigner(/*spec*/ nullptr)));
        harness.GetFunction()->Process(New<TInputContext>(messages, timers), rootOutput->SetParents(messages, timers, {}), runtime);
        auto result = rootOutput->CollectResult();
        ASSERT_EQ(result.OutputTimers.size(), 1u);
        EXPECT_EQ(result.OutputTimers.front().StreamId, expectedId);
        EXPECT_EQ(result.OutputTimers.front().Key, key);
        if (!timers.empty()) {
            ASSERT_EQ(result.OutputTimersParentMessageIds.front()->ParentTimers.size(), 1u);
            EXPECT_EQ(result.OutputTimersParentMessageIds.front()->ParentTimers.front()->MessageId, timers.front()->MessageId);
        }
        timers = {New<TInputTimer>(std::move(result.OutputTimers.front()), DefaultTestKeySchema())};
        messages.clear();
    }
}

TEST(TProcessFunctionGraphTest, LocalDependenciesAreNotMaskedByFanIn)
{
    for (bool perMessage : {false, true}) {
        SCOPED_TRACE(perMessage);
        TGraphHarness harness(Format(R"({
            computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
            input_stream_ids = [left; right];
            output_stream_ids = [result];
            process_function_graph = {
                nodes = {
                    select = {
                        processing_function = "%v";
                        input_stream_ids = [left; right];
                        output_stream_ids = [selected];
                        streams_dependency = {selected = [left];};
                    };
                    merge = {
                        processing_function = "$counter";
                        input_stream_ids = [selected; right];
                        output_stream_ids = [result];
                    };
                };
            };
        })",
            perMessage ? "$counter" : "$batch"),
            {"left", "right", "selected", "result"});

        auto first = MakeKeyMessage("left", 7, TSystemTimestamp(100));
        auto second = MakeKeyMessage("right", 7);
        auto input = New<TInputContext>(
            std::vector<TInputMessageConstPtr>{first, second},
            std::vector<TInputTimerConstPtr>{});
        auto output = New<TRecordingOutputCollector>();
        auto runtime = harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), TSystemTimestamp(200));
        harness.GetEnvironment().PreloadEpoch(input);
        if (perMessage) {
            EXPECT_THROW_WITH_SUBSTRING(
                harness.GetFunction()->Process(input, output, runtime),
                R"(No parent of stream "selected" of process function graph node "select" is allowed by its dependencies)");
        } else {
            harness.GetFunction()->Process(input, output, runtime);
            ASSERT_EQ(output->GetMessages().size(), 2u);
            int leftResults = 0;
            for (const auto& message : output->GetMessages()) {
                ASSERT_EQ(message.ParentIds.size(), 1u);
                if (message.ParentIds[0] == first->MessageId) {
                    ++leftResults;
                    EXPECT_EQ(message.Message.EventTimestamp, first->EventTimestamp);
                } else {
                    EXPECT_EQ(message.ParentIds[0], second->MessageId);
                }
            }
            EXPECT_EQ(leftResults, 1);
        }
    }
}

TEST(TProcessFunctionGraphTest, OutputErrorsNameTheNodeAndFailTheEpochEvenIfCaught)
{
    TGraphHarness harness(R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                forward = {processing_function = "$wrong_stream"; input_stream_ids = [source]; output_stream_ids = [result];};
            };
        };
    })",
        {"source", "result"});
    EXPECT_THROW_WITH_SUBSTRING(
        harness.Run({MakeKeyMessage("source", 7)}),
        R"(Process function graph node "forward" emitted a message to stream "source", which is not one of its output streams)");
    EXPECT_THROW_WITH_SUBSTRING(harness.Run({MakeKeyMessage("source", 7)}), R"(Process function graph node "forward" failed)");
}

TEST(TProcessFunctionGraphTest, RejectsOutputsWithoutParents)
{
    TGraphHarness harness(R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                emit = {processing_function = "$parentless"; input_stream_ids = [source]; output_stream_ids = [ticks];};
                relay = {processing_function = "$relay"; input_stream_ids = [ticks]; output_stream_ids = [result];};
            };
        };
    })",
        {"source", "ticks", "result"});
    EXPECT_THROW_WITH_SUBSTRING(
        harness.Run({MakeKeyMessage("source", 7)}),
        R"(Output of process function graph node "emit" to stream "ticks" has no parents)");
}

TEST(TProcessFunctionGraphTest, OutputsOfFanOutShareParentsAndKeepEmissionOrder)
{
    TGraphHarness harness(R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                fan_out = {processing_function = "$fan_out"; input_stream_ids = [source]; output_stream_ids = [stage];};
                probe = {processing_function = "$id_probe"; input_stream_ids = [stage]; output_stream_ids = [result];};
            };
        };
    })",
        {"source", "stage", "result"});
    const auto& computation = harness.GetComputation();
    std::vector<TInputMessageConstPtr> messages{MakeKeyMessage("source", 7), MakeKeyMessage("source", 7), MakeKeyMessage("source", 7)};
    auto rootOutput = New<TRootOutputCollector>(
        computation,
        CreateUniqueMetaSetter(computation, TUniqueSeqNo(42), TSystemTimestamp(2), CreateEventTimestampAssigner(/*spec*/ nullptr)));
    TGraphMessageIdProbeFunction::GetObserved().clear();
    harness.GetFunction()->Process(New<TInputContext>(messages, std::vector<TInputTimerConstPtr>{}), rootOutput->SetParents(messages, {}, {}), harness.MakeRuntime());

    // Outputs derived from the same inputs share one parent list instead of copying it per output.
    auto result = rootOutput->CollectResult();
    ASSERT_EQ(result.OutputMessages.size(), 12u);
    for (const auto& parents : result.OutputMessagesParentMessageIds) {
        EXPECT_EQ(parents, result.OutputMessagesParentMessageIds.front());
    }
    EXPECT_EQ(result.OutputMessagesParentMessageIds.front()->ParentMessages.size(), 3u);

    // IDs of internal messages order them as they were emitted.
    const auto& ids = TGraphMessageIdProbeFunction::GetObserved();
    ASSERT_EQ(ids.size(), 12u);
    EXPECT_TRUE(std::is_sorted(ids.begin(), ids.end()));
}

TEST(TProcessFunctionGraphTest, InternalIdsSurviveReplayAndTransformExportsKeepFreshIds)
{
    std::vector<TInputMessageConstPtr> messages{MakeKeyMessage("source", 7)};
    const auto boundaryId = messages.front()->MessageId;
    std::vector<TMessageId> previousIds;
    std::vector<TMessageId> previousExportIds;
    for (i64 sequence : {42, 43}) {
        SCOPED_TRACE(sequence);
        TGraphHarness harness(R"({
            computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
            input_stream_ids = [source];
            output_stream_ids = [stage; result];
            process_function_graph = {
                nodes = {
                    fan_out = {processing_function = "$fan_out"; input_stream_ids = [source]; output_stream_ids = [stage];};
                    probe = {processing_function = "$id_probe"; input_stream_ids = [stage]; output_stream_ids = [result];};
                };
            };
        })",
            {"source", "stage", "result"});
        const auto& computation = harness.GetComputation();
        auto runtime = harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), TSystemTimestamp(2), TUniqueSeqNo(sequence));
        auto output = New<TRootOutputCollector>(
            computation,
            CreateUniqueMetaSetter(computation, TUniqueSeqNo(sequence), TSystemTimestamp(2), CreateEventTimestampAssigner(/*spec*/ nullptr)));
        TGraphMessageIdProbeFunction::GetObserved().clear();
        harness.GetFunction()->Process(
            New<TInputContext>(messages, std::vector<TInputTimerConstPtr>{}),
            output->SetParents(messages, {}, {}),
            runtime);

        const auto& ids = TGraphMessageIdProbeFunction::GetObserved();
        ASSERT_EQ(ids.size(), 12u);
        EXPECT_EQ(THashSet<TMessageId>(ids.begin(), ids.end()).size(), ids.size());
        if (!previousIds.empty()) {
            for (ssize_t index = 0; index < std::ssize(ids); ++index) {
                EXPECT_EQ(ids[index], previousIds[index]);
            }
        }
        previousIds = ids;

        // The same stream is exported and consumed internally; both exports retain the original input lineage.
        auto result = output->CollectResult();
        ASSERT_EQ(result.OutputMessages.size(), 24u);
        std::vector<TMessageId> exportIds;
        for (const auto& message : result.OutputMessages) {
            exportIds.push_back(message.MessageId);
        }
        if (!previousExportIds.empty()) {
            for (ssize_t index = 0; index < std::ssize(exportIds); ++index) {
                EXPECT_NE(exportIds[index], previousExportIds[index]);
            }
        }
        previousExportIds = std::move(exportIds);
        for (const auto& parents : result.OutputMessagesParentMessageIds) {
            ASSERT_EQ(parents->ParentMessages.size(), 1u);
            EXPECT_EQ(parents->ParentMessages.front()->MessageId, boundaryId);
            EXPECT_TRUE(parents->ParentTimers.empty());
        }
        EXPECT_EQ(messages.front()->MessageId, boundaryId);
    }
}

TEST(TProcessFunctionGraphTest, FanInAfterFanOutKeepsOriginalParents)
{
    TGraphHarness harness(R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                fan_out = {processing_function = "$fan_out"; input_stream_ids = [source]; output_stream_ids = [stage];};
                fan_in = {processing_function = "$batch"; input_stream_ids = [stage]; output_stream_ids = [result];};
            };
        };
    })",
        {"source", "stage", "result"});
    std::vector<TInputMessageConstPtr> messages{MakeKeyMessage("source", 7), MakeKeyMessage("source", 7), MakeKeyMessage("source", 7)};
    auto output = harness.Run(messages);

    // The twelve parents of the output share their origins, which are the three inputs.
    ASSERT_EQ(output->GetMessages().size(), 1u);
    auto parentIds = output->GetMessages().front().ParentIds;
    std::sort(parentIds.begin(), parentIds.end());
    std::vector<TMessageId> inputIds;
    for (const auto& message : messages) {
        inputIds.push_back(message->MessageId);
    }
    std::sort(inputIds.begin(), inputIds.end());
    EXPECT_EQ(parentIds, inputIds);
}

constexpr TStringBuf BatchChainComputation = R"({
    computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
    input_stream_ids = [source];
    output_stream_ids = [stage; result];
    process_function_graph = {
        nodes = {
            batch = {processing_function = "$batch_map"; input_stream_ids = [source]; output_stream_ids = [stage];};
            first = {processing_function = "$meta_probe"; input_stream_ids = [stage]; output_stream_ids = [middle];};
            second = {processing_function = "$meta_probe"; input_stream_ids = [middle]; output_stream_ids = [result];};
        };
    };
})";

std::string MakeBatchChainSpec(bool swift, bool merge = false)
{
    auto spec = std::string(BatchChainComputation);
    if (swift) {
        SubstGlobal(spec, "TProcessFunctionComputation", "TProcessFunctionSwiftMapComputation");
    }
    if (merge) {
        SubstGlobal(spec, "$batch_map", "$repeated_batch");
        if (swift) {
            SubstGlobal(spec, "computation_class_name =", "parameters = {allow_batching_with_relaxed_guarantees = %true;}; computation_class_name =");
        }
    }
    return spec;
}

TIntrusivePtr<TRootOutputCollector> MakeGraphRootOutput(
    const TComputationSpecPtr& computation,
    bool swift,
    TUniqueSeqNo sequence,
    TSystemTimestamp timestamp,
    bool merge = false)
{
    auto assigner = CreateEventTimestampAssigner(/*spec*/ nullptr);
    auto setter = swift
        ? (merge ? CreateSwiftMergeMetaSetter(computation, sequence, assigner) : CreateDeterministicMetaSetter(computation, assigner))
        : CreateUniqueMetaSetter(computation, sequence, timestamp, assigner);
    return New<TRootOutputCollector>(computation, setter);
}

TEST(TProcessFunctionGraphTest, BatchToPerMessageIdsSurviveReplayAndBatchChanges)
{
    auto first = MakeKeyMessage("source", 7, TSystemTimestamp(3));
    auto second = MakeKeyMessage("source", 8, TSystemTimestamp(4));
    auto extra = MakeKeyMessage("source", 9);
    // Keys identify distinct boundary inputs in this fixture.
    using TIdsByInputAndStream = TMap<std::pair<ui64, TStreamId>, std::vector<TMessageId>>;
    auto sortIds = [] (TIdsByInputAndStream& ids) {
        for (auto& [_, streamIds] : ids) {
            std::sort(streamIds.begin(), streamIds.end());
        }
    };
    TIdsByInputAndStream expectedInternalIds;
    TIdsByInputAndStream expectedSwiftExportIds;
    for (bool swift : {false, true}) {
        SCOPED_TRACE(Format("Swift: %v", swift));
        TGraphHarness harness(MakeBatchChainSpec(swift), {"source", "stage", "middle", "result"});
        auto pipeline = MakePipeline("graph", MakeBatchChainSpec(swift), {"source", "stage", "middle", "result"});
        ResolveProcessFunctionGraphs(pipeline);
        EXPECT_NO_THROW(ValidateProcessFunctionGraphComputation(pipeline, "graph", pipeline->Computations.at("graph")));
        const std::vector<std::vector<std::vector<TInputMessageConstPtr>>> variants{
            {{first, second}},
            {{second, first}},
            {{first}, {second}},
            {{extra, first, second}},
        };
        i64 sequence = 50;
        for (const auto& batches : variants) {
            TGraphMetaProbeFunction::GetObserved().clear();
            TGraphBatchMapFunction::GetBatchSizes().clear();
            TIdsByInputAndStream exportIds;
            for (const auto& batch : batches) {
                auto timestamp = TSystemTimestamp(++sequence);
                auto output = MakeGraphRootOutput(harness.GetComputation(), swift, TUniqueSeqNo(sequence), timestamp);
                ssize_t oldSize = std::ssize(TGraphMetaProbeFunction::GetObserved());
                harness.GetFunction()->Process(
                    New<TInputContext>(batch, std::vector<TInputTimerConstPtr>{}),
                    output->SetParents(batch, {}, {}),
                    harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), timestamp, swift ? std::nullopt : std::optional(TUniqueSeqNo(sequence))));
                const auto& observed = TGraphMetaProbeFunction::GetObserved();
                for (ssize_t index = oldSize; index < std::ssize(observed); ++index) {
                    EXPECT_EQ(observed[index].SystemTimestamp, swift ? TSystemTimestamp(1) : timestamp);
                    EXPECT_EQ(observed[index].AlignmentTimestamp, swift ? TSystemTimestamp(1) : timestamp);
                }
                auto result = output->CollectResult();
                ASSERT_EQ(result.OutputMessages.size(), batch.size() * 4);
                for (ssize_t index = 0; index < std::ssize(result.OutputMessages); ++index) {
                    const auto& message = result.OutputMessages[index];
                    const auto& parents = result.OutputMessagesParentMessageIds[index];
                    ASSERT_EQ(parents->ParentMessages.size(), 1u);
                    EXPECT_TRUE(parents->ParentTimers.empty());
                    const auto& parent = parents->ParentMessages.front();
                    EXPECT_EQ(GetColumnValue<ui64>(message, "key"), GetColumnValue<ui64>(parent, "key"));
                    EXPECT_EQ(message.EventTimestamp, parent->EventTimestamp);
                    EXPECT_EQ(message.SystemTimestamp, swift ? parent->SystemTimestamp : timestamp);
                    EXPECT_EQ(message.AlignmentTimestamp, swift ? parent->AlignmentTimestamp : timestamp);
                    if (GetColumnValue<ui64>(message, "key") != 9) {
                        exportIds[std::pair(GetColumnValue<ui64>(parent, "key"), message.StreamId)].push_back(message.MessageId);
                    }
                }
            }
            std::vector<size_t> batchSizes;
            for (const auto& batch : batches) {
                batchSizes.push_back(batch.size());
            }
            EXPECT_EQ(TGraphBatchMapFunction::GetBatchSizes(), batchSizes);
            TIdsByInputAndStream internalIds;
            THashSet<TMessageId> uniqueIds;
            for (const auto& message : TGraphMetaProbeFunction::GetObserved()) {
                ui64 key = GetColumnValue<ui64>(message, "key");
                if (key != 9) {
                    internalIds[std::pair(key, message.StreamId)].push_back(message.MessageId);
                    EXPECT_TRUE(uniqueIds.insert(message.MessageId).second);
                }
            }
            ASSERT_EQ(uniqueIds.size(), 8u);
            ASSERT_EQ(internalIds.size(), 4u);
            for (const auto& [_, streamIds] : internalIds) {
                EXPECT_EQ(streamIds.size(), 2u);
            }
            sortIds(internalIds);
            if (expectedInternalIds.empty()) {
                expectedInternalIds = internalIds;
            } else {
                EXPECT_EQ(internalIds, expectedInternalIds);
            }
            if (swift) {
                sortIds(exportIds);
                if (expectedSwiftExportIds.empty()) {
                    expectedSwiftExportIds = exportIds;
                } else {
                    EXPECT_EQ(exportIds, expectedSwiftExportIds);
                }
            }
        }
    }
}

TEST(TProcessFunctionGraphTest, MergedBatchToPerMessageSharesCountersAcrossCollectors)
{
    auto first = MakeKeyMessage("source", 7, TSystemTimestamp(3));
    auto second = MakeKeyMessage("source", 7, TSystemTimestamp(4));
    auto secondCopy = TMessage(*second);
    secondCopy.SystemTimestamp = TSystemTimestamp(5);
    secondCopy.AlignmentTimestamp = TSystemTimestamp(4);
    second = New<TInputMessage>(std::move(secondCopy), second->Key);
    std::vector<TMessageId> expectedInternalIds;
    for (bool swift : {false, true}) {
        TGraphHarness harness(MakeBatchChainSpec(swift, /*merge*/ true), {"source", "stage", "middle", "result"});
        std::vector<TMessageId> expectedExportIds;
        for (bool reverse : {false, true}) {
            SCOPED_TRACE(Format("Swift: %v, reverse: %v", swift, reverse));
            std::vector<TInputMessageConstPtr> batch = reverse
                ? std::vector<TInputMessageConstPtr>{second, first}
                : std::vector<TInputMessageConstPtr>{first, second};
            auto output = MakeGraphRootOutput(harness.GetComputation(), swift, TUniqueSeqNo(reverse ? 43 : 42), TSystemTimestamp(10), /*merge*/ true);
            TGraphMetaProbeFunction::GetObserved().clear();
            harness.GetFunction()->Process(
                New<TInputContext>(batch, std::vector<TInputTimerConstPtr>{}),
                output->SetParents(batch, {}, {}),
                harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), TSystemTimestamp(10), swift ? std::nullopt : std::optional(TUniqueSeqNo(42))));
            std::vector<TMessageId> internalIds;
            for (const auto& message : TGraphMetaProbeFunction::GetObserved()) {
                internalIds.push_back(message.MessageId);
                EXPECT_EQ(message.EventTimestamp, TSystemTimestamp(3));
                EXPECT_EQ(message.SystemTimestamp, swift ? TSystemTimestamp(5) : TSystemTimestamp(10));
                EXPECT_EQ(message.AlignmentTimestamp, swift ? TSystemTimestamp(1) : TSystemTimestamp(10));
            }
            ASSERT_EQ(internalIds.size(), 6u);
            EXPECT_EQ(THashSet<TMessageId>(internalIds.begin(), internalIds.end()).size(), internalIds.size());
            if (expectedInternalIds.empty()) {
                expectedInternalIds = internalIds;
            } else {
                EXPECT_EQ(internalIds, expectedInternalIds);
            }
            auto result = output->CollectResult();
            ASSERT_EQ(result.OutputMessages.size(), 6u);
            std::vector<TMessageId> exportIds;
            for (ssize_t index = 0; index < std::ssize(result.OutputMessages); ++index) {
                exportIds.push_back(result.OutputMessages[index].MessageId);
                const auto& parents = result.OutputMessagesParentMessageIds[index];
                EXPECT_EQ(parents->ParentMessages.size(), 2u);
                EXPECT_TRUE(parents->ParentTimers.empty());
                EXPECT_EQ(parents, result.OutputMessagesParentMessageIds.front());
            }
            EXPECT_EQ(THashSet<TMessageId>(exportIds.begin(), exportIds.end()).size(), exportIds.size());
            if (swift && reverse) {
                EXPECT_EQ(exportIds, expectedExportIds);
            }
            expectedExportIds = std::move(exportIds);
        }
    }
}

TEST(TProcessFunctionGraphTest, StrictSwiftRejectsMergedBoundaryParentsAndCanRetry)
{
    auto spec = MakeBatchChainSpec(/*swift*/ true);
    SubstGlobal(spec, "$batch_map", "$repeated_batch");
    TGraphHarness harness(spec, {"source", "stage", "middle", "result"});
    auto first = MakeKeyMessage("source", 7);
    auto second = MakeKeyMessage("source", 7);
    EXPECT_THROW_WITH_SUBSTRING(harness.Run({first, second}), "merging requires allow_batching_with_relaxed_guarantees=true");
    auto output = MakeGraphRootOutput(harness.GetComputation(), /*swift*/ true, TUniqueSeqNo(42), TSystemTimestamp(10));
    harness.GetFunction()->Process(
        New<TInputContext>(std::vector<TInputMessageConstPtr>{first}, std::vector<TInputTimerConstPtr>{}),
        output->SetParents({first}, {}, {}),
        harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), TSystemTimestamp(10), std::nullopt));
    auto result = output->CollectResult();
    EXPECT_EQ(result.OutputMessages.size(), 6u);
}

TEST(TProcessFunctionGraphTest, StrictSwiftFanInAcceptsDescendantsOfOneBoundaryParent)
{
    TGraphHarness harness(R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionSwiftMapComputation";
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                fan_out = {processing_function = "$fan_out"; input_stream_ids = [source]; output_stream_ids = [stage];};
                fan_in = {processing_function = "$batch"; input_stream_ids = [stage]; output_stream_ids = [merged];};
                relay = {processing_function = "$relay"; input_stream_ids = [merged]; output_stream_ids = [result];};
            };
        };
    })",
        {"source", "stage", "merged", "result"});
    auto parent = MakeKeyMessage("source", 7);
    auto output = MakeGraphRootOutput(harness.GetComputation(), /*swift*/ true, TUniqueSeqNo(42), TSystemTimestamp(10));
    harness.GetFunction()->Process(
        New<TInputContext>(std::vector<TInputMessageConstPtr>{parent}, std::vector<TInputTimerConstPtr>{}),
        output->SetParents({parent}, {}, {}),
        harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), TSystemTimestamp(10), std::nullopt));
    auto result = output->CollectResult();
    ASSERT_EQ(result.OutputMessages.size(), 1u);
    EXPECT_EQ(result.OutputMessagesParentMessageIds.front()->ParentMessages, std::vector<TInputMessageConstPtr>{parent});
}

TEST(TProcessFunctionGraphSpecTest, SwiftRejectsTimerDependentMessages)
{
    auto pipeline = MakeCountersPipeline();
    pipeline->Computations.at("graph")->ComputationClassName = "NYT::NFlow::TProcessFunctionSwiftMapComputation";
    EXPECT_THROW_WITH_SUBSTRING(ResolveProcessFunctionGraphs(pipeline), "must not depend on timer stream");
}

TEST(TProcessFunctionGraphTest, UserSuffixesAreScopedToImmediateParents)
{
    auto parent = MakeKeyMessage("source", 7);
    std::vector<TMessageId> expectedInternalIds;
    for (bool swift : {false, true}) {
        auto spec = MakeBatchChainSpec(swift);
        SubstGlobal(spec, "output_stream_ids = [stage; result];", "output_stream_ids = [stage; middle; result];");
        SubstGlobal(spec, "first = {processing_function = \"$meta_probe\"", "first = {processing_function = \"$suffix_relay\"");
        TGraphHarness harness(spec, {"source", "stage", "middle", "result"});
        std::vector<TMessageId> expectedSwiftExportIds;
        for (i64 sequence : {42, 43}) {
            auto output = MakeGraphRootOutput(harness.GetComputation(), swift, TUniqueSeqNo(sequence), TSystemTimestamp(sequence));
            TGraphMetaProbeFunction::GetObserved().clear();
            harness.GetFunction()->Process(
                New<TInputContext>(std::vector<TInputMessageConstPtr>{parent}, std::vector<TInputTimerConstPtr>{}),
                output->SetParents({parent}, {}, {}),
                harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), TSystemTimestamp(sequence), swift ? std::nullopt : std::optional(TUniqueSeqNo(sequence))));
            std::vector<TMessageId> internalIds;
            for (const auto& message : TGraphMetaProbeFunction::GetObserved()) {
                internalIds.push_back(message.MessageId);
            }
            ASSERT_EQ(internalIds.size(), 4u);
            EXPECT_EQ(THashSet<TMessageId>(internalIds.begin(), internalIds.end()).size(), internalIds.size());
            if (expectedInternalIds.empty()) {
                expectedInternalIds = internalIds;
            } else {
                EXPECT_EQ(internalIds, expectedInternalIds);
            }
            auto result = output->CollectResult();
            ASSERT_EQ(result.OutputMessages.size(), 10u);
            std::vector<TMessageId> exportIds;
            for (const auto& message : result.OutputMessages) {
                exportIds.push_back(message.MessageId);
            }
            EXPECT_EQ(THashSet<TMessageId>(exportIds.begin(), exportIds.end()).size(), exportIds.size());
            if (swift) {
                if (expectedSwiftExportIds.empty()) {
                    expectedSwiftExportIds = exportIds;
                } else {
                    EXPECT_EQ(exportIds, expectedSwiftExportIds);
                }
            }
        }
    }
}

TEST(TProcessFunctionGraphTest, BatchSuffixesKeepSeparateParentScopesOnReplay)
{
    auto parent = MakeKeyMessage("source", 7);
    std::vector<TMessageId> expectedInternalIds;
    for (bool swift : {false, true}) {
        auto spec = MakeBatchChainSpec(swift);
        SubstGlobal(spec, "$batch_map", "$fan_out");
        SubstGlobal(spec, "output_stream_ids = [stage; result];", "output_stream_ids = [stage; middle; result];");
        SubstGlobal(spec, "first = {processing_function = \"$meta_probe\"", "first = {processing_function = \"$suffix_batch\"");
        TGraphHarness harness(spec, {"source", "stage", "middle", "result"});
        std::vector<TMessageId> expectedExportIds;
        for (i64 sequence : {42, 43}) {
            auto output = MakeGraphRootOutput(harness.GetComputation(), swift, TUniqueSeqNo(sequence), TSystemTimestamp(sequence));
            TGraphMetaProbeFunction::GetObserved().clear();
            harness.GetFunction()->Process(
                New<TInputContext>(std::vector<TInputMessageConstPtr>{parent}, std::vector<TInputTimerConstPtr>{}),
                output->SetParents({parent}, {}, {}),
                harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), TSystemTimestamp(sequence), swift ? std::nullopt : std::optional(TUniqueSeqNo(sequence))));
            std::vector<TMessageId> internalIds;
            for (const auto& message : TGraphMetaProbeFunction::GetObserved()) {
                internalIds.push_back(message.MessageId);
            }
            // Both collectors reuse the same suffixes, but their immediate parent sets differ.
            ASSERT_EQ(internalIds.size(), 4u);
            EXPECT_EQ(THashSet<TMessageId>(internalIds.begin(), internalIds.end()).size(), internalIds.size());
            if (expectedInternalIds.empty()) {
                expectedInternalIds = internalIds;
            } else {
                EXPECT_EQ(internalIds, expectedInternalIds);
            }
            auto result = output->CollectResult();
            ASSERT_EQ(result.OutputMessages.size(), 20u);
            std::vector<TMessageId> exportIds;
            for (ssize_t index = 0; index < std::ssize(result.OutputMessages); ++index) {
                exportIds.push_back(result.OutputMessages[index].MessageId);
                EXPECT_EQ(result.OutputMessagesParentMessageIds[index]->ParentMessages, std::vector<TInputMessageConstPtr>{parent});
            }
            EXPECT_EQ(THashSet<TMessageId>(exportIds.begin(), exportIds.end()).size(), exportIds.size());
            if (swift && !expectedExportIds.empty()) {
                EXPECT_EQ(exportIds, expectedExportIds);
            }
            expectedExportIds = std::move(exportIds);
        }
    }
}

TEST(TProcessFunctionGraphTestHarnessTest, SwiftBatchChainReplaysWithoutEpochSequenceNumber)
{
    TTestStateEnvironment environment;
    TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(MakeBatchChainSpec(/*swift*/ true)));
    for (const auto& streamId : {"source", "stage", "middle", "result"}) {
        harness.RegisterStream(TStreamId(streamId), MakeTestPayloadSchema());
    }
    auto parent = MakeKeyMessage("source", 7);
    std::vector<TMessageId> previousIds;
    for (i64 timestamp : {10, 20}) {
        harness.SetCurrentTimestamp(TSystemTimestamp(timestamp));
        harness.RunEpoch({parent});
        ASSERT_EQ(harness.GetMessages().size(), 4u);
        std::vector<TMessageId> ids;
        for (const auto& output : harness.GetMessages()) {
            ids.push_back(output.Message.MessageId);
            EXPECT_EQ(output.Message.SystemTimestamp, parent->SystemTimestamp);
            EXPECT_EQ(output.ParentIds, std::vector<TMessageId>{parent->MessageId});
        }
        if (!previousIds.empty()) {
            EXPECT_EQ(ids, previousIds);
        }
        previousIds = std::move(ids);
    }
}

TEST(TProcessFunctionGraphTest, TransformTimerDescendantsKeepIdsButUseCurrentTimestamps)
{
    auto spec = MakeBatchChainSpec(/*swift*/ false);
    SubstGlobal(spec, "$batch_map", "$counter");
    SubstGlobal(spec, "batch = {", "batch = {timer_streams = {timer = {};};");
    TGraphHarness harness(spec, {"source", "stage", "middle", "result"});
    auto timer = MakeTestTimer(MakeKey<ui64>(ui64{7}), TSystemTimestamp(5), DefaultTestKeySchema(), MakeProcessFunctionGraphTimerId("batch", "timer"));
    std::vector<TMessageId> previousIds;
    for (i64 timestamp : {10, 20}) {
        auto output = MakeGraphRootOutput(harness.GetComputation(), /*swift*/ false, TUniqueSeqNo(timestamp), TSystemTimestamp(timestamp));
        TGraphMetaProbeFunction::GetObserved().clear();
        harness.GetFunction()->Process(
            New<TInputContext>(std::vector<TInputMessageConstPtr>{}, std::vector<TInputTimerConstPtr>{timer}),
            output->SetParents({}, {timer}, {}),
            harness.MakeRuntime(New<TProcessFunctionGraphDynamicParameters>(), TSystemTimestamp(timestamp), TUniqueSeqNo(timestamp)));
        std::vector<TMessageId> ids;
        for (const auto& message : TGraphMetaProbeFunction::GetObserved()) {
            ids.push_back(message.MessageId);
            EXPECT_EQ(message.SystemTimestamp, TSystemTimestamp(timestamp));
            EXPECT_EQ(message.AlignmentTimestamp, TSystemTimestamp(timestamp));
        }
        ASSERT_EQ(ids.size(), 2u);
        if (!previousIds.empty()) {
            EXPECT_EQ(ids, previousIds);
        }
        previousIds = std::move(ids);
        auto result = output->CollectResult();
        ASSERT_EQ(result.OutputMessages.size(), 2u);
        EXPECT_EQ(result.OutputMessagesParentMessageIds.front()->ParentTimers, std::vector<TInputTimerConstPtr>{timer});
    }
}

TEST(TProcessFunctionGraphTest, RejectsInputsWithUnexpectedSchemas)
{
    TGraphHarness harness(R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                relay = {processing_function = "$relay"; input_stream_ids = [source]; output_stream_ids = [result];};
            };
        };
    })",
        {"source", "result"});
    auto otherSchema = ConvertTo<TTableSchemaPtr>(NYson::TYsonStringBuf("[{name=key;type=uint64;};{name=extra;type=string;}]"));
    auto message = MakeTestMessage(TStreamId("source"), MakeKey<ui64>(ui64{7}), otherSchema, [] (TMessageBuilder& builder) {
        builder.Payload().Set<ui64>(7, "key");
        builder.Payload().Set(std::string("value"), "extra");
    });
    EXPECT_THROW_WITH_SUBSTRING(harness.Run({message}), R"(Input message of stream "source" has an unexpected schema)");
}

TEST(TProcessFunctionGraphTest, CollectorKeptByFunctionDoesNotKeepGraphAlive)
{
    TWeakPtr<IBatchProcessFunction> graph;
    {
        TGraphHarness harness(R"({
            computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
            input_stream_ids = [source];
            process_function_graph = {
                nodes = {
                    keeper = {processing_function = "$collector_keeper"; input_stream_ids = [source];};
                };
            };
        })",
            {"source"});
        harness.Run({MakeKeyMessage("source", 7)});
        graph = MakeWeak(harness.GetFunction());
    }
    // The graph owns the function, which owns the collector; the collector must not own the graph.
    EXPECT_TRUE(graph.IsExpired());
}

TEST(TProcessFunctionGraphTest, RejectsOutputsWithKeyDifferentFromSingleParent)
{
    for (bool swift : {false, true}) {
        auto spec = std::string(R"({
            computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
            input_stream_ids = [source];
            output_stream_ids = [result];
            process_function_graph = {
                nodes = {
                    producer = {processing_function = "$changing_key"; input_stream_ids = [source]; output_stream_ids = [changed];};
                    relay = {processing_function = "$relay"; input_stream_ids = [changed]; output_stream_ids = [result];};
                };
            };
        })");
        if (swift) {
            SubstGlobal(spec, "TProcessFunctionComputation", "TProcessFunctionSwiftMapComputation");
        }
        TGraphHarness harness(spec, {"source", "changed", "result"});
        EXPECT_THROW_WITH_SUBSTRING(
            harness.Run({MakeKeyMessage("source", 7)}),
            R"(Process function graph node "producer" changed the key of a message on stream "changed" read by other nodes)");
    }
}

TEST(TProcessFunctionGraphTest, RejectsBatchOutputsWithParentsOfDifferentKeys)
{
    TGraphHarness harness(R"({
        computation_class_name = "NYT::NFlow::TProcessFunctionComputation";
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                batch = {processing_function = "$batch"; input_stream_ids = [source]; output_stream_ids = [batched];};
                relay = {processing_function = "$relay"; input_stream_ids = [batched]; output_stream_ids = [result];};
            };
        };
    })",
        {"source", "batched", "result"});
    EXPECT_NO_THROW(harness.Run({MakeKeyMessage("source", 7), MakeKeyMessage("source", 7)}));
    EXPECT_THROW_WITH_SUBSTRING(
        harness.Run({MakeKeyMessage("source", 7), MakeKeyMessage("source", 8)}),
        "have different keys; a batch function must set the parents of each output");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProcessFunctionGraphTestHarnessTest, RunsEpochsWithStateTimersAndDynamicParameters)
{
    TTestStateEnvironment environment;
    TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(CountersComputation));
    for (const auto& streamId : CountersStreams) {
        harness.RegisterStream(TStreamId(streamId), MakeTestPayloadSchema());
    }
    auto multiplier = New<TGraphCounterDynamicParameters>();
    multiplier->Multiplier = 4;
    harness.SetNodeDynamicParameters("a", multiplier);

    auto key = MakeKey<ui64>(7);
    auto makeMessage = [&] (TStringBuf streamId) {
        return harness.MakeMessage(TStreamId(streamId), key, [] (TMessageBuilder& builder) {
            builder.Payload().Set<ui64>(7, "key");
        });
    };
    auto left = makeMessage("left");
    auto right = makeMessage("right");
    harness.SetCurrentTimestamp(TSystemTimestamp(5));
    harness.RunEpoch({left, right});

    ASSERT_EQ(harness.GetMessages().size(), 2u);
    THashSet<TMessageId> parents;
    for (const auto& output : harness.GetMessages()) {
        EXPECT_EQ(output.Message.StreamId, TStreamId("merged"));
        EXPECT_EQ(output.Message.SystemTimestamp, TSystemTimestamp(5));
        ASSERT_EQ(output.ParentIds.size(), 1u);
        parents.insert(output.ParentIds.front());
    }
    EXPECT_EQ(parents, THashSet<TMessageId>({left->MessageId, right->MessageId}));
    ASSERT_EQ(harness.GetTimers().size(), 2u);
    THashSet<std::string> timerNodes;
    for (const auto& timer : harness.GetTimers()) {
        EXPECT_EQ(timer.TimerStreamId, TStreamId("timer"));
        EXPECT_EQ(timer.Timer.StreamId, MakeProcessFunctionGraphTimerId(timer.NodeId, "timer"));
        timerNodes.insert(timer.NodeId);
    }
    EXPECT_EQ(timerNodes, THashSet<std::string>({"a", "b"}));
    EXPECT_EQ(harness.ReadNodeState<i64>("a", "counter", key), 8);
    EXPECT_EQ(harness.ReadNodeState<i64>("b", "counter", key), 3);
    EXPECT_EQ(harness.ReadNodeState<i64>("c", "counter", key), 2);

    // Timers scheduled in one epoch fire in the next one and return to the nodes that scheduled them.
    auto timers = harness.MakeFiringTimers();
    harness.SetNodeDynamicParameters("a", nullptr);
    harness.RunEpoch({}, timers);
    ASSERT_EQ(harness.GetMessages().size(), 2u);
    parents.clear();
    for (const auto& output : harness.GetMessages()) {
        ASSERT_EQ(output.ParentIds.size(), 1u);
        parents.insert(output.ParentIds.front());
    }
    EXPECT_EQ(parents, THashSet<TMessageId>({timers[0]->MessageId, timers[1]->MessageId}));
    EXPECT_TRUE(harness.GetTimers().empty());
    EXPECT_EQ(harness.ReadNodeState<i64>("a", "counter", key), 8);
    EXPECT_EQ(harness.ReadNodeState<i64>("c", "counter", key), 4);

    // Dynamic parameters changed or removed between epochs apply to the messages of the next epoch.
    multiplier->Multiplier = 2;
    harness.SetNodeDynamicParameters("a", multiplier);
    harness.RunEpoch({makeMessage("left")});
    EXPECT_EQ(harness.ReadNodeState<i64>("a", "counter", key), 12);
    harness.SetNodeDynamicParameters("a", nullptr);
    harness.RunEpoch({makeMessage("left")});
    EXPECT_EQ(harness.ReadNodeState<i64>("a", "counter", key), 14);
}

TEST(TProcessFunctionGraphTestHarnessTest, NodesSeePipelineWatermarksAndDeriveInternalOnes)
{
    TTestStateEnvironment environment;
    TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(R"({
        input_stream_ids = [left];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                a = {processing_function = "$relay"; input_stream_ids = [left]; output_stream_ids = [a_out]; timer_streams = {timer = {};};};
                probe = {processing_function = "$watermark_probe"; input_stream_ids = [a_out]; output_stream_ids = [result];};
            };
        };
    })"));
    for (const auto& streamId : {"left", "a_out", "result"}) {
        harness.RegisterStream(TStreamId(streamId), MakeTestPayloadSchema());
    }
    auto upstreamTimer = TStreamId(Format("%v/%v", environment.GetComputationId(), MakeProcessFunctionGraphTimerId("a", "timer")));
    harness
        .SetWatermark("left", TSystemTimestamp(100))
        .SetWatermark(upstreamTimer, TSystemTimestamp(50))
        .SetWatermark("unrelated", TSystemTimestamp(70));
    TGraphWatermarkProbeFunction::GetObserved().clear();
    harness.RunEpoch({harness.MakeMessage("left", MakeKey<ui64>(ui64{7}), [] (TMessageBuilder& builder) {
        builder.Payload().Set<ui64>(7, "key");
    })});

    const auto& observed = TGraphWatermarkProbeFunction::GetObserved();
    // An internal stream waits for its external inputs and the pending timers of its producer.
    EXPECT_EQ(observed.at("a_out"), TSystemTimestamp(50));
    EXPECT_EQ(observed.at("input"), TSystemTimestamp(50));
    EXPECT_EQ(observed.at("left"), TSystemTimestamp(100));
    EXPECT_EQ(observed.at("unrelated"), TSystemTimestamp(70));
}

TEST(TProcessFunctionGraphTestHarnessTest, LocalTimerWatermarkShadowsStreamWithSameId)
{
    TTestStateEnvironment environment;
    TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(R"({
        input_stream_ids = [left; timer];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                probe = {processing_function = "$watermark_probe"; input_stream_ids = [left]; output_stream_ids = [result]; timer_streams = {timer = {};};};
                other = {processing_function = "$discard"; input_stream_ids = [timer];};
            };
        };
    })"));
    for (const auto& streamId : {"left", "timer", "result"}) {
        harness.RegisterStream(TStreamId(streamId), MakeTestPayloadSchema());
    }
    auto probeTimer = TStreamId(Format("%v/%v", environment.GetComputationId(), MakeProcessFunctionGraphTimerId("probe", "timer")));
    harness
        .SetWatermark("left", TSystemTimestamp(100))
        .SetWatermark("timer", TSystemTimestamp(100))
        .SetWatermark(probeTimer, TSystemTimestamp(5));
    TGraphWatermarkProbeFunction::GetObserved().clear();
    harness.RunEpoch({harness.MakeMessage("left", MakeKey<ui64>(ui64{7}), [] (TMessageBuilder& builder) {
        builder.Payload().Set<ui64>(7, "key");
    })});

    // Inside the node, its local timer ID refers to its timer, not to the input stream of another node.
    const auto& observed = TGraphWatermarkProbeFunction::GetObserved();
    EXPECT_EQ(observed.at("timer"), TSystemTimestamp(5));
    EXPECT_EQ(observed.at("left"), TSystemTimestamp(100));
}

TEST(TProcessFunctionGraphTestHarnessTest, DefaultTimerDependenciesRejectSiblingTimers)
{
    for (bool declared : {false, true}) {
        SCOPED_TRACE(declared);
        TTestStateEnvironment environment;
        TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(Format(R"({
            input_stream_ids = [source];
            output_stream_ids = [result];
            process_function_graph = {
                nodes = {
                    timers = {
                        processing_function = "$timers";
                        input_stream_ids = [source];
                        output_stream_ids = [result];
                        timer_streams = {first = {time_type = current_time;}; second = {time_type = current_time;};};
                        %v
                    };
                };
            };
        })",
            declared ? "streams_dependency = {second = [first; second];};" : "")));
        harness
            .RegisterStream("source", MakeTestPayloadSchema())
            .RegisterStream("result", MakeTestPayloadSchema());
        harness.RunEpoch({harness.MakeMessage("source", MakeKey<ui64>(ui64{7}), [] (TMessageBuilder& builder) {
            builder.Payload().Set<ui64>(7, "key");
        })});
        ASSERT_EQ(harness.GetTimers().size(), 1u);
        EXPECT_EQ(harness.GetTimers().front().TimerStreamId, TStreamId("first"));
        auto fire = [&] {
            harness.RunEpoch({}, harness.MakeFiringTimers());
        };
        if (declared) {
            fire();
            ASSERT_EQ(harness.GetTimers().size(), 1u);
            EXPECT_EQ(harness.GetTimers().front().TimerStreamId, TStreamId("second"));
            EXPECT_EQ(harness.GetMessages().size(), 1u);
        } else {
            EXPECT_THROW_WITH_SUBSTRING(fire(), R"(No parent of stream "second" of process function graph node "timers" is allowed)");
        }
    }
}

TEST(TProcessFunctionGraphTestHarnessTest, TimersKeepExplicitKeysLikeCore)
{
    for (std::optional<ui64> timerKey : {std::optional<ui64>(), std::optional<ui64>(9)}) {
        SCOPED_TRACE(Format("TimerKey: %v", timerKey));
        TTestStateEnvironment environment;
        TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(Format(R"({
            input_stream_ids = [source];
            output_stream_ids = [result];
            process_function_graph = {
                nodes = {
                    timers = {
                        processing_function = "$timer_key";
                        processing_function_parameters = %v;
                        input_stream_ids = [source];
                        output_stream_ids = [result];
                        timer_streams = {timer = {time_type = current_time;};};
                    };
                };
            };
        })",
            timerKey ? std::string(Format("{timer_key = %v;}", *timerKey)) : std::string("{}"))));
        harness
            .RegisterStream("source", MakeTestPayloadSchema())
            .RegisterStream("result", MakeTestPayloadSchema());
        auto makeMessage = [&] (ui64 key) {
            return harness.MakeMessage("source", MakeKey<ui64>(ui64{key}), [&] (TMessageBuilder& builder) {
                builder.Payload().Set<ui64>(key, "key");
            });
        };

        // Without an explicit key a timer takes the key of its parents.
        harness.RunEpoch({makeMessage(7)});
        ASSERT_EQ(harness.GetTimers().size(), 1u);
        EXPECT_EQ(harness.GetTimers().front().Timer.Key, MakeKey<ui64>(timerKey.value_or(7)));
        auto firingTimers = harness.MakeFiringTimers();

        // Parents with different keys require an explicit key; an explicit key is not compared with the parents.
        if (timerKey) {
            harness.RunEpoch({makeMessage(7), makeMessage(8)});
            ASSERT_EQ(harness.GetTimers().size(), 1u);
            EXPECT_EQ(harness.GetTimers().front().Timer.Key, MakeKey<ui64>(ui64{*timerKey}));
        } else {
            EXPECT_THROW_WITH_SUBSTRING(
                harness.RunEpoch({makeMessage(7), makeMessage(8)}),
                R"(Parents of a timer of process function graph node "timers" have different keys)");
        }

        // A fired timer returns to its node with its own key.
        harness.RunEpoch({}, firingTimers);
        ASSERT_EQ(harness.GetMessages().size(), 1u);
        EXPECT_EQ(GetColumnValue<ui64>(harness.GetMessages().front().Message, "key"), timerKey.value_or(7));
        EXPECT_EQ(harness.GetMessages().front().ParentIds, std::vector<TMessageId>{firingTimers.front()->MessageId});
    }
}

TEST(TProcessFunctionGraphTestHarnessTest, DownstreamTimersWaitForUpstreamTimersOnlyInEventTime)
{
    for (bool systemTime : {false, true}) {
        SCOPED_TRACE(Format("SystemTime: %v", systemTime));
        TTestStateEnvironment environment;
        TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(Format(R"({
            input_stream_ids = [source];
            output_stream_ids = [result];
            process_function_graph = {
                nodes = {
                    upstream = {processing_function = "$counter"; input_stream_ids = [source]; output_stream_ids = [stage]; timer_streams = {timer = {};};};
                    downstream = {processing_function = "$counter"; input_stream_ids = [stage]; output_stream_ids = [result]; timer_streams = {timer = {time_type = %v;};};};
                };
            };
        })",
            systemTime ? "system_time" : "event_time")));
        for (const auto& streamId : {"source", "stage", "result"}) {
            harness.RegisterStream(TStreamId(streamId), MakeTestPayloadSchema());
        }
        // Both nodes schedule a timer triggering at 10.
        harness.RunEpoch({harness.MakeMessage("source", MakeKey<ui64>(ui64{7}), [] (TMessageBuilder& builder) {
            builder.Payload().Set<ui64>(7, "key");
            builder.SetEventTimestamp(TSystemTimestamp(5));
        })});
        ASSERT_EQ(harness.GetTimers().size(), 2u);

        const auto& computation = harness.GetComputationSpec();
        auto upstreamTimer = MakeProcessFunctionGraphTimerId("upstream", "timer");
        auto downstreamTimer = MakeProcessFunctionGraphTimerId("downstream", "timer");
        auto upstreamTimerGlobalId = MakeGlobalStreamId(environment.GetComputationId(), upstreamTimer, computation);
        // Timers emit messages with the current system timestamp, so pending timers do not hold back system time.
        EXPECT_EQ(
            *computation->TimerStreams.at(downstreamTimer)->Streams,
            systemTime ? THashSet<TStreamId>({"source"}) : THashSet<TStreamId>({"source", upstreamTimerGlobalId}));

        // The timers go through the core timer store configured with the generated computation spec.
        auto partition = New<TPartition>();
        partition->ComputationId = environment.GetComputationId();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->LowerKey = MakeKey<ui64>(0);
        partition->UpperKey = MakeKey<ui64>(100);
        auto context = New<TTimerStoreContext>();
        context->Partition = std::move(partition);
        context->KeySchema = DefaultTestKeySchema();
        context->StreamsDependency = computation->StreamsDependency;
        context->TimerSpecs = computation->TimerStreams;
        context->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
        context->Logger = NLogging::TLogger("Test");
        context->TimersTable = New<NTables::TInMemoryTimers>();
        auto dynamicContext = New<TDynamicTimerStoreContext>();
        dynamicContext->DynamicTimerStoreSpec = New<TDynamicTimerStoreSpec>();
        auto store = CreateTimerStore(context, dynamicContext);
        NConcurrency::WaitFor(store->Init())
            .ThrowOnError();
        std::vector<TTimer> timers;
        for (const auto& timer : harness.GetTimers()) {
            timers.push_back(timer.Timer);
        }
        store->Register(std::move(timers));

        auto advance = [&] (ui64 sourceWatermark, ui64 upstreamTimerWatermark) {
            auto state = New<TWatermarkState>();
            for (const auto& [streamId, value] : {std::pair(TStreamId("source"), sourceWatermark), std::pair(upstreamTimerGlobalId, upstreamTimerWatermark)}) {
                auto watermarks = New<TWatermarks>();
                watermarks->EventWatermark = TSystemTimestamp(value);
                watermarks->SystemWatermark = TSystemTimestamp(value);
                state->Streams[streamId] = std::move(watermarks);
            }
            store->UpdateWatermarkState(state);
            THashSet<TStreamId> fired;
            for (const auto& timer : store->GetNextBatch({upstreamTimer, downstreamTimer}, /*maxRows*/ 100, /*maxByteSize*/ 1_MB)) {
                fired.insert(timer->StreamId);
            }
            return fired;
        };
        // The input is complete, but the upstream timer is still pending.
        if (systemTime) {
            EXPECT_EQ(advance(/*sourceWatermark*/ 100, /*upstreamTimerWatermark*/ 5), THashSet<TStreamId>({upstreamTimer, downstreamTimer}));
        } else {
            EXPECT_EQ(advance(/*sourceWatermark*/ 100, /*upstreamTimerWatermark*/ 5), THashSet<TStreamId>({upstreamTimer}));
            // Once the upstream timer stream passes the trigger time, the downstream event-time timer fires too.
            EXPECT_EQ(advance(/*sourceWatermark*/ 100, /*upstreamTimerWatermark*/ 100), THashSet<TStreamId>({downstreamTimer}));
        }
    }
}

TEST(TProcessFunctionGraphTestHarnessTest, FailedInitializationIsNotRetried)
{
    TTestStateEnvironment environment;
    TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(R"({
        input_stream_ids = [source];
        process_function_graph = {
            nodes = {
                failing = {processing_function = "$failing_init"; input_stream_ids = [source];};
            };
        };
    })"));
    harness.RegisterStream("source", MakeTestPayloadSchema());
    EXPECT_THROW_WITH_SUBSTRING(harness.RunEpoch({}), R"(Cannot initialize process function graph node "failing")");
    // The next epoch must not run on a partly initialized graph.
    EXPECT_THROW_WITH_SUBSTRING(harness.RunEpoch({}), "failed to initialize in an earlier epoch");
}

TEST(TProcessFunctionGraphTestHarnessTest, RejectsMismatchedKeySchemaAndUnknownDynamicNodes)
{
    TTestStateEnvironment environment;
    auto computation = SubstituteFunctions(R"({
        input_stream_ids = [source];
        group_by_schema = [{name = other; type = uint64; sort_order = ascending;}];
        process_function_graph = {
            nodes = {
                discard = {processing_function = "$discard"; input_stream_ids = [source];};
            };
        };
    })");
    EXPECT_THROW_WITH_SUBSTRING(
        TProcessFunctionGraphTestHarness(environment, computation),
        R"("group_by_schema" of the computation differs from the key schema of the harness)");

    SubstGlobal(computation, "group_by_schema = [{name = other; type = uint64; sort_order = ascending;}];", "");
    TProcessFunctionGraphTestHarness harness(environment, computation);
    harness.RegisterStream("source", MakeTestPayloadSchema());
    harness.SetNodeDynamicParameters("missing", New<TGraphCounterDynamicParameters>());
    EXPECT_THROW_WITH_SUBSTRING(harness.RunEpoch({}), R"(Unknown process function graph node "missing" in dynamic parameters)");
    harness.SetNodeDynamicParameters("missing", nullptr);
    EXPECT_NO_THROW(harness.RunEpoch({}));
}

TEST(TProcessFunctionGraphSpecTest, RejectsSyncNodesBeforeResolutionAndAfterResolution)
{
    auto pipeline = MakeCountersPipeline();
    auto computation = pipeline->Computations.at("graph");
    computation->ProcessFunctionGraph->Nodes.at("b")->ProcessingFunction = TypeName<TGraphSyncFunction>();
    const auto original = ConvertToNode(pipeline);
    EXPECT_THROW_WITH_SUBSTRING(ResolveProcessFunctionGraphs(pipeline), "implementing ISyncProcessFunction");
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(pipeline)));

    pipeline = MakeCountersPipeline();
    ResolveProcessFunctionGraphs(pipeline);
    computation = pipeline->Computations.at("graph");
    auto parameters = ConvertTo<TProcessFunctionGraphParametersPtr>(computation->ProcessingFunctionParameters);
    parameters->Graph->Nodes.at("b")->ProcessingFunction = TypeName<TGraphSyncFunction>();
    parameters->Graph->Nodes.at("b")->ProcessingFunctionParameters.Reset();
    computation->ProcessingFunctionParameters = ConvertTo<IMapNodePtr>(parameters);
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateProcessFunctionGraphComputation(pipeline, TComputationId("graph"), computation),
        R"(Process function graph node "b" uses processing function)");

    // A host that bypassed spec validation must still reject the sync function before Init.
    TTestStateEnvironment environment;
    environment.SetStaticParameters(parameters);
    auto context = environment.CreateProcessFunctionContext();
    context->ComputationSpec = computation;
    auto function = WrapAsBatch(TRegistry::Get()->CreateProcessFunction(
        std::string(ProcessFunctionGraphFunctionName),
        context));
    EXPECT_THROW_WITH_SUBSTRING(function->Init(context->InitContext), "Sync process functions are not supported");
}

TEST(TProcessFunctionGraphTestHarnessTest, RejectsSyncNodes)
{
    TTestStateEnvironment environment;
    TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(R"({
        input_stream_ids = [source];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                sync_node = {processing_function = "$sync"; input_stream_ids = [source]; output_stream_ids = [result];};
            };
        };
    })"));
    harness.RegisterStream("source", MakeTestPayloadSchema());
    harness.RegisterStream("result", MakeTestPayloadSchema());
    EXPECT_THROW_WITH_SUBSTRING(harness.RunEpoch({}), "implementing ISyncProcessFunction");
}

TEST(TProcessFunctionGraphTestHarnessTest, RejectsCollectorsFromPreviousInvocation)
{
    for (bool primeParents : {false, true}) {
        for (const auto& operation : {"message", "timer", "parents"}) {
            SCOPED_TRACE(Format("PrimeParents: %v, Operation: %v", primeParents, operation));
            TTestStateEnvironment environment;
            TProcessFunctionGraphTestHarness harness(environment, SubstituteFunctions(Format(R"({
                input_stream_ids = [source];
                output_stream_ids = [result];
                process_function_graph = {
                    nodes = {
                        saved = {
                            processing_function = "$saved_collector";
                            processing_function_parameters = {prime_parents = %v; operation = "%v";};
                            input_stream_ids = [source];
                            output_stream_ids = [result];
                            timer_streams = {timer = {};};
                        };
                    };
                };
            })",
                primeParents ? "%true" : "%false",
                operation)));
            harness.RegisterStream("source", MakeTestPayloadSchema());
            harness.RegisterStream("result", MakeTestPayloadSchema());
            auto run = [&] {
                harness.RunEpoch({harness.MakeMessage("source", MakeKey<ui64>(ui64{7}), [] (TMessageBuilder& builder) {
                    builder.Payload().Set<ui64>(7, "key");
                })});
            };
            EXPECT_NO_THROW(run());
            EXPECT_THROW_WITH_SUBSTRING(run(), "outside its Process invocation");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
