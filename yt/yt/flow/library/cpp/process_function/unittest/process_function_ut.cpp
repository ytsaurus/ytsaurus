#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/in_memory_external_state_manager.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/string/split.h>
#include <util/system/type_name.h>

#include <algorithm>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NYT::NFlow::NTesting;

////////////////////////////////////////////////////////////////////////////////

struct TWordMessage
    : public TYsonMessage
{
    std::string Word;

    REGISTER_YSON_STRUCT(TWordMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("word", &TThis::Word)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TWordMessage);

////////////////////////////////////////////////////////////////////////////////

//! Per-key counter. Increments internal key state and emits a timer carrying the count.
class TCountingRowFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitClient<i64>(Counter_, "counter");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& /*context*/) override
    {
        auto state = Counter_.GetState(message->Key);
        *state = *state + 1;
        output->AddTimer(TSystemTimestamp(*state));
    }

private:
    TMutableStateKeyClient<i64> Counter_;
};

////////////////////////////////////////////////////////////////////////////////

//! Splits the "text" column into words, emitting one TWordMessage per word. A plain row
//! function — used below under a source computation, but equally hostable by transform.
class TSplitFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto text = GetColumnValue<std::string>(message, "text");
        for (const auto& word : StringSplitter(text).Split(' ').SkipEmpty()) {
            auto wordMessage = New<TWordMessage>();
            wordMessage->Word = word;
            output->AddMessage(context->ConvertToMessage(wordMessage));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Whole-batch variant of TSplitFunction: overrides Process to split every message in the
//! epoch's batch.
class TBatchSplitFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        for (const auto& message : input->GetMessages()) {
            auto text = GetColumnValue<std::string>(message, "text");
            for (const auto& word : StringSplitter(text).Split(' ').SkipEmpty()) {
                auto wordMessage = New<TWordMessage>();
                wordMessage->Word = word;
                output->AddMessage(context->ConvertToMessage(wordMessage));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Whole-batch function: sees the whole epoch's input — all keys — in one call (the
//! Process analog) and emits a single summary of the total message count.
class TWholeBatchFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto summary = New<TWordMessage>();
        summary->Word = Format("%v", input->GetMessages().size());
        output->AddMessage(context->ConvertToMessage(summary));
    }
};

//! Whole-key function: sees a key's messages and timers together in one call (the
//! ProcessKey analog) and emits a single summary of the counts.
class TKeySummaryFunction
    : public IKeyedBatchProcessFunction
{
public:
    void ProcessKey(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto summary = New<TWordMessage>();
        summary->Word = Format("%v:%v", input->GetMessages().size(), input->GetTimers().size());
        output->AddMessage(context->ConvertToMessage(summary));
    }
};

////////////////////////////////////////////////////////////////////////////////

// User-defined parameters struct, deserialized from the spec's `function_parameters` block.
struct TThresholdParameters
    : public NYTree::TYsonStruct
{
    i64 Threshold = 0;

    REGISTER_YSON_STRUCT(TThresholdParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("threshold", &TThis::Threshold)
            .Default(0);
    }
};

// Reads its static parameters in Init and its dynamic parameters per message; records both
// so the test can assert on them.
class TParameterReadingFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        StaticThreshold = initContext->GetParameters<TThresholdParameters>()->Threshold;
    }

    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& context) override
    {
        DynamicThreshold = context->GetDynamicParameters<TThresholdParameters>()->Threshold;
    }

    i64 StaticThreshold = -1;
    i64 DynamicThreshold = -1;
};

////////////////////////////////////////////////////////////////////////////////

// Opts into the end-of-epoch sync phase by deriving ISyncProcessFunction.
class TSyncingProcessFunction
    : public IProcessFunction
    , public ISyncProcessFunction
{
public:
    void Sync(const IRetryableTransactionPtr& /*transaction*/, const IRuntimeContextPtr& /*context*/) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! Per-key counter kept in external state: reads the "count" column, writes it back +1.
class TExternalCountingFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        auto state = StateClient_.GetState(message->Key);
        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

//! Reads a per-key "limit" from a read-only external state joiner and echoes it as a word.
class TJoinerReadingFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitExternalStateClient(ReferenceJoiner_, "/reference");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto state = ReferenceJoiner_.GetState(message->Key);
        i64 limit = state->GetColumnValue<std::optional<i64>>("limit").value_or(0);
        auto word = New<TWordMessage>();
        word->Word = Format("%v", limit);
        output->AddMessage(context->ConvertToMessage(word));
    }

private:
    TJoinedStateKeyClient<TSimpleExternalState> ReferenceJoiner_;
};

////////////////////////////////////////////////////////////////////////////////

// Register the test functions in the process-function registry (the hosting computation is
// chosen in the spec, not here).
YT_FLOW_DEFINE_PROCESS_FUNCTION(TCountingRowFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TSplitFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TBatchSplitFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TParameterReadingFunction, TThresholdParameters, TThresholdParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TSyncingProcessFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TExternalCountingFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TJoinerReadingFunction);

////////////////////////////////////////////////////////////////////////////////

TEST(TProcessFunctionTest, RowFunctionStateAndOutput)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder().Build();
    auto output = New<TRecordingOutputCollector>();

    auto function = New<TCountingRowFunction>();
    function->Init(stateEnv.GetInitContext());

    auto key = MakeKey<ui64>(7);
    auto emptySchema = New<TTableSchema>();

    auto message1 = MakeTestMessage("input", key, emptySchema);
    auto message2 = MakeTestMessage("input", key, emptySchema);
    stateEnv.PreloadKeyStates(New<TInputContext>(
        std::vector<TInputMessageConstPtr>{message1, message2},
        std::vector<TInputTimerConstPtr>{}));

    function->ProcessMessage(message1, output, context);
    function->ProcessMessage(message2, output, context);

    ASSERT_EQ(std::ssize(output->GetTimers()), 2);
    EXPECT_EQ(output->GetTimers()[0].TriggerTimestamp, TSystemTimestamp(1));
    EXPECT_EQ(output->GetTimers()[1].TriggerTimestamp, TSystemTimestamp(2));

    // State persisted and reloaded from the in-memory tables.
    EXPECT_EQ(stateEnv.ReadKeyState<i64>("counter", key), 2);
}

TEST(TProcessFunctionTest, RowFunctionPerKeyIsolation)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder().Build();
    auto output = New<TRecordingOutputCollector>();

    auto function = New<TCountingRowFunction>();
    function->Init(stateEnv.GetInitContext());

    auto emptySchema = New<TTableSchema>();
    auto message1 = MakeTestMessage("input", MakeKey<ui64>(1), emptySchema);
    auto message2 = MakeTestMessage("input", MakeKey<ui64>(2), emptySchema);
    stateEnv.PreloadKeyStates(New<TInputContext>(
        std::vector<TInputMessageConstPtr>{message1, message2},
        std::vector<TInputTimerConstPtr>{}));

    function->ProcessMessage(message1, output, context);
    function->ProcessMessage(message2, output, context);

    EXPECT_EQ(stateEnv.ReadKeyState<i64>("counter", MakeKey<ui64>(1)), 1);
    EXPECT_EQ(stateEnv.ReadKeyState<i64>("counter", MakeKey<ui64>(2)), 1);
}

TEST(TProcessFunctionTest, SourceFunctionEmitsWords)
{
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
    auto output = New<TRecordingOutputCollector>();

    auto inputSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=text;type=string}])")));
    auto message = MakeTestMessage("input", MakeKey<ui64>(0), inputSchema, [] (TMessageBuilder& builder) {
        builder.Payload().Set(std::string("hello world flow"), "text");
    });

    auto function = New<TSplitFunction>();
    function->ProcessMessage(message, output, context);

    ASSERT_EQ(std::ssize(output->GetMessages()), 3);
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[0].Message, "word"), "hello");
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[1].Message, "word"), "world");
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[2].Message, "word"), "flow");
}

TEST(TProcessFunctionTest, OutputCollectorStampsParents)
{
    auto output = New<TRecordingOutputCollector>();
    auto parent = MakeTestMessage("input", MakeKey<ui64>(1), New<TTableSchema>());

    auto child = output->SetParents({parent}, {}, {});
    child->AddTimer(TSystemTimestamp(5));

    ASSERT_EQ(std::ssize(output->GetTimers()), 1);
    ASSERT_EQ(std::ssize(output->GetTimers()[0].ParentIds), 1);
    EXPECT_EQ(output->GetTimers()[0].ParentIds[0], parent->MessageId);
}

TEST(TProcessFunctionTest, RegistrationUnderFunctionTypeName)
{
    // Each function is registered in the common registry under its TypeName and can be
    // instantiated by name.
    EXPECT_TRUE(static_cast<bool>(
        TRegistry::Get()->CreateProcessFunction(std::string(TypeName<TCountingRowFunction>()))));
    EXPECT_TRUE(static_cast<bool>(
        TRegistry::Get()->CreateProcessFunction(std::string(TypeName<TSplitFunction>()))));
    EXPECT_TRUE(static_cast<bool>(
        TRegistry::Get()->CreateProcessFunction(std::string(TypeName<TBatchSplitFunction>()))));

    // An unknown function name throws.
    EXPECT_THROW(TRegistry::Get()->CreateProcessFunction("NoSuchFunction"), std::exception);
}

TEST(TProcessFunctionTest, BatchSourceFunctionEmitsWords)
{
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
    auto output = New<TRecordingOutputCollector>();

    auto inputSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=text;type=string}])")));
    std::vector<TInputMessageConstPtr> messages;
    messages.push_back(MakeTestMessage("input", MakeKey<ui64>(0), inputSchema, [] (TMessageBuilder& builder) {
        builder.Payload().Set(std::string("hello world"), "text");
    }));
    messages.push_back(MakeTestMessage("input", MakeKey<ui64>(1), inputSchema, [] (TMessageBuilder& builder) {
        builder.Payload().Set(std::string("flow"), "text");
    }));

    auto function = New<TBatchSplitFunction>();
    function->Process(New<TInputContext>(messages, std::vector<TInputTimerConstPtr>{}), output, context);

    ASSERT_EQ(std::ssize(output->GetMessages()), 3);
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[0].Message, "word"), "hello");
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[1].Message, "word"), "world");
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[2].Message, "word"), "flow");
}

TEST(TProcessFunctionTest, ExternalStateManagerRegisteredGenerically)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder().Build();
    auto output = New<TRecordingOutputCollector>();

    auto stateSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=count;type=int64}])")));
    // The generic overload accepts any IExternalStateManager instance (a real serializable-
    // profile TProfileManager over a mock client would be registered the same way).
    stateEnv.RegisterExternalState("/state", New<TInMemorySimpleExternalStateManager>(stateSchema));

    auto function = New<TExternalCountingFunction>();
    function->Init(stateEnv.GetInitContext());

    auto key = MakeKey<ui64>(7);
    auto emptySchema = New<TTableSchema>();
    auto message = MakeTestMessage("input", key, emptySchema);

    function->ProcessMessage(message, output, context);
    function->ProcessMessage(message, output, context);

    EXPECT_EQ(
        stateEnv.ReadExternalKeyState<TSimpleExternalState>("/state", key)
            .GetColumnValue<std::optional<i64>>("count"),
        2);
}

TEST(TProcessFunctionTest, ExternalStateJoiner)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
    auto output = New<TRecordingOutputCollector>();

    auto stateSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=limit;type=int64}])")));
    auto joiner = stateEnv.RegisterExternalStateJoiner("/reference", stateSchema);

    // Seed the read-only reference state for the key the function will look up.
    auto key = MakeKey<ui64>(42);
    auto seed = joiner->GetMutableState(key);
    TPayloadBuilder builder(seed->Get().Schema);
    builder.Set(i64(100), "limit");
    seed->Get().Payload = builder.Finish();

    auto function = New<TJoinerReadingFunction>();
    function->Init(stateEnv.GetInitContext());

    auto message = MakeTestMessage("input", key, New<TTableSchema>());
    function->ProcessMessage(message, output, context);

    ASSERT_EQ(std::ssize(output->GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[0].Message, "word"), "100");
}

TEST(TProcessFunctionTest, WholeBatchFunctionSeesAllKeys)
{
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
    auto output = New<TRecordingOutputCollector>();

    auto emptySchema = New<TTableSchema>();
    // Messages of different keys are delivered together, without per-key grouping.
    std::vector<TInputMessageConstPtr> messages{
        MakeTestMessage("input", MakeKey<ui64>(1), emptySchema),
        MakeTestMessage("input", MakeKey<ui64>(2), emptySchema),
        MakeTestMessage("input", MakeKey<ui64>(1), emptySchema),
    };

    auto function = New<TWholeBatchFunction>();
    function->Process(New<TInputContext>(messages, std::vector<TInputTimerConstPtr>{}), output, context);

    ASSERT_EQ(std::ssize(output->GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[0].Message, "word"), "3");
}

TEST(TProcessFunctionTest, ElementFunctionDispatchesEachEntityKind)
{
    // WrapAsBatch yields a batch form that fans the epoch's input to the per-element hooks in
    // timer, message then visit order; each hook tags its emission so the test can assert it ran.
    class TTaggingFunction
        : public IProcessFunction
    {
    public:
        void ProcessTimer(const TInputTimerConstPtr& /*timer*/, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context) override
        {
            Emit("timer", output, context);
        }

        void ProcessMessage(const TInputMessageConstPtr& /*message*/, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context) override
        {
            Emit("message", output, context);
        }

        void ProcessVisit(const TInputVisitConstPtr& /*visit*/, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context) override
        {
            Emit("visit", output, context);
        }

    private:
        static void Emit(TStringBuf tag, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context)
        {
            auto message = New<TWordMessage>();
            message->Word = tag;
            output->AddMessage(context->ConvertToMessage(message));
        }
    };

    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
    auto output = New<TRecordingOutputCollector>();

    auto emptySchema = New<TTableSchema>();
    auto key = MakeKey<ui64>(1);
    std::vector<TInputMessageConstPtr> messages{MakeTestMessage("input", key, emptySchema)};
    std::vector<TInputTimerConstPtr> timers{MakeTestTimer(key, TSystemTimestamp(1))};
    std::vector<TInputVisitConstPtr> visits{MakeTestVisit(key, "input")};

    auto function = New<TTaggingFunction>();
    WrapAsBatch(function)->Process(New<TInputContext>(messages, timers, visits), output, context);

    ASSERT_EQ(std::ssize(output->GetMessages()), 3);
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[0].Message, "word"), "timer");
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[1].Message, "word"), "message");
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[2].Message, "word"), "visit");
}

TEST(TProcessFunctionTest, KeyFunctionSeesMessagesAndTimersTogether)
{
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
    auto output = New<TRecordingOutputCollector>();

    auto emptySchema = New<TTableSchema>();
    auto key = MakeKey<ui64>(7);
    std::vector<TInputMessageConstPtr> messages{
        MakeTestMessage("input", key, emptySchema),
        MakeTestMessage("input", key, emptySchema),
    };
    std::vector<TInputTimerConstPtr> timers{
        MakeTestTimer(key, TSystemTimestamp(5)),
    };

    auto function = New<TKeySummaryFunction>();
    function->ProcessKey(New<TInputContext>(messages, timers), output, context);

    ASSERT_EQ(std::ssize(output->GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<std::string>(output->GetMessages()[0].Message, "word"), "2:1");
}

TEST(TProcessFunctionTest, KeyedFunctionProcessGroupsByKey)
{
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
    auto output = New<TRecordingOutputCollector>();

    auto emptySchema = New<TTableSchema>();
    std::vector<TInputMessageConstPtr> messages{
        MakeTestMessage("input", MakeKey<ui64>(1), emptySchema),
        MakeTestMessage("input", MakeKey<ui64>(2), emptySchema),
        MakeTestMessage("input", MakeKey<ui64>(1), emptySchema),
    };

    // WrapAsBatch yields a batch form that groups by key and calls ProcessKey once per key:
    // key 1 (2 messages) and key 2 (1 message) yield two per-key summaries.
    auto function = New<TKeySummaryFunction>();
    WrapAsBatch(function)->Process(New<TInputContext>(messages, std::vector<TInputTimerConstPtr>{}), output, context);

    ASSERT_EQ(std::ssize(output->GetMessages()), 2);
    std::vector<std::string> summaries{
        GetColumnValue<std::string>(output->GetMessages()[0].Message, "word"),
        GetColumnValue<std::string>(output->GetMessages()[1].Message, "word"),
    };
    std::sort(summaries.begin(), summaries.end());
    EXPECT_EQ(summaries[0], "1:0");
    EXPECT_EQ(summaries[1], "2:0");
}

TEST(TProcessFunctionTest, SyncIsInvoked)
{
    class TSyncingFunction
        : public IProcessFunction
        , public ISyncProcessFunction
    {
    public:
        void Sync(const IRetryableTransactionPtr& /*transaction*/, const IRuntimeContextPtr& /*context*/) override
        {
            Synced = true;
        }

        bool Synced = false;
    };

    auto function = New<TSyncingFunction>();
    function->Sync(/*transaction*/ nullptr, TTestRuntimeContextBuilder().Build());
    EXPECT_TRUE(function->Synced);
}

TEST(TProcessFunctionTest, SyncCommitsSideEffects)
{
    // Buffers per message and, in Sync, enrolls the buffered work into the epoch's transaction —
    // the real shape of a sync-phase commit.
    class TBufferingSyncFunction
        : public IProcessFunction
        , public ISyncProcessFunction
    {
    public:
        void ProcessMessage(
            const TInputMessageConstPtr& message,
            const IOutputCollectorPtr& /*output*/,
            const IRuntimeContextPtr& /*context*/) override
        {
            Buffered_.push_back(message->Key);
        }

        void Sync(const IRetryableTransactionPtr& transaction, const IRuntimeContextPtr& /*context*/) override
        {
            if (Buffered_.empty()) {
                return;
            }
            transaction->Apply(BIND([buffered = std::move(Buffered_)] (const NApi::ITransactionPtr& /*tx*/) {
                // In production this writes |buffered| rows; the test only checks the writer was enrolled.
            }));
            Flushed = true;
        }

        bool Flushed = false;

    private:
        std::vector<TKey> Buffered_;
    };

    auto context = TTestRuntimeContextBuilder().Build();
    auto output = New<TRecordingOutputCollector>();
    auto emptySchema = New<TTableSchema>();

    auto function = New<TBufferingSyncFunction>();
    function->ProcessMessage(MakeTestMessage("input", MakeKey<ui64>(1), emptySchema), output, context);
    function->ProcessMessage(MakeTestMessage("input", MakeKey<ui64>(2), emptySchema), output, context);

    auto transaction = CreateRetryableTransaction();
    EXPECT_TRUE(transaction->IsEmpty());

    function->Sync(transaction, context);

    EXPECT_TRUE(function->Flushed);
    EXPECT_FALSE(transaction->IsEmpty());
}

TEST(TProcessFunctionTest, SyncViewResolvesMixin)
{
    // The registry recovers the sync mix-in without RTTI for a function that opted in, and yields
    // null for one that did not.
    auto syncName = std::string(TypeName<TSyncingProcessFunction>());
    auto syncFunction = TRegistry::Get()->CreateProcessFunction(syncName);
    EXPECT_TRUE(TRegistry::Get()->ViewProcessFunctionAsSync(syncName, syncFunction));

    auto plainName = std::string(TypeName<TSplitFunction>());
    auto plainFunction = TRegistry::Get()->CreateProcessFunction(plainName);
    EXPECT_FALSE(TRegistry::Get()->ViewProcessFunctionAsSync(plainName, plainFunction));
}

TEST(TProcessFunctionTest, FunctionParameters)
{
    // Static parameters: read from the init context in Init.
    TTestStateEnvironment stateEnv;
    auto staticParameters = New<TThresholdParameters>();
    staticParameters->Threshold = 5;
    stateEnv.SetStaticParameters(staticParameters);

    auto function = New<TParameterReadingFunction>();
    function->Init(stateEnv.GetInitContext());
    EXPECT_EQ(function->StaticThreshold, 5);

    // Dynamic parameters: read from the runtime context per message.
    auto dynamicParameters = New<TThresholdParameters>();
    dynamicParameters->Threshold = 9;
    auto context = TTestRuntimeContextBuilder()
        .SetDynamicParameters(dynamicParameters)
        .Build();
    auto output = New<TRecordingOutputCollector>();
    auto message = MakeTestMessage("input", MakeKey<ui64>(1), New<TTableSchema>());
    function->ProcessMessage(message, output, context);
    EXPECT_EQ(function->DynamicThreshold, 9);
}

TEST(TProcessFunctionTest, FunctionParametersDefaultWhenAbsent)
{
    // No `function_parameters` block: the struct gets its declared defaults.
    TTestStateEnvironment stateEnv;
    auto function = New<TParameterReadingFunction>();
    function->Init(stateEnv.GetInitContext());
    EXPECT_EQ(function->StaticThreshold, 0);

    auto context = TTestRuntimeContextBuilder().Build();
    auto output = New<TRecordingOutputCollector>();
    auto message = MakeTestMessage("input", MakeKey<ui64>(1), New<TTableSchema>());
    function->ProcessMessage(message, output, context);
    EXPECT_EQ(function->DynamicThreshold, 0);
}

////////////////////////////////////////////////////////////////////////////////

// Builds a one-computation pipeline spec that hosts |functionName| under a process-function
// adapter with the given static `processing_function_parameters`, and runs the spec validator.
std::vector<TError> ValidateStaticSpec(const std::string& functionName, TStringBuf parametersYson)
{
    auto specYson = Format("{computations={comp={"
        "computation_class_name=\"NYT::NFlow::TProcessFunctionComputation\";"
        "input_stream_ids=[s];"
        "processing_function=%Qv;"
        "processing_function_parameters=%v}}}",
        functionName,
        parametersYson);
    return TRegistry::Get()->ValidatePipelineSpecParseability(ConvertTo<IMapNodePtr>(TYsonString(specYson)));
}

// Builds a one-computation pipeline spec that hosts |functionName| under |computationClassName|
// (no parameters) and runs the spec validator.
std::vector<TError> ValidateStaticSpecWithHost(
    TStringBuf computationClassName,
    const std::string& functionName)
{
    auto specYson = Format("{computations={comp={"
        "computation_class_name=%Qv;"
        "input_stream_ids=[s];"
        "processing_function=%Qv}}}",
        computationClassName,
        functionName);
    return TRegistry::Get()->ValidatePipelineSpecParseability(ConvertTo<IMapNodePtr>(TYsonString(specYson)));
}

// Same, but validates the dynamic `processing_function_parameters` against the dynamic schema.
std::vector<TError> ValidateDynamicSpec(const std::string& functionName, TStringBuf dynamicParametersYson)
{
    auto staticSpecYson = Format("{computations={comp={"
        "computation_class_name=\"NYT::NFlow::TProcessFunctionComputation\";"
        "input_stream_ids=[s];"
        "processing_function=%Qv}}}",
        functionName);
    auto pipelineSpec = ConvertTo<TPipelineSpecPtr>(TYsonString(staticSpecYson));

    auto dynamicSpecYson = Format("{computations={comp={processing_function_parameters=%v}}}", dynamicParametersYson);
    return TRegistry::Get()->ValidateDynamicPipelineSpecParseability(
        pipelineSpec,
        ConvertTo<IMapNodePtr>(TYsonString(dynamicSpecYson)));
}

bool HasErrorContaining(const std::vector<TError>& errors, TStringBuf needle)
{
    for (const auto& error : errors) {
        if (ToString(error).Contains(needle)) {
            return true;
        }
    }
    return false;
}

TEST(TProcessFunctionSpecValidationTest, AcceptsValidStaticParameters)
{
    auto errors = ValidateStaticSpec(std::string(TypeName<TParameterReadingFunction>()), "{threshold=5}");
    EXPECT_FALSE(HasErrorContaining(errors, "unrecognized"));
    EXPECT_FALSE(HasErrorContaining(errors, "processing_function_parameters"));
}

TEST(TProcessFunctionSpecValidationTest, RejectsUnknownStaticField)
{
    auto errors = ValidateStaticSpec(std::string(TypeName<TParameterReadingFunction>()), "{threshold=5;bogus_field=1}");
    EXPECT_TRUE(HasErrorContaining(errors, "unrecognized"));
}

TEST(TProcessFunctionSpecValidationTest, RejectsParametersForParameterlessFunction)
{
    auto errors = ValidateStaticSpec(std::string(TypeName<TSplitFunction>()), "{anything=1}");
    EXPECT_TRUE(HasErrorContaining(errors, "unrecognized"));
}

TEST(TProcessFunctionSpecValidationTest, RejectsUnknownProcessingFunction)
{
    auto errors = ValidateStaticSpec("NoSuchProcessingFunction", "{}");
    EXPECT_TRUE(HasErrorContaining(errors, "Unknown processing function"));
}

TEST(TProcessFunctionSpecValidationTest, RejectsMissingProcessingFunction)
{
    // The adapter requires a processing_function; a spec that omits it fails at parse.
    auto specNode = ConvertTo<IMapNodePtr>(TYsonString(TStringBuf(
        "{computations={comp={computation_class_name=\"NYT::NFlow::TProcessFunctionComputation\";input_stream_ids=[s]}}}")));
    auto errors = TRegistry::Get()->ValidatePipelineSpecParseability(specNode);
    EXPECT_TRUE(HasErrorContaining(errors, "processing_function"));
}

TEST(TProcessFunctionSpecValidationTest, RejectsSyncFunctionOnNonSyncHost)
{
    // A sync function on a host that runs no sync phase is rejected; its side effects would be lost.
    auto errors = ValidateStaticSpecWithHost(
        "NYT::NFlow::TProcessFunctionSwiftMapComputation",
        std::string(TypeName<TSyncingProcessFunction>()));
    EXPECT_TRUE(HasErrorContaining(errors, "requires a sync phase"));
}

TEST(TProcessFunctionSpecValidationTest, AcceptsSyncFunctionOnSyncHost)
{
    // The host runs a sync phase, so a sync function is allowed.
    auto errors = ValidateStaticSpecWithHost(
        "NYT::NFlow::TProcessFunctionComputation",
        std::string(TypeName<TSyncingProcessFunction>()));
    EXPECT_FALSE(HasErrorContaining(errors, "requires a sync phase"));
}

TEST(TProcessFunctionSpecValidationTest, AcceptsNonSyncFunctionOnNonSyncHost)
{
    // A function with no sync phase is fine on a host that runs none.
    auto errors = ValidateStaticSpecWithHost(
        "NYT::NFlow::TProcessFunctionSwiftMapComputation",
        std::string(TypeName<TSplitFunction>()));
    EXPECT_FALSE(HasErrorContaining(errors, "requires a sync phase"));
}

TEST(TProcessFunctionSpecValidationTest, AcceptsValidDynamicParameters)
{
    auto errors = ValidateDynamicSpec(std::string(TypeName<TParameterReadingFunction>()), "{threshold=9}");
    EXPECT_FALSE(HasErrorContaining(errors, "unrecognized"));
}

TEST(TProcessFunctionSpecValidationTest, RejectsUnknownDynamicField)
{
    auto errors = ValidateDynamicSpec(std::string(TypeName<TParameterReadingFunction>()), "{bogus_field=1}");
    EXPECT_TRUE(HasErrorContaining(errors, "unrecognized"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
