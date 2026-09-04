#include "yql_ytflow_runtime_node_test_utils.h"

#include <yt/yql/providers/ytflow/common/yql_ytflow_environment.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_computation_graph_with_codecs_base.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_computation_pattern.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_computation_pattern_resource.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_function_registry.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_map_computation_graph_with_codecs.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_postprocess_computation_graph_with_codecs.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_update_state_computation_graph_with_codecs.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <library/cpp/testing/common/scope.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/core/profiling/public.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <util/system/tempfile.h>

#include <optional>

namespace NYql::NYtflow {
namespace {

constexpr TStringBuf TestResult =
    "YtFlow computation pattern shared literal that requires reference locking";
constexpr TStringBuf LookupTokenName = "lookup_token";

void WriteTestLambda(TTempFileHandle& file)
{
    // A non-embedded string exercises cross-allocator reference locking, while
    // the non-literal node gives every clone observable mutable storage.
    NTest::WriteConditionalStringLambda(file, TestResult);
}

void WriteLookupJoinLambda(TTempFileHandle& file)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [](TProgramBuilder& programBuilder) {
        auto* uint64Type = programBuilder.NewDataType(NUdf::EDataSlot::Uint64);
        auto* streamRowType = programBuilder.NewStructType({
            {"key", uint64Type},
            {"stream_value", uint64Type},
        });
        auto* lookupRowType = programBuilder.NewStructType({
            {"key", uint64Type},
            {"lookup_value", uint64Type},
        });
        auto* outputRowType = programBuilder.NewStructType({
            {"l.lookup_value", uint64Type},
            {"s.key", uint64Type},
            {"s.stream_value", uint64Type},
        });

        auto stream = programBuilder.Iterator(
            programBuilder.NewEmptyList(streamRowType),
            {});
        auto wrappedLookupSourceType = programBuilder.Nop(
            programBuilder.NewVoid(),
            programBuilder.NewListType(lookupRowType));
        auto lookupSourceArgs = programBuilder.NewTuple({
            programBuilder.NewDataLiteral<NUdf::EDataSlot::String>("yt"),
            programBuilder.NewTuple({
                programBuilder.NewDataLiteral<NUdf::EDataSlot::String>("localhost"),
                programBuilder.NewDataLiteral<NUdf::EDataSlot::String>("//test/table"),
                programBuilder.NewDataLiteral<NUdf::EDataSlot::String>(LookupTokenName),
            }),
        });
        auto joinKind = programBuilder.NewDataLiteral<ui32>(
            static_cast<ui32>(EJoinKind::Inner));

        auto buildScope = [&](TStringBuf label, TStringBuf side) {
            return programBuilder.NewTuple({
                programBuilder.NewDataLiteral<NUdf::EDataSlot::String>(label),
                programBuilder.NewDataLiteral<NUdf::EDataSlot::String>(side),
                programBuilder.NewTuple({
                    programBuilder.NewDataLiteral<NUdf::EDataSlot::String>("key"),
                }),
                programBuilder.NewDataLiteral<ui32>(
                    static_cast<ui32>(ERowSelectionMode::Any)),
            });
        };

        auto inflightRowLimit = programBuilder.NewDataLiteral<ui64>(1);
        auto inflightLookupLimit = programBuilder.NewDataLiteral<ui64>(1);
        auto lookupTimeoutMs = programBuilder.NewDataLiteral<ui64>(1000);
        auto settings = programBuilder.NewTuple({
            inflightRowLimit,
            inflightLookupLimit,
            lookupTimeoutMs,
        });

        TCallableBuilder callableBuilder(
            programBuilder.GetTypeEnvironment(),
            "YtflowLookupJoin",
            programBuilder.NewStreamType(outputRowType));
        callableBuilder.Add(stream);
        callableBuilder.Add(wrappedLookupSourceType);
        callableBuilder.Add(lookupSourceArgs);
        callableBuilder.Add(joinKind);
        callableBuilder.Add(buildScope("s", "left"));
        callableBuilder.Add(buildScope("l", "right"));
        callableBuilder.Add(settings);
        return TRuntimeNode(callableBuilder.Build(), /*isImmediate*/ false);
    });
}

void WriteFileCallable(
    TTempFileHandle& file,
    TStringBuf callableName,
    TStringBuf fileName)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [callableName, fileName](TProgramBuilder& programBuilder) {
        TCallableBuilder callableBuilder(
            programBuilder.GetTypeEnvironment(),
            callableName,
            programBuilder.NewDataType(NUdf::TDataType<char*>::Id));
        callableBuilder.Add(
            programBuilder.NewDataLiteral<NUdf::EDataSlot::String>(fileName));
        return TRuntimeNode(callableBuilder.Build(), /*isImmediate=*/false);
    });
}

void WriteYtflowInputCallable(
    TTempFileHandle& file,
    TStringBuf callableName,
    bool stream)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [callableName, stream](TProgramBuilder& programBuilder) {
        auto* itemType = programBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
        TCallableBuilder callableBuilder(
            programBuilder.GetTypeEnvironment(),
            callableName,
            stream ? programBuilder.NewStreamType(itemType) : itemType);
        return TRuntimeNode(callableBuilder.Build(), /*isImmediate=*/false);
    });
}

void WriteChunkedForwardListCallable(TTempFileHandle& file)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [](TProgramBuilder& programBuilder) {
        const auto& env = programBuilder.GetTypeEnvironment();
        auto* itemType = programBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* inputType = programBuilder.NewStreamType(itemType);
        TCallableBuilder inputBuilder(env, "YtflowInputStream", inputType);
        auto input = TRuntimeNode(inputBuilder.Build(), /*isImmediate=*/false);
        TCallableBuilder chunkBuilder(
            env,
            "YtflowChunkedForwardList",
            programBuilder.NewStreamType(programBuilder.NewListType(itemType)));
        chunkBuilder.Add(input);
        return TRuntimeNode(chunkBuilder.Build(), /*isImmediate=*/false);
    });
}

void WriteMapLambda(TTempFileHandle& file)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [](TProgramBuilder& programBuilder) {
        auto* itemType = programBuilder.NewStructType({
            {"value", programBuilder.NewDataType(NUdf::TDataType<ui64>::Id)},
        });
        TCallableBuilder inputBuilder(
            programBuilder.GetTypeEnvironment(),
            "YtflowInputStream",
            programBuilder.NewStreamType(itemType));
        return TRuntimeNode(inputBuilder.Build(), /*isImmediate=*/false);
    });
}

void WriteUpdateStateLambda(TTempFileHandle& file)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [](TProgramBuilder& programBuilder) {
        const auto& env = programBuilder.GetTypeEnvironment();
        auto* itemType = programBuilder.NewStructType({
            {"value", programBuilder.NewDataType(NUdf::TDataType<ui64>::Id)},
        });
        auto* stateType = programBuilder.NewListType(
            programBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
        auto* timerInfoType = programBuilder.NewTupleType({
            programBuilder.NewDataType(NUdf::TDataType<ui64>::Id),
            programBuilder.NewDataType(NUdf::TDataType<ui64>::Id),
        });

        TCallableBuilder streamBuilder(
            env,
            "YtflowInputStream",
            programBuilder.NewStreamType(itemType));
        TCallableBuilder stateBuilder(env, "YtflowInputState", stateType);
        auto stream = TRuntimeNode(streamBuilder.Build(), /*isImmediate*/ false);
        auto state = TRuntimeNode(stateBuilder.Build(), /*isImmediate*/ false);
        auto output = programBuilder.NewTuple({
            state,
            programBuilder.NewEmptyList(timerInfoType),
        });
        return programBuilder.Seq({stream, output}, output.GetStaticType());
    });
}

void WritePostprocessLambda(TTempFileHandle& file)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [](TProgramBuilder& programBuilder) {
        const auto& env = programBuilder.GetTypeEnvironment();
        auto* dataType = programBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* stateType = programBuilder.NewListType(dataType);
        auto* itemType = programBuilder.NewStructType({{"value", dataType}});

        TCallableBuilder keyBuilder(env, "YtflowInputKey", dataType);
        TCallableBuilder stateBuilder(env, "YtflowInputState", stateType);
        TCallableBuilder maxHopStartTimeBuilder(
            env,
            "YtflowInputMaxHopStartTime",
            dataType);
        auto key = TRuntimeNode(keyBuilder.Build(), /*isImmediate*/ false);
        auto state = TRuntimeNode(stateBuilder.Build(), /*isImmediate*/ false);
        auto maxHopStartTime = TRuntimeNode(
            maxHopStartTimeBuilder.Build(),
            /*isImmediate*/ false);
        auto messages = programBuilder.Iterator(
            programBuilder.NewEmptyList(itemType),
            {});
        auto output = programBuilder.NewTuple({
            messages,
            state,
            programBuilder.NewDataLiteral<bool>(false),
        });
        return programBuilder.Seq(
            {key, maxHopStartTime, output},
            output.GetStaticType());
    });
}

NYT::NTableClient::TTableSchemaPtr MakeMapSchema()
{
    NYT::NTableClient::TColumnSchema valueColumn(
        "value",
        NYT::NTableClient::EValueType::Uint64);
    valueColumn.SetRequired(true);
    return NYT::New<NYT::NTableClient::TTableSchema>(
        std::vector<NYT::NTableClient::TColumnSchema>{
            std::move(valueColumn),
        });
}

TMessageHolder MakeMapMessage(
    const NYT::NTableClient::TTableSchemaPtr& schema,
    ui64 value)
{
    NYT::NTableClient::TUnversionedRowBuilder rowBuilder(1);
    rowBuilder.AddValue(NYT::NTableClient::MakeUnversionedUint64Value(value));

    auto message = MakeHolder<NYT::NFlow::TMessage>();
    message->Payload = NYT::NFlow::TPayload(
        NYT::NFlow::TCompactUnversionedOwningRow(rowBuilder.GetRow()));
    message->PayloadSchema = schema;
    return TMessageHolder(std::move(message));
}

class TMapComputationPatternTestBuilder {
public:
    TMapComputationPatternTestBuilder()
        : Schema(MakeMapSchema())
    {
        WriteMapLambda(LambdaFile);
    }

    TIntrusivePtr<TComputationPatternHolder> BuildPattern(
        TIntrusivePtr<TFunctionRegistryHolder> registryHolder) const
    {
        return BuildComputationPatternHolder(
            LambdaFile.GetName(),
            std::move(registryHolder),
            UnknownLangVersion,
            "OFF",
            MakeRuntimeSettings());
    }

    THolder<IMapComputationGraphWithCodecs> BuildGraph(
        EInputMode inputMode,
        TComputationGraphResources resources = {}) const
    {
        THashMap<ui32, TVector<TOutputStreamInfo>> outputs;
        outputs[0].push_back(TOutputStreamInfo{
            .StreamId = NYT::NFlow::TStreamId("output"),
            .OutputSchema = Schema,
        });
        return CreateMapComputationGraphWithCodecs(
            LambdaFile.GetName(),
            Schema,
            std::move(outputs),
            /*udfPaths*/ {},
            inputMode,
            UnknownLangVersion,
            "OFF",
            MakeRuntimeSettings(),
            /*injectInputMessageId*/ false,
            /*profiler*/ {},
            /*converterCache*/ nullptr,
            std::move(resources));
    }

    TMessageHolder MakeMessage(ui64 value) const
    {
        return MakeMapMessage(Schema, value);
    }

    TVector<ui64> Execute(
        IMapComputationGraphWithCodecs& graph,
        const TMessageHolder& message) const
    {
        graph.SetInput(message);
        return DrainAndReset(graph);
    }

    TVector<ui64> Execute(
        IMapComputationGraphWithCodecs& graph,
        const std::vector<TMessageHolder>& messages) const
    {
        graph.SetInput(messages);
        return DrainAndReset(graph);
    }

    TVector<ui64> DrainAndReset(IMapComputationGraphWithCodecs& graph) const
    {
        TVector<NYT::NFlow::TMessage> outputMessages;
        TVector<ui64> values;
        while (graph.FetchOutput(outputMessages)) {
            for (const auto& message : outputMessages) {
                const NYT::NFlow::TStreamId expectedStreamId("output");
                EXPECT_EQ(expectedStreamId, message.StreamId);
                if (message.StreamId != expectedStreamId) {
                    continue;
                }
                EXPECT_EQ(Schema.Get(), message.PayloadSchema.Get());
                if (message.PayloadSchema.Get() != Schema.Get()) {
                    continue;
                }
                const auto valueCount = message.Payload.Underlying().GetCount();
                EXPECT_EQ(1, valueCount);
                if (valueCount != 1) {
                    continue;
                }
                values.push_back(message.Payload.Underlying()[0].Data.Uint64);
            }
            outputMessages.clear();
        }
        graph.ResetInput();
        return values;
    }

private:
    TTempFileHandle LambdaFile;
    NYT::NTableClient::TTableSchemaPtr Schema;
};

class THoppingComputationPatternTestBuilder {
public:
    THoppingComputationPatternTestBuilder()
        : Schema(MakeMapSchema())
    {
        WriteUpdateStateLambda(UpdateStateLambdaFile);
        WritePostprocessLambda(PostprocessLambdaFile);
    }

    TIntrusivePtr<TComputationPatternHolder> BuildUpdateStatePattern(
        TIntrusivePtr<TFunctionRegistryHolder> registryHolder) const
    {
        return BuildComputationPatternHolder(
            UpdateStateLambdaFile.GetName(),
            std::move(registryHolder),
            UnknownLangVersion,
            "OFF",
            MakeRuntimeSettings());
    }

    TIntrusivePtr<TComputationPatternHolder> BuildPostprocessPattern(
        TIntrusivePtr<TFunctionRegistryHolder> registryHolder) const
    {
        return BuildComputationPatternHolder(
            PostprocessLambdaFile.GetName(),
            std::move(registryHolder),
            UnknownLangVersion,
            "OFF",
            MakeRuntimeSettings());
    }

    THolder<IUpdateStateComputationGraphWithCodecs> BuildUpdateStateGraph(
        TComputationGraphResources resources = {},
        std::optional<TString> lambdaFile = std::nullopt,
        TVector<TString> udfPaths = {}) const
    {
        return CreateUpdateStateComputationGraphWithCodecs(
            lambdaFile
                ? std::move(*lambdaFile)
                : TString(UpdateStateLambdaFile.GetName()),
            std::move(udfPaths),
            UnknownLangVersion,
            "OFF",
            MakeRuntimeSettings(),
            Schema,
            /*profiler*/ {},
            /*converterCache*/ nullptr,
            std::move(resources));
    }

    THolder<IPostprocessComputationGraphWithCodecs> BuildPostprocessGraph(
        TComputationGraphResources resources = {},
        std::optional<TString> lambdaFile = std::nullopt,
        TVector<TString> udfPaths = {}) const
    {
        return CreatePostprocessComputationGraphWithCodecs(
            lambdaFile
                ? std::move(*lambdaFile)
                : TString(PostprocessLambdaFile.GetName()),
            {
                TOutputStreamInfo{
                    .StreamId = NYT::NFlow::TStreamId("output"),
                    .OutputSchema = Schema,
                },
            },
            std::move(udfPaths),
            UnknownLangVersion,
            "OFF",
            MakeRuntimeSettings(),
            /*profiler*/ {},
            /*converterCache*/ nullptr,
            std::move(resources));
    }

    void Execute(
        IUpdateStateComputationGraphWithCodecs& updateStateGraph,
        IPostprocessComputationGraphWithCodecs& postprocessGraph) const
    {
        updateStateGraph.SetInput({}, std::nullopt);
        auto updateStateOutput = updateStateGraph.GetOutput();
        updateStateGraph.ResetInput();

        ASSERT_TRUE(updateStateOutput.TimerInfos.empty());
        ASSERT_FALSE(updateStateOutput.State.empty());

        postprocessGraph.SetInput(
            NYT::NFlow::MakeKey(ui64{0}, ui64{7}),
            updateStateOutput.State,
            /*maxHopStartTime*/ 0);
        auto postprocessOutput = postprocessGraph.GetOutput();
        postprocessGraph.ResetInput();

        ASSERT_TRUE(postprocessOutput.Messages.empty());
        ASSERT_EQ(updateStateOutput.State, postprocessOutput.State);
        ASSERT_FALSE(postprocessOutput.CleanupState);
    }

private:
    TTempFileHandle UpdateStateLambdaFile;
    TTempFileHandle PostprocessLambdaFile;
    NYT::NTableClient::TTableSchemaPtr Schema;
};

class TTwoItemYieldStream
    : public NKikimr::NMiniKQL::TComputationValue<TTwoItemYieldStream> {
public:
    TTwoItemYieldStream(
        NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
        ui64 first,
        ui64 second)
        : TComputationValue(memInfo)
        , First(first)
        , Second(second)
    {
    }

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        switch (Position++) {
            case 0:
                result = NUdf::TUnboxedValuePod(First);
                return NUdf::EFetchStatus::Ok;
            case 1:
                return NUdf::EFetchStatus::Yield;
            case 2:
                result = NUdf::TUnboxedValuePod(Second);
                return NUdf::EFetchStatus::Ok;
            default:
                return NUdf::EFetchStatus::Finish;
        }
    }

private:
    const ui64 First;
    const ui64 Second;
    ui32 Position = 0;
};

bool BuildCorePatternAndGetSuitability()
{
    using namespace NKikimr::NMiniKQL;

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
    TProgramBuilder programBuilder(env, *registry);
    TCallableBuilder callableBuilder(
        env,
        "MultiHoppingCore",
        programBuilder.NewDataType(NUdf::TDataType<bool>::Id));
    auto root = TRuntimeNode(callableBuilder.Build(), /*isImmediate=*/false);

    TExploringNodeVisitor explorer;
    explorer.Walk(root.GetNode(), env.GetNodeStack());
    auto nodeFactory = GetCompositeWithBuiltinFactory({
        [](TCallable& callable, const TComputationNodeFactoryContext& ctx) {
            if (callable.GetType()->GetName() == "MultiHoppingCore") {
                return static_cast<IComputationNode*>(
                    new TExternalComputationNode(ctx.Mutables));
            }
            return static_cast<IComputationNode*>(nullptr);
        },
    });
    auto runtimeSettings = MakeRuntimeSettings();
    TComputationPatternOpts patternOpts(
        alloc.Ref(),
        env,
        std::move(nodeFactory),
        registry.Get(),
        NUdf::EValidateMode::None,
        NUdf::EValidatePolicy::Fail,
        /*optLLVM*/ "",
        EGraphPerProcess::Multi,
        /*stats*/ nullptr,
        /*countersProvider*/ nullptr,
        /*secureParamsProvider*/ nullptr,
        /*logProvider*/ nullptr,
        UnknownLangVersion,
        runtimeSettings);
    auto pattern = MakeComputationPattern(
        explorer,
        root,
        {root.GetNode()},
        patternOpts);
    return pattern->GetSuitableForCache();
}

TIntrusivePtr<TComputationPatternHolder> BuildTestPattern(const TString& lambdaFile)
{
    return BuildComputationPatternHolder(
        lambdaFile,
        CreateFunctionRegistryHolder({}),
        UnknownLangVersion,
        "OFF",
        MakeRuntimeSettings());
}

TIntrusivePtr<TComputationPatternHolder> BuildFilePattern(
    TStringBuf callableName,
    TStringBuf fileName)
{
    TTempFileHandle lambdaFile;
    WriteFileCallable(lambdaFile, callableName, fileName);
    return BuildComputationPatternHolder(
        lambdaFile.GetName(),
        CreateFunctionRegistryHolder({}),
        UnknownLangVersion,
        "OFF",
        MakeRuntimeSettings());
}

class TCloneState {
public:
    explicit TCloneState(TIntrusivePtr<TComputationPatternHolder> patternHolder)
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , RandomProvider(CreateDefaultRandomProvider())
        , TimeProvider(CreateDefaultTimeProvider())
        , Graph(CloneComputationGraph(
              std::move(patternHolder),
              Alloc,
              TypeEnv,
              *RandomProvider,
              *TimeProvider))
    {
        Graph->Prepare();
        Alloc.Release();
    }

    ~TCloneState()
    {
        DestroyGraph();
        // TypeEnv and the remaining allocator-owned state are destroyed after
        // the destructor body and require this allocator to be active.
        Alloc.Acquire();
    }

    void DestroyGraph()
    {
        if (Graph) {
            Alloc.Acquire();
            ChunkIterator = {};
            CurrentChunk = {};
            Output = {};
            Graph.Reset();
            // Leave the allocator unbound so another clone can be used or
            // destroyed before this fixture itself is destroyed.
            Alloc.Release();
        }
    }

    TString GetResult()
    {
        auto guard = Guard(Alloc);
        const auto result = Graph->GetValue();
        const auto value = result.AsStringRef();
        return TString(value.Data(), value.Size());
    }

    ui64 GetUi64Result()
    {
        auto guard = Guard(Alloc);
        return Graph->GetValue().Get<ui64>();
    }

    void SetExternalUi64(
        const NKikimr::NMiniKQL::IComputationExternalNode& node,
        ui64 value)
    {
        auto guard = Guard(Alloc);
        node.SetValue(GetContext(), NUdf::TUnboxedValuePod(value));
    }

    void SetExternalStream(
        const NKikimr::NMiniKQL::IComputationExternalNode& node,
        ui64 first,
        ui64 second)
    {
        auto guard = Guard(Alloc);
        node.SetValue(
            GetContext(),
            GetContext().HolderFactory.Create<TTwoItemYieldStream>(first, second));
    }

    NUdf::EFetchStatus FetchOutput(ui64* value = nullptr)
    {
        auto guard = Guard(Alloc);
        if (!Output) {
            Output = Graph->GetValue();
        }
        NUdf::TUnboxedValue item;
        const auto status = Output.Fetch(item);
        if (status == NUdf::EFetchStatus::Ok && value) {
            *value = item.Get<ui64>();
        }
        return status;
    }

    const void* GetOutputIdentity()
    {
        auto guard = Guard(Alloc);
        if (!Output) {
            Output = Graph->GetValue();
        }
        return Output.AsBoxed().Get();
    }

    NUdf::EFetchStatus FetchChunk()
    {
        auto guard = Guard(Alloc);
        if (!Output) {
            Output = Graph->GetValue();
        }
        NUdf::TUnboxedValue list;
        const auto status = Output.Fetch(list);
        if (status == NUdf::EFetchStatus::Ok) {
            CurrentChunk = std::move(list);
            ChunkIterator = CurrentChunk.GetListIterator();
        }
        return status;
    }

    void RestartCurrentChunk()
    {
        auto guard = Guard(Alloc);
        ChunkIterator = CurrentChunk.GetListIterator();
    }

    bool NextChunkItem(ui64& value)
    {
        auto guard = Guard(Alloc);
        NUdf::TUnboxedValue item;
        if (!ChunkIterator.Next(item)) {
            return false;
        }
        value = item.Get<ui64>();
        return true;
    }

    NKikimr::NMiniKQL::TComputationContext& GetContext()
    {
        return Graph->GetContext();
    }

public:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;

private:
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    THolder<NKikimr::NMiniKQL::IComputationGraph> Graph;
    NUdf::TUnboxedValue Output;
    NUdf::TUnboxedValue CurrentChunk;
    NUdf::TUnboxedValue ChunkIterator;
};

class TLegacyComputationGraph
    : public TComputationGraphWithCodecsBase {
public:
    explicit TLegacyComputationGraph(const TString& lambdaFile)
        : TComputationGraphWithCodecsBase(
              lambdaFile,
              /*udfPaths*/ {},
              UnknownLangVersion,
              "OFF",
              MakeRuntimeSettings())
    {
    }

    TString GetResult()
    {
        auto guard = Guard(Alloc);
        const auto result = ComputationGraph->GetValue();
        const auto value = result.AsStringRef();
        return TString(value.Data(), value.Size());
    }
};

TString ReadSecureParamFromBuilder(
    const NKikimr::NMiniKQL::TComputationContext& context,
    TStringBuf key)
{
    NUdf::TStringRef value;
    if (!context.Builder) {
        ADD_FAILURE() << "Computation context has no value builder";
        return {};
    }
    if (!context.Builder->GetSecureParam(
            NUdf::TStringRef(key.data(), key.size()),
            value))
    {
        ADD_FAILURE() << "Secure parameter is missing: " << TString(key);
        return {};
    }
    return TString(value.Data(), value.Size());
}

void AssertPatternCallables(
    const TComputationPatternHolder& holder,
    std::initializer_list<TStringBuf> callableNames)
{
    const auto& actual = holder.GetYtflowPatternMetadata().SpecializedCallables;
    ASSERT_EQ(callableNames.size(), actual.size());
    bool allShareable = true;
    for (const auto name : callableNames) {
        const auto iterator = actual.find(TString(name));
        ASSERT_NE(actual.end(), iterator);
        allShareable &=
            iterator->second == EYtflowCallablePatternSharing::Shareable;
    }
    ASSERT_EQ(allShareable, holder.GetSuitableForCache());
    if (!allShareable) {
        ASSERT_TRUE(holder.GetUnsuitabilityReason());
        ASSERT_EQ(
            EComputationPatternUnsuitabilityReason::YtflowCallableDenied,
            *holder.GetUnsuitabilityReason());
    }
}

void AssertSuitableAndCloneable(
    const TIntrusivePtr<TComputationPatternHolder>& holder,
    std::initializer_list<TStringBuf> callableNames)
{
    ASSERT_NO_FATAL_FAILURE(AssertPatternCallables(*holder, callableNames));
    ASSERT_TRUE(holder->GetSuitableForCache());
    ASSERT_FALSE(holder->GetUnsuitabilityReason());
    TCloneState first(holder);
    TCloneState second(holder);
    ASSERT_NE(&first.GetContext(), &second.GetContext());
}

void CheckScalarExternalNodeIsGraphLocal(TStringBuf callableName)
{
    TTempFileHandle lambdaFile;
    WriteYtflowInputCallable(lambdaFile, callableName, /*stream*/ false);
    auto patternHolder = BuildTestPattern(lambdaFile.GetName());
    ASSERT_NO_FATAL_FAILURE(AssertPatternCallables(*patternHolder, {callableName}));
    const auto* inputNode = patternHolder->GetYtflowInputNodes().at(TString(callableName));
    TCloneState first(patternHolder);
    TCloneState second(patternHolder);

    ASSERT_NE(&first.GetContext(), &second.GetContext());
    ASSERT_NE(
        first.GetContext().MutableValues.get(),
        second.GetContext().MutableValues.get());
    first.SetExternalUi64(*inputNode, 17);
    second.SetExternalUi64(*inputNode, 29);
    ASSERT_EQ(29, second.GetUi64Result());
    ASSERT_EQ(17, first.GetUi64Result());
    first.SetExternalUi64(*inputNode, 41);
    ASSERT_EQ(41, first.GetUi64Result());
    ASSERT_EQ(29, second.GetUi64Result());
}

TEST(TComputationPatternLifetimeTest, TwoClonesHaveIndependentExecutionStateAndMatchLegacyFacade)
{
    TTempFileHandle lambdaFile;
    WriteTestLambda(lambdaFile);

    auto patternHolder = BuildTestPattern(lambdaFile.GetName());
    TCloneState first(patternHolder);
    TCloneState second(patternHolder);
    TLegacyComputationGraph legacy(lambdaFile.GetName());

    ASSERT_NE(&first.Alloc.Ref(), &second.Alloc.Ref());
    ASSERT_NE(&first.TypeEnv, &second.TypeEnv);
    ASSERT_NE(&first.GetContext(), &second.GetContext());
    ASSERT_NE(
        &first.GetContext().HolderFactory,
        &second.GetContext().HolderFactory);
    ASSERT_EQ(
        static_cast<NKikimr::TAlignedPagePool*>(&first.Alloc.Ref()),
        &first.GetContext().HolderFactory.GetPagePool());
    ASSERT_EQ(
        static_cast<NKikimr::TAlignedPagePool*>(&second.Alloc.Ref()),
        &second.GetContext().HolderFactory.GetPagePool());
    ASSERT_NE(first.GetContext().Builder, second.GetContext().Builder);
    ASSERT_NE(
        &first.GetContext().RandomProvider,
        &second.GetContext().RandomProvider);
    ASSERT_NE(
        &first.GetContext().TimeProvider,
        &second.GetContext().TimeProvider);
    ASSERT_GT(first.GetContext().Mutables.CurValueIndex, 0);
    ASSERT_GT(second.GetContext().Mutables.CurValueIndex, 0);
    ASSERT_NE(
        first.GetContext().MutableValues.get(),
        second.GetContext().MutableValues.get());
    ASSERT_EQ(&first.TypeEnv, &first.GetContext().TypeEnv);
    ASSERT_EQ(&second.TypeEnv, &second.GetContext().TypeEnv);

    ASSERT_EQ(TestResult, first.GetResult());
    ASSERT_EQ(TestResult, second.GetResult());
    ASSERT_EQ(legacy.GetResult(), first.GetResult());
}

TEST(TComputationPatternLifetimeTest, GraphProxyRetainsPatternHolderAndGraphsCanBeDestroyedFirstToLast)
{
    TTempFileHandle lambdaFile;
    WriteTestLambda(lambdaFile);

    auto patternHolder = BuildTestPattern(lambdaFile.GetName());
    TCloneState first(patternHolder);
    TCloneState second(patternHolder);
    patternHolder.Reset();

    ASSERT_EQ(TestResult, first.GetResult());
    ASSERT_EQ(TestResult, second.GetResult());
    first.DestroyGraph();
    ASSERT_EQ(TestResult, second.GetResult());
    second.DestroyGraph();
}

TEST(TComputationPatternLifetimeTest, GraphProxyRetainsPatternHolderAndGraphsCanBeDestroyedLastToFirst)
{
    TTempFileHandle lambdaFile;
    WriteTestLambda(lambdaFile);

    auto patternHolder = BuildTestPattern(lambdaFile.GetName());
    TCloneState first(patternHolder);
    TCloneState second(patternHolder);
    patternHolder.Reset();

    ASSERT_EQ(TestResult, second.GetResult());
    ASSERT_EQ(TestResult, first.GetResult());
    second.DestroyGraph();
    ASSERT_EQ(TestResult, first.GetResult());
    first.DestroyGraph();
}

TEST(TComputationPatternLifetimeTest, GraphsCanBeDestroyedBeforeExternalPatternHolder)
{
    TTempFileHandle lambdaFile;
    WriteTestLambda(lambdaFile);

    auto patternHolder = BuildTestPattern(lambdaFile.GetName());
    const auto referencesWithoutGraphs = patternHolder->RefCount();
    TCloneState first(patternHolder);
    const auto referencesWithFirstGraph = patternHolder->RefCount();
    ASSERT_GT(referencesWithFirstGraph, referencesWithoutGraphs);
    TCloneState second(patternHolder);
    const auto referencesWithBothGraphs = patternHolder->RefCount();
    ASSERT_GT(referencesWithBothGraphs, referencesWithFirstGraph);

    first.DestroyGraph();
    const auto referencesAfterFirstGraph = patternHolder->RefCount();
    ASSERT_LT(referencesAfterFirstGraph, referencesWithBothGraphs);
    second.DestroyGraph();
    ASSERT_EQ(referencesWithoutGraphs, patternHolder->RefCount());
    patternHolder.Reset();
}

TEST(TSecureParamsSnapshotTest, PatternAndItsLaterClonesKeepSnapshotWhileNewPatternReadsNewGeneration)
{
    TTempFileHandle lambdaFile;
    WriteTestLambda(lambdaFile);

    const TString secretA = "stage6-secret-value-A";
    const TString secretB = "stage6-secret-value-B";

    NTesting::TScopedEnvironment environmentA(
        TString(SecureParamsEnvironmentVariable),
        R"({"secret"="stage6-secret-value-A";})");
    auto firstGeneration = BuildTestPattern(lambdaFile.GetName());

    {
        // Production keeps this environment stable within one resource-manager
        // generation; changing it here models construction of a new generation.
        NTesting::TScopedEnvironment environmentB(
            TString(SecureParamsEnvironmentVariable),
            R"({"secret"="stage6-secret-value-B";})");
        TCloneState firstClone(firstGeneration);
        TCloneState laterClone(firstGeneration);
        auto secondGeneration = BuildTestPattern(lambdaFile.GetName());
        TCloneState newGenerationClone(secondGeneration);
        firstGeneration.Reset();
        secondGeneration.Reset();

        ASSERT_EQ(secretA, ReadSecureParamFromBuilder(firstClone.GetContext(), "secret"));
        ASSERT_EQ(secretA, ReadSecureParamFromBuilder(laterClone.GetContext(), "secret"));
        ASSERT_EQ(secretB, ReadSecureParamFromBuilder(newGenerationClone.GetContext(), "secret"));
    }
}

TEST(TPatternLocalFilesTest, ContentSnapshotsGenerations)
{
    constexpr TStringBuf contentA = "local-file-content-A";
    constexpr TStringBuf contentB = "local-file-content-B";
    TIntrusivePtr<TComputationPatternHolder> firstGeneration;
    TIntrusivePtr<TComputationPatternHolder> secondGeneration;
    THolder<TCloneState> firstClone;
    THolder<TCloneState> secondClone;

    {
        TTempFileHandle contentFile;
        contentFile.Write(contentA.data(), contentA.size());
        contentFile.Flush();
        firstGeneration =
            BuildFilePattern("FileContent", contentFile.GetName());
        ASSERT_NO_FATAL_FAILURE(
            AssertPatternCallables(*firstGeneration, {"FileContent"}));

        contentFile.Resize(0);
        contentFile.Seek(0, sSet);
        contentFile.Write(contentB.data(), contentB.size());
        contentFile.Flush();
        firstClone = MakeHolder<TCloneState>(firstGeneration);
        secondClone = MakeHolder<TCloneState>(firstGeneration);
        ASSERT_EQ(contentA, firstClone->GetResult());
        ASSERT_EQ(contentA, secondClone->GetResult());

        secondGeneration =
            BuildFilePattern("FileContent", contentFile.GetName());
        ASSERT_NO_FATAL_FAILURE(
            AssertPatternCallables(*secondGeneration, {"FileContent"}));
    }

    ASSERT_EQ(contentA, firstClone->GetResult());
    ASSERT_EQ(contentA, secondClone->GetResult());
    TCloneState laterFirstGenerationClone(firstGeneration);
    TCloneState laterSecondGenerationClone(secondGeneration);
    ASSERT_EQ(contentA, laterFirstGenerationClone.GetResult());
    ASSERT_EQ(contentB, laterSecondGenerationClone.GetResult());
}

TEST(TPatternLocalFilesTest, PathOwnsString)
{
    TIntrusivePtr<TComputationPatternHolder> patternHolder;
    TString expectedPath;
    {
        TTempFileHandle contentFile;
        expectedPath = contentFile.GetName();
        patternHolder = BuildFilePattern("FilePath", expectedPath);
        ASSERT_NO_FATAL_FAILURE(
            AssertPatternCallables(*patternHolder, {"FilePath"}));
    }

    TCloneState first(patternHolder);
    TCloneState second(patternHolder);
    ASSERT_EQ(expectedPath, first.GetResult());
    ASSERT_EQ(expectedPath, second.GetResult());
}

TEST(TYtflowShareableNodeTest, InputStreamIsGraphLocal)
{
    TTempFileHandle lambdaFile;
    WriteYtflowInputCallable(lambdaFile, "YtflowInputStream", /*stream*/ true);
    auto patternHolder = BuildTestPattern(lambdaFile.GetName());
    ASSERT_NO_FATAL_FAILURE(
        AssertPatternCallables(*patternHolder, {"YtflowInputStream"}));
    const auto* inputNode =
        patternHolder->GetYtflowInputNodes().at("YtflowInputStream");
    TCloneState first(patternHolder);
    TCloneState second(patternHolder);

    ASSERT_NE(
        first.GetContext().MutableValues.get(),
        second.GetContext().MutableValues.get());
    first.SetExternalStream(*inputNode, 11, 12);
    second.SetExternalStream(*inputNode, 21, 22);
    ui64 value = 0;
    ASSERT_EQ(NUdf::EFetchStatus::Ok, first.FetchOutput(&value));
    ASSERT_EQ(11, value);
    ASSERT_EQ(NUdf::EFetchStatus::Ok, second.FetchOutput(&value));
    ASSERT_EQ(21, value);
    ASSERT_EQ(NUdf::EFetchStatus::Yield, first.FetchOutput());
    ASSERT_EQ(NUdf::EFetchStatus::Yield, second.FetchOutput());
    ASSERT_EQ(NUdf::EFetchStatus::Ok, second.FetchOutput(&value));
    ASSERT_EQ(22, value);
    ASSERT_EQ(NUdf::EFetchStatus::Ok, first.FetchOutput(&value));
    ASSERT_EQ(12, value);
}

TEST(TYtflowShareableNodeTest, InputStateIsGraphLocal)
{
    ASSERT_NO_FATAL_FAILURE(
        CheckScalarExternalNodeIsGraphLocal("YtflowInputState"));
}

TEST(TYtflowShareableNodeTest, InputKeyIsGraphLocal)
{
    ASSERT_NO_FATAL_FAILURE(
        CheckScalarExternalNodeIsGraphLocal("YtflowInputKey"));
}

TEST(TYtflowShareableNodeTest, InputMaxHopStartTimeIsGraphLocal)
{
    ASSERT_NO_FATAL_FAILURE(
        CheckScalarExternalNodeIsGraphLocal("YtflowInputMaxHopStartTime"));
}

TEST(TYtflowShareableNodeTest, ChunkedForwardListStateIsGraphLocal)
{
    TTempFileHandle lambdaFile;
    WriteChunkedForwardListCallable(lambdaFile);
    auto patternHolder = BuildTestPattern(lambdaFile.GetName());
    ASSERT_NO_FATAL_FAILURE(AssertPatternCallables(
        *patternHolder,
        {"YtflowInputStream", "YtflowChunkedForwardList"}));
    const auto* inputNode =
        patternHolder->GetYtflowInputNodes().at("YtflowInputStream");
    TCloneState first(patternHolder);
    TCloneState second(patternHolder);

    ASSERT_NE(
        first.GetContext().MutableValues.get(),
        second.GetContext().MutableValues.get());
    first.SetExternalStream(*inputNode, 101, 102);
    second.SetExternalStream(*inputNode, 201, 202);
    ui64 value = 0;
    ASSERT_EQ(NUdf::EFetchStatus::Ok, first.FetchChunk());
    ASSERT_TRUE(first.NextChunkItem(value));
    ASSERT_EQ(101, value);
    ASSERT_FALSE(first.NextChunkItem(value));
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        first.RestartCurrentChunk(),
        yexception,
        "Only one pass over input is supported");

    ASSERT_EQ(NUdf::EFetchStatus::Ok, second.FetchChunk());
    ASSERT_TRUE(second.NextChunkItem(value));
    ASSERT_EQ(201, value);
    ASSERT_FALSE(second.NextChunkItem(value));
    ASSERT_EQ(NUdf::EFetchStatus::Yield, first.FetchChunk());
    ASSERT_EQ(NUdf::EFetchStatus::Yield, second.FetchChunk());

    ASSERT_EQ(NUdf::EFetchStatus::Ok, second.FetchChunk());
    ASSERT_TRUE(second.NextChunkItem(value));
    ASSERT_EQ(202, value);
    ASSERT_FALSE(second.NextChunkItem(value));
    ASSERT_EQ(NUdf::EFetchStatus::Ok, first.FetchChunk());
    ASSERT_TRUE(first.NextChunkItem(value));
    ASSERT_EQ(102, value);
    ASSERT_FALSE(first.NextChunkItem(value));
    ASSERT_EQ(NUdf::EFetchStatus::Finish, first.FetchChunk());
    ASSERT_EQ(NUdf::EFetchStatus::Finish, second.FetchChunk());
}

TEST(TShareableFileNodeTest, AllowedCallablesAreSuitableAndCloneable)
{
    const auto checkInput = [](TStringBuf callableName, bool stream) {
        TTempFileHandle lambdaFile;
        WriteYtflowInputCallable(lambdaFile, callableName, stream);
        ASSERT_NO_FATAL_FAILURE(AssertSuitableAndCloneable(
            BuildTestPattern(lambdaFile.GetName()),
            {callableName}));
    };

    ASSERT_NO_FATAL_FAILURE(checkInput("YtflowInputStream", /*stream*/ true));
    ASSERT_NO_FATAL_FAILURE(checkInput("YtflowInputState", /*stream*/ false));
    ASSERT_NO_FATAL_FAILURE(checkInput("YtflowInputKey", /*stream*/ false));
    ASSERT_NO_FATAL_FAILURE(checkInput(
        "YtflowInputMaxHopStartTime",
        /*stream*/ false));

    TTempFileHandle chunkedLambda;
    WriteChunkedForwardListCallable(chunkedLambda);
    ASSERT_NO_FATAL_FAILURE(AssertSuitableAndCloneable(
        BuildTestPattern(chunkedLambda.GetName()),
        {"YtflowInputStream", "YtflowChunkedForwardList"}));

    TTempFileHandle pathFile;
    ASSERT_NO_FATAL_FAILURE(AssertSuitableAndCloneable(
        BuildFilePattern("FilePath", pathFile.GetName()),
        {"FilePath"}));
}

TEST(TShareableFileNodeTest, FileContentIsHolderOwnedSnapshot)
{
    constexpr TStringBuf initialContent = "shareable-file-content";
    TIntrusivePtr<TComputationPatternHolder> holder;
    {
        TTempFileHandle contentFile;
        contentFile.Write(initialContent.data(), initialContent.size());
        contentFile.Flush();
        holder = BuildFilePattern("FileContent", contentFile.GetName());
        ASSERT_NO_FATAL_FAILURE(
            AssertPatternCallables(*holder, {"FileContent"}));

        contentFile.Resize(0);
        contentFile.Seek(0, sSet);
        constexpr TStringBuf replacement = "replacement";
        contentFile.Write(replacement.data(), replacement.size());
        contentFile.Flush();
    }

    ASSERT_TRUE(holder->GetSuitableForCache());
    ASSERT_FALSE(holder->GetUnsuitabilityReason());
    TCloneState first(holder);
    TCloneState second(holder);
    ASSERT_EQ(initialContent, first.GetResult());
    ASSERT_EQ(initialContent, second.GetResult());
}

TEST(TLookupJoinComputationPatternTest, SuitableHolderCreatesIndependentClones)
{
    TTempFileHandle lambdaFile;
    WriteLookupJoinLambda(lambdaFile);
    NTesting::TScopedEnvironment secureParams(
        TString(SecureParamsEnvironmentVariable),
        R"({"lookup_token"="test-token";})");

    auto patternHolder = BuildTestPattern(lambdaFile.GetName());
    ASSERT_NO_FATAL_FAILURE(
        AssertPatternCallables(*patternHolder, {"YtflowLookupJoin"}));
    ASSERT_TRUE(patternHolder->GetSuitableForCache());
    ASSERT_FALSE(patternHolder->GetUnsuitabilityReason());
    ASSERT_TRUE(patternHolder->GetOutputType()->IsStream());

    TCloneState first(patternHolder);
    TCloneState second(patternHolder);
    patternHolder.Reset();

    ASSERT_NE(&first.GetContext(), &second.GetContext());
    ASSERT_NE(
        first.GetContext().MutableValues.get(),
        second.GetContext().MutableValues.get());
    ASSERT_NE(first.GetContext().Builder, second.GetContext().Builder);
    ASSERT_NE(first.GetOutputIdentity(), second.GetOutputIdentity());
    ASSERT_EQ(NUdf::EFetchStatus::Finish, first.FetchOutput());
    ASSERT_EQ(NUdf::EFetchStatus::Finish, second.FetchOutput());
}

struct TOperationCase {
    TStringBuf Name;
    EInputMode InputMode;
};

const TOperationCase OperationCases[] = {
    // SourceMap and SwiftMap use distinct Flow computations but share the
    // SingleMessage computation-graph contract exercised here.
    {"SourceMap", EInputMode::SingleMessage},
    {"TransformMap", EInputMode::MessageSequence},
    {"SwiftMap", EInputMode::SingleMessage},
};

std::vector<TMessageHolder> MakeSequence(
    const TMapComputationPatternTestBuilder& builder,
    ui64 first,
    ui64 second)
{
    std::vector<TMessageHolder> result;
    result.push_back(builder.MakeMessage(first));
    result.push_back(builder.MakeMessage(second));
    return result;
}

TVector<ui64> ExecuteOperation(
    const TMapComputationPatternTestBuilder& builder,
    IMapComputationGraphWithCodecs& graph,
    const TOperationCase& operation,
    ui64 first,
    ui64 second)
{
    if (operation.InputMode == EInputMode::SingleMessage) {
        auto message = builder.MakeMessage(first);
        return builder.Execute(graph, message);
    }
    auto messages = MakeSequence(builder, first, second);
    return builder.Execute(graph, messages);
}

TVector<ui64> ExpectedValues(
    const TOperationCase& operation,
    ui64 first,
    ui64 second)
{
    return operation.InputMode == EInputMode::SingleMessage
        ? TVector<ui64>{first}
        : TVector<ui64>{first, second};
}

TEST(TMapComputationPatternTest, SuitableMapComputationPatternIsSharedByIndependentGraphs)
{
    TMapComputationPatternTestBuilder builder;

    for (const auto& operation : OperationCases) {
        auto registryHolder = CreateFunctionRegistryHolder({});
        auto patternHolder = builder.BuildPattern(registryHolder);
        ASSERT_TRUE(patternHolder->GetSuitableForCache());
        ASSERT_EQ(
            &registryHolder->GetFunctionRegistry(),
            &patternHolder->GetFunctionRegistry());
        THolder<IMapComputationGraphWithCodecs> first;
        THolder<IMapComputationGraphWithCodecs> second;
        {
            const auto resources = TComputationGraphResources{
                .PatternHolder = patternHolder,
                .FunctionRegistryHolder = registryHolder,
            };
            first = builder.BuildGraph(operation.InputMode, resources);
            second = builder.BuildGraph(operation.InputMode, resources);
        }
        patternHolder.Reset();
        registryHolder.Reset();

        ASSERT_EQ(
            ExpectedValues(operation, 21, 22),
            ExecuteOperation(builder, *second, operation, 21, 22));
        ASSERT_EQ(
            ExpectedValues(operation, 11, 12),
            ExecuteOperation(builder, *first, operation, 11, 12));
        ASSERT_EQ(
            ExpectedValues(operation, 13, 14),
            ExecuteOperation(builder, *first, operation, 13, 14));
        ASSERT_EQ(
            ExpectedValues(operation, 23, 24),
            ExecuteOperation(builder, *second, operation, 23, 24));
    }
}

TEST(TMapComputationPatternTest, SharedRegistryBuildsIndependentPrivateGraphs)
{
    TMapComputationPatternTestBuilder builder;

    for (const auto& operation : OperationCases) {
        auto registryHolder = CreateFunctionRegistryHolder({});
        THolder<IMapComputationGraphWithCodecs> first;
        THolder<IMapComputationGraphWithCodecs> second;
        {
            const auto resources = TComputationGraphResources{
                .FunctionRegistryHolder = registryHolder,
            };
            first = builder.BuildGraph(operation.InputMode, resources);
            second = builder.BuildGraph(operation.InputMode, resources);
        }
        registryHolder.Reset();

        ASSERT_EQ(
            ExpectedValues(operation, 31, 32),
            ExecuteOperation(builder, *first, operation, 31, 32));
        ASSERT_EQ(
            ExpectedValues(operation, 41, 42),
            ExecuteOperation(builder, *second, operation, 41, 42));
    }
}

TEST(TMapComputationPatternTest, NoMapComputationPatternResourceKeepsLegacyExecution)
{
    TMapComputationPatternTestBuilder builder;
    for (const auto& operation : OperationCases) {
        auto graph = builder.BuildGraph(operation.InputMode);
        ASSERT_EQ(
            ExpectedValues(operation, 51, 52),
            ExecuteOperation(builder, *graph, operation, 51, 52));
    }
}

TEST(THoppingComputationPatternTest, SuitablePatternsShareRegistryAndRetainTheirHolders)
{
    THoppingComputationPatternTestBuilder builder;
    auto registryHolder = CreateFunctionRegistryHolder({});
    auto updateStatePattern = builder.BuildUpdateStatePattern(registryHolder);
    auto postprocessPattern = builder.BuildPostprocessPattern(registryHolder);

    ASSERT_TRUE(updateStatePattern->GetSuitableForCache());
    ASSERT_TRUE(postprocessPattern->GetSuitableForCache());
    ASSERT_EQ(
        &registryHolder->GetFunctionRegistry(),
        &updateStatePattern->GetFunctionRegistry());
    ASSERT_EQ(
        &registryHolder->GetFunctionRegistry(),
        &postprocessPattern->GetFunctionRegistry());

    THolder<IUpdateStateComputationGraphWithCodecs> updateStateGraph;
    THolder<IPostprocessComputationGraphWithCodecs> postprocessGraph;
    {
        updateStateGraph = builder.BuildUpdateStateGraph(
            {
                .PatternHolder = updateStatePattern,
                .FunctionRegistryHolder = registryHolder,
            },
            "/definitely-missing/update-state-lambda");
        postprocessGraph = builder.BuildPostprocessGraph(
            {
                .PatternHolder = postprocessPattern,
                .FunctionRegistryHolder = registryHolder,
            },
            "/definitely-missing/postprocess-lambda");
    }
    updateStatePattern.Reset();
    postprocessPattern.Reset();
    registryHolder.Reset();

    builder.Execute(*updateStateGraph, *postprocessGraph);
}

TEST(THoppingComputationPatternTest, MixedSuitableAndUnsuitablePatternsUseIndependentPaths)
{
    THoppingComputationPatternTestBuilder builder;
    auto registryHolder = CreateFunctionRegistryHolder({});
    auto postprocessPattern = builder.BuildPostprocessPattern(registryHolder);
    ASSERT_TRUE(postprocessPattern->GetSuitableForCache());

    THolder<IUpdateStateComputationGraphWithCodecs> updateStateGraph;
    THolder<IPostprocessComputationGraphWithCodecs> postprocessGraph;
    {
        updateStateGraph = builder.BuildUpdateStateGraph(
            {
                .FunctionRegistryHolder = registryHolder,
                .PatternUnsuitabilityReason =
                    EComputationPatternUnsuitabilityReason::MiniKqlPatternNotSuitable,
            },
            std::nullopt,
            {"/definitely-missing/udf.so"});
        postprocessGraph = builder.BuildPostprocessGraph(
            {
                .PatternHolder = postprocessPattern,
                .FunctionRegistryHolder = registryHolder,
            },
            "/definitely-missing/postprocess-lambda");
    }
    postprocessPattern.Reset();
    registryHolder.Reset();

    builder.Execute(*updateStateGraph, *postprocessGraph);
}

TEST(THoppingComputationPatternTest, NoPatternResourcesKeepLegacyExecution)
{
    THoppingComputationPatternTestBuilder builder;
    auto updateStateGraph = builder.BuildUpdateStateGraph();
    auto postprocessGraph = builder.BuildPostprocessGraph();

    builder.Execute(*updateStateGraph, *postprocessGraph);
}

TEST(TPatternSuitabilityDefaultDenyTest, MiniKqlUnsuitablePatternKeepsItsSpecificReason)
{
    const bool miniKqlPatternSuitable = BuildCorePatternAndGetSuitability();
    ASSERT_FALSE(miniKqlPatternSuitable);

    const auto reason = GetPatternUnsuitabilityReason(
        /*ytflowPatternMetadata*/ {},
        miniKqlPatternSuitable);
    ASSERT_TRUE(reason);
    ASSERT_EQ(
        EComputationPatternUnsuitabilityReason::MiniKqlPatternNotSuitable,
        *reason);
}

TEST(TPatternSuitabilityDefaultDenyTest, SpecializedCallablePatternSharingHasStableReasonPriority)
{
    TYtflowPatternMetadata metadata;
    metadata.SpecializedCallables.emplace(
        "YtflowPrivateOnlyCallable",
        EYtflowCallablePatternSharing::PrivateOnly);

    auto reason = GetPatternUnsuitabilityReason(
        metadata,
        /*miniKqlPatternSuitable*/ false);
    ASSERT_TRUE(reason);
    ASSERT_EQ(
        EComputationPatternUnsuitabilityReason::YtflowCallableDenied,
        *reason);

    metadata.SpecializedCallables.emplace(
        "YtflowUnknownCallable",
        EYtflowCallablePatternSharing::Unknown);

    reason = GetPatternUnsuitabilityReason(
        metadata,
        /*miniKqlPatternSuitable*/ false);
    ASSERT_TRUE(reason);
    ASSERT_EQ(
        EComputationPatternUnsuitabilityReason::UnknownYtflowCallable,
        *reason);
}

TEST(TPatternSuitabilityDefaultDenyTest, MalformedLookupJoinReachesNormalNodeFactoryValidation)
{
    TTempFileHandle lambdaFile;
    NTest::WriteZeroInputCallable(lambdaFile, "YtflowLookupJoin");

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        BuildTestPattern(lambdaFile.GetName()),
        yexception,
        "Unexpected inputs count: 0");
}

TEST(TPatternSuitabilityDefaultDenyTest, NonYtflowPatternBuildErrorIsNotConvertedToUnsuitable)
{
    TTempFileHandle lambdaFile;
    NTest::WriteZeroInputCallable(lambdaFile, "UnsupportedNonYtflowCallable");

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        BuildTestPattern(lambdaFile.GetName()),
        yexception,
        "unsupported function: UnsupportedNonYtflowCallable");
}

} // namespace
} // namespace NYql::NYtflow
