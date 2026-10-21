#include <yt/yql/providers/ytflow/gateway/yql_ytflow_config_clusters.h>
#include <yt/yql/providers/ytflow/gateway/yql_ytflow_pipeline_spec.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

#include <yt/yql/providers/ytflow/integration/proto/pq.pb.h>
#include <yt/yql/providers/ytflow/integration/proto/yt.pb.h>

#include <yt/yql/providers/ytflow/provider/yql_ytflow_configuration.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/resources/public.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <google/protobuf/any.pb.h>

#include <utility>

namespace NYql::NYtflow::NPrivate {
namespace {

using namespace NNodes;

class TStreamTestIntegration : public TEmptyYtflowIntegration {
public:
    void FillSourceSettings(
        const TExprNode& /*source*/,
        ::google::protobuf::Any& settings,
        TExprContext& /*ctx*/) override
    {
        NProto::TPQSourceMessage source;
        source.SetTopicPath("input");
        settings.PackFrom(source);
    }

    void FillSinkSettings(
        const TExprNode& /*sink*/,
        ::google::protobuf::Any& settings,
        TExprContext& /*ctx*/) override
    {
        NProto::TPQSinkMessage sink;
        sink.SetTopicPath("output");
        settings.PackFrom(sink);
    }
};

class TStreamTestProvider : public TDataProviderBase {
public:
    TStringBuf GetName() const override
    {
        return "test";
    }

    IYtflowIntegration* GetYtflowIntegration() override
    {
        return &Integration_;
    }

private:
    TStreamTestIntegration Integration_;
};

class TPipelineStreamsTest : public testing::Test {
protected:
    TExprContext Ctx_;
    const TPositionHandle Pos_ = Ctx_.AppendPosition(TPosition());
    const TStructExprType* RowType_ = Ctx_.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        Ctx_.MakeType<TItemExprType>("payload", Ctx_.MakeType<TDataExprType>(NUdf::EDataSlot::String)),
    });

    TCoLambda MakeLambda()
    {
        auto lambda = Build<TCoLambda>(Ctx_, Pos_).Args({"stream"}).Body("stream").Done();
        const auto* streamType = Ctx_.MakeType<TStreamExprType>(RowType_);
        lambda.Args().Ref().ChildPtr(0)->SetTypeAnn(streamType);
        lambda.Ptr()->SetTypeAnn(streamType);
        return lambda;
    }

    TExprNode::TPtr MakeSourceMap()
    {
        auto dataSource = Build<TCoDataSource>(Ctx_, Pos_).Category().Value("test").Build().Done();
        auto read = Ctx_.NewCallable(Pos_, "TestRead", {Ctx_.NewWorld(Pos_), dataSource.Ptr()});
        auto source = Build<TYtflowPersistentSource>(Ctx_, Pos_)
            .Input<TYtflowReadWrap>().Input(read).Build()
            .Done();
        source.Ptr()->SetTypeAnn(Ctx_.MakeType<TListExprType>(RowType_));
        return Build<TYtflowSourceMap>(Ctx_, Pos_)
            .World(Ctx_.NewWorld(Pos_))
            .Sources().Add(source).Build()
            .Settings().Build()
            .Lambda(MakeLambda())
            .Done().Ptr();
    }

    TExprNode::TPtr MakeOutput(const TExprNode::TPtr& operation)
    {
        auto output = Build<TYtflowOutput>(Ctx_, Pos_)
            .Operation(operation)
            .OutputIndex().Value(0).Build()
            .Done().Ptr();
        output->SetTypeAnn(Ctx_.MakeType<TListExprType>(RowType_));
        return output;
    }

    TExprNode::TPtr MakeMap(const TExprNode::TPtr& output)
    {
        return Build<TYtflowSwiftMap>(Ctx_, Pos_)
            .World(Ctx_.NewWorld(Pos_))
            .Sources().Add(TExprBase(output)).Build()
            .Settings().Build()
            .Lambda(MakeLambda())
            .GroupByColumns().Add<TCoAtom>().Value("payload").Build().Build()
            .Done().Ptr();
    }

    TExprNode::TPtr MakeSink(const TExprNode::TPtr& output)
    {
        auto dataSink = Build<TCoDataSink>(Ctx_, Pos_).Category().Value("test").Build().Done();
        auto write = Ctx_.NewCallable(Pos_, "TestWrite", {Ctx_.NewWorld(Pos_), dataSink.Ptr()});
        return Build<TYtflowPersistentSink>(Ctx_, Pos_)
            .World(Ctx_.NewWorld(Pos_))
            .Output(output)
            .Write<TYtflowWriteWrap>().Input(write).Build()
            .Done().Ptr();
    }

    NYT::NFlow::TPipelineSpecPtr BuildSpec(TExprNode::TListType roots)
    {
        TTypeAnnotationContext types;
        types.RuntimeSettings = MakeRuntimeSettings();
        auto provider = MakeIntrusive<TStreamTestProvider>();
        types.AddDataSource("test", provider);
        types.AddDataSink("test", provider);

        auto onError = [] (const TString& message, bool) {
            ADD_FAILURE() << message;
            return false;
        };

        auto config = MakeIntrusive<TYtflowConfiguration>();
        YQL_ENSURE(config->Dispatch(
            NCommon::ALL_CLUSTERS, "LogbrokerConsumerPath", TString("consumer"),
            NCommon::ISettingDispatcher::EStage::STATIC, onError));
        YQL_ENSURE(config->Dispatch(
            NCommon::ALL_CLUSTERS, "_FiniteStreams", TString("false"),
            NCommon::ISettingDispatcher::EStage::STATIC, onError));

        IYtflowGateway::TRunOptions runOptions;
        runOptions.Config(std::make_shared<const TYtflowSettings>(*config)).Types(&types);

        NPrepare::TContext prepareCtx{Ctx_, runOptions, nullptr};
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        THashMap<TStringBuf, ui32> computationCounters;
        TUserDataTable userDataBlocks;
        TBuildPipelineSpecContext buildCtx(prepareCtx, computationCounters, registry.Get(), userDataBlocks, {});

        return BuildPipelineSpec(Ctx_.NewList(Pos_, std::move(roots)), buildCtx).PipelineSpec;
    }

    void AssertSharedStream(const NYT::NFlow::TPipelineSpecPtr& spec)
    {
        ASSERT_EQ(spec->Streams.size(), 1);
        ASSERT_EQ(spec->Computations.size(), 3);
        const auto& streamId = spec->Streams.begin()->first;
        for (const auto& [_, computation] : spec->Computations) {
            if (computation->ComputationClassName == "NYql::NYtflow::TSourceMap") {
                ASSERT_EQ(computation->OutputStreamIds.size(), 1);
                EXPECT_TRUE(computation->OutputStreamIds.contains(streamId));
                const auto indices = computation->Parameters->GetChildValueOrThrow<THashMap<TString, TString>>(
                    "output_indices_by_output_stream_id");
                ASSERT_EQ(indices.size(), 1);
                EXPECT_EQ(indices.begin()->second, "0");
            } else {
                EXPECT_EQ(computation->ComputationClassName, "NYql::NYtflow::TSwiftMap");
                ASSERT_EQ(computation->InputStreamIds.size(), 1);
                EXPECT_TRUE(computation->InputStreamIds.contains(streamId));
            }
        }
    }
};

TUserDataBlock MakeUserDataBlock(std::initializer_list<EUserDataBlockUsage> usages)
{
    TUserDataBlock block;
    for (auto usage : usages) {
        block.Usage.Set(usage);
    }
    return block;
}

void AssertUdfPaths(
    const TVector<TString>& actual,
    std::initializer_list<TStringBuf> expected)
{
    ASSERT_EQ(expected.size(), actual.size());

    auto actualIt = actual.begin();
    for (auto expectedPath : expected) {
        ASSERT_EQ(expectedPath, *actualIt);
        ++actualIt;
    }
}

TYtflowSettings::TConstPtr MakeConfig(TMaybe<TString> value = Nothing())
{
    auto config = MakeIntrusive<TYtflowConfiguration>();
    if (value) {
        TString error;
        const bool dispatched = config->Dispatch(
            NCommon::ALL_CLUSTERS,
            "EnableComputationPatternResources",
            value,
            NCommon::ISettingDispatcher::EStage::STATIC,
            [&error](const TString& message, bool) {
                error = message;
                return false;
            });
        EXPECT_TRUE(dispatched) << error;
    }
    return std::make_shared<const TYtflowSettings>(*config);
}

void AssertNoOperationPipelineSpec(const NYT::NFlow::TPipelineSpecPtr& pipelineSpec)
{
    ASSERT_TRUE(pipelineSpec->Computations.empty());
    ASSERT_TRUE(pipelineSpec->Streams.empty());
    ASSERT_EQ(1, pipelineSpec->Resources.size());

    const auto it = pipelineSpec->Resources.find(NYT::NFlow::YTClientFactoryDefaultResourceId);
    ASSERT_NE(pipelineSpec->Resources.end(), it);
    const auto& resource = it->second;
    ASSERT_EQ("NYT::NFlow::TYTClientFactory", resource->ResourceClassName);
}

template <class TCallback>
void WithBuildPipelineSpecContext(
    bool enabled,
    const TUserDataTable& userDataBlocks,
    TCallback callback,
    TMaybe<TString> optLLVM = Nothing())
{
    TExprContext exprContext;
    TTypeAnnotationContext types;
    types.RuntimeSettings = MakeRuntimeSettings();
    types.OptLLVM = std::move(optLLVM);

    IYtflowGateway::TRunOptions runOptions;
    runOptions
        .Config(MakeConfig(TString(enabled ? "true" : "false")))
        .Types(&types);

    NPrepare::TContext prepareCtx{exprContext, runOptions, nullptr};
    THashMap<TStringBuf, ui32> computationCounters;
    THashMap<TString, TString> secureParams;
    TBuildPipelineSpecContext buildCtx(
        prepareCtx,
        computationCounters,
        nullptr,
        userDataBlocks,
        secureParams);

    callback(buildCtx);
}

class TQueueSourceProvider
    : public TDataProviderBase
    , public TEmptyYtflowIntegration
{
public:
    TStringBuf GetName() const override
    {
        return "test";
    }

    IYtflowIntegration* GetYtflowIntegration() override
    {
        return this;
    }

    void FillSourceSettings(
        const TExprNode& /*source*/,
        ::google::protobuf::Any& settings,
        TExprContext& /*ctx*/) override
    {
        NProto::TYtQueueSourceMessage sourceSettings;
        sourceSettings.SetCluster("test");
        sourceSettings.SetPath("//queue");
        settings.PackFrom(sourceSettings);
    }
};

TExprNode::TPtr MakeSourceMap(TExprContext& ctx)
{
    using namespace NNodes;

    TPositionHandle position;
    const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        ctx.MakeType<TItemExprType>(
            "value",
            ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int64)),
    });
    auto world = ctx.NewWorld(position);
    auto dataSource = ctx.NewCallable(
        position,
        "DataSource",
        {ctx.NewAtom(position, "test")});
    auto read = ctx.NewCallable(position, "TestRead", {world, dataSource});
    auto readWrap = ctx.NewCallable(position, TYtflowReadWrap::CallableName(), {read});
    auto source = ctx.NewCallable(
        position,
        TYtflowPersistentSource::CallableName(),
        {readWrap});
    source->SetTypeAnn(ctx.MakeType<TListExprType>(rowType));

    const auto* streamType = ctx.MakeType<TStreamExprType>(rowType);
    auto stream = ctx.NewArgument(position, "stream");
    stream->SetTypeAnn(streamType);
    auto lambda = ctx.NewLambda(
        position,
        ctx.NewArguments(position, {stream}),
        std::move(stream));
    lambda->SetTypeAnn(streamType);

    return ctx.NewCallable(
        position,
        TYtflowSourceMap::CallableName(),
        {
            world,
            ctx.NewList(position, {source}),
            ctx.NewList(position, /*children*/ {}),
            lambda,
        });
}

NYT::NFlow::TComputationSpecPtr MakeMapComputationSpec(
    TStringBuf className,
    TStringBuf lambdaFile)
{
    auto computationSpec = NYT::New<NYT::NFlow::TComputationSpec>();
    computationSpec->ComputationClassName = className;
    computationSpec->Parameters->AddChild(
        "lambda_file",
        NYT::NYTree::ConvertToNode(lambdaFile));
    return computationSpec;
}

void AssertResourceDescription(
    const NYT::NFlow::TResourceDescriptionPtr& description,
    const NYT::NFlow::TResourceId& alias)
{
    ASSERT_TRUE(description->Alias);
    ASSERT_EQ(alias, *description->Alias);
    ASSERT_TRUE(description->Worker);
    ASSERT_FALSE(description->Controller);
}

void AssertResourceFlags(const NYT::NFlow::TResourceSpecPtr& resourceSpec)
{
    ASSERT_FALSE(resourceSpec->PreloadRequired);
    ASSERT_FALSE(resourceSpec->AlwaysOn);
}

void AssertFunctionRegistryResource(
    const NYT::NFlow::TPipelineSpecPtr& pipelineSpec,
    std::initializer_list<TStringBuf> udfPaths)
{
    const auto resourceIt = pipelineSpec->Resources.find(
        NYT::NFlow::TResourceId("yql-function-registry"));
    ASSERT_NE(pipelineSpec->Resources.end(), resourceIt);

    const auto& resourceSpec = resourceIt->second;
    ASSERT_EQ(
        "NYql::NYtflow::TFunctionRegistryResource",
        resourceSpec->ResourceClassName);
    ASSERT_EQ(2, resourceSpec->Parameters->GetChildren().size());
    ASSERT_EQ(
        1,
        resourceSpec->Parameters->GetChildValueOrThrow<int>("recipe_version"));
    AssertUdfPaths(
        resourceSpec->Parameters->GetChildValueOrThrow<TVector<TString>>("udf_paths"),
        udfPaths);
    ASSERT_TRUE(resourceSpec->Dependencies.empty());
    AssertResourceFlags(resourceSpec);
}

void AssertComputationPatternResource(
    const NYT::NFlow::TPipelineSpecPtr& pipelineSpec,
    const NYT::NFlow::TResourceId& resourceId,
    TStringBuf lambdaFile,
    const TTypeAnnotationContext& types,
    TStringBuf expectedOptLLVM = "")
{
    const auto resourceIt = pipelineSpec->Resources.find(resourceId);
    ASSERT_NE(pipelineSpec->Resources.end(), resourceIt);

    const auto& resourceSpec = resourceIt->second;
    ASSERT_EQ(
        "NYql::NYtflow::TComputationPatternResource",
        resourceSpec->ResourceClassName);
    ASSERT_EQ(5, resourceSpec->Parameters->GetChildren().size());
    ASSERT_EQ(
        1,
        resourceSpec->Parameters->GetChildValueOrThrow<int>("recipe_version"));
    ASSERT_EQ(
        lambdaFile,
        resourceSpec->Parameters->GetChildValueOrThrow<TString>("lambda_file"));
    ASSERT_EQ(
        types.LangVer,
        resourceSpec->Parameters->GetChildValueOrThrow<TLangVersion>("lang_version"));
    ASSERT_EQ(
        expectedOptLLVM,
        resourceSpec->Parameters->GetChildValueOrThrow<TString>("opt_llvm"));
    ASSERT_EQ(
        SerializeRuntimeSettingsToString(*types.RuntimeSettings),
        resourceSpec->Parameters->GetChildValueOrThrow<TString>("runtime_settings"));

    ASSERT_EQ(1, resourceSpec->Dependencies.size());
    const auto dependencyIt = resourceSpec->Dependencies.find(
        NYT::NFlow::TResourceId("yql-function-registry"));
    ASSERT_NE(resourceSpec->Dependencies.end(), dependencyIt);
    AssertResourceDescription(
        dependencyIt->second,
        NYT::NFlow::TResourceId("function_registry"));
    AssertResourceFlags(resourceSpec);
}

void AssertPatternRequirements(
    const NYT::NFlow::TComputationSpecPtr& computationSpec,
    const NYT::NFlow::TResourceId& resourceId,
    TStringBuf alias = "computation_pattern",
    size_t requirementCount = 2,
    size_t parameterCount = 1)
{
    ASSERT_EQ(requirementCount, computationSpec->RequiredResourceIds.size());
    auto requirementIt = computationSpec->RequiredResourceIds.find(resourceId);
    ASSERT_NE(computationSpec->RequiredResourceIds.end(), requirementIt);
    AssertResourceDescription(
        requirementIt->second,
        NYT::NFlow::TResourceId(alias));

    requirementIt = computationSpec->RequiredResourceIds.find(
        NYT::NFlow::TResourceId("yql-function-registry"));
    ASSERT_NE(computationSpec->RequiredResourceIds.end(), requirementIt);
    AssertResourceDescription(
        requirementIt->second,
        NYT::NFlow::TResourceId("function_registry"));

    ASSERT_EQ(parameterCount, computationSpec->Parameters->GetChildren().size());
    ASSERT_FALSE(computationSpec->Parameters->FindChild("function_registry"));
    ASSERT_FALSE(computationSpec->Parameters->FindChild("computation_pattern"));
    ASSERT_FALSE(computationSpec->Parameters->FindChild("function_registry_resource_id"));
    ASSERT_FALSE(computationSpec->Parameters->FindChild("pattern_resource_id"));
}

void PrepareTransformForPipelineValidation(
    const NYT::NFlow::TComputationSpecPtr& computationSpec,
    const TTypeAnnotationContext& types)
{
    using namespace NYT::NTableClient;

    // This key-visitor-only input is a minimal validation harness, not planner output
    // intended for execution. It avoids introducing unrelated global stream wiring.
    computationSpec->GroupBySchema = NYT::New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("hash", EValueType::Uint64, ESortOrder::Ascending),
    });
    computationSpec->KeyVisitorStreams.emplace(
        NYT::NFlow::TStreamId("validation-input"),
        NYT::New<NYT::NFlow::TKeyVisitorStreamSpec>());

    computationSpec->Parameters->AddChild(
        "udf_paths",
        NYT::NYTree::ConvertToNode(TVector<TString>{}));
    computationSpec->Parameters->AddChild(
        "output_indices_by_output_stream_id",
        NYT::NYTree::ConvertToNode(THashMap<NYT::NFlow::TStreamId, TString>{}));
    computationSpec->Parameters->AddChild(
        "lang_version",
        NYT::NYTree::ConvertToNode(types.LangVer));
    computationSpec->Parameters->AddChild(
        "opt_llvm",
        NYT::NYTree::ConvertToNode(types.OptLLVM.GetOrElse(TString())));
    computationSpec->Parameters->AddChild(
        "runtime_settings",
        NYT::NYTree::ConvertToNode(SerializeRuntimeSettingsToString(
            *types.RuntimeSettings)));
}

} // anonymous namespace

TEST_F(TPipelineStreamsTest, SharedOutputCreatesOneStreamForTwoConsumers)
{
    auto operation = MakeSourceMap();
    auto output = MakeOutput(operation);
    auto first = MakeMap(output);
    auto second = MakeMap(output);

    AssertSharedStream(BuildSpec({first, second}));
}

TEST_F(TPipelineStreamsTest, DistinctOutputsWithSameOperationAndIndexShareStream)
{
    auto operation = MakeSourceMap();
    auto firstOutput = MakeOutput(operation);
    auto secondOutput = MakeOutput(operation);
    ASSERT_NE(firstOutput.Get(), secondOutput.Get());
    auto first = MakeMap(firstOutput);
    auto second = MakeMap(secondOutput);

    AssertSharedStream(BuildSpec({first, second}));
}

TEST_F(TPipelineStreamsTest, LambdaOutputHasPersistentAndIntermediateConsumers)
{
    auto operation = MakeSourceMap();
    auto output = MakeOutput(operation);
    auto sink = MakeSink(output);
    auto map = MakeMap(output);

    const auto spec = BuildSpec({sink, map});
    ASSERT_EQ(spec->Streams.size(), 2);
    ASSERT_EQ(spec->Computations.size(), 2);

    NYT::NFlow::TComputationSpecPtr producer;
    NYT::NFlow::TComputationSpecPtr consumer;
    for (const auto& [_, computation] : spec->Computations) {
        if (computation->ComputationClassName == "NYql::NYtflow::TSourceMap") {
            producer = computation;
        } else {
            consumer = computation;
        }
    }
    ASSERT_TRUE(producer);
    ASSERT_TRUE(consumer);
    ASSERT_EQ(producer->OutputStreamIds.size(), 2);
    ASSERT_EQ(producer->Sinks.size(), 1);
    ASSERT_EQ(consumer->InputStreamIds.size(), 1);
    const auto& intermediateId = *consumer->InputStreamIds.begin();
    EXPECT_TRUE(producer->OutputStreamIds.contains(intermediateId));
    EXPECT_TRUE(spec->Streams.contains(intermediateId));

    const auto& sinkSpec = producer->Sinks.begin()->second;
    ASSERT_EQ(sinkSpec->InputStreamIds.size(), 1);
    const auto& persistentId = *sinkSpec->InputStreamIds.begin();
    EXPECT_NE(persistentId, intermediateId);
    EXPECT_TRUE(producer->OutputStreamIds.contains(persistentId));
    EXPECT_TRUE(spec->Streams.contains(persistentId));

    const auto indices = producer->Parameters->GetChildValueOrThrow<THashMap<TString, TString>>(
        "output_indices_by_output_stream_id");
    ASSERT_EQ(indices.size(), 2);
    for (const auto& [_, index] : indices) {
        EXPECT_EQ(index, "0");
    }
}

TEST(TEnableComputationPatternResources, DispatcherParsesExplicitValues)
{
    ASSERT_TRUE(MakeConfig(TString("true"))->GetEnableComputationPatternResources());
    ASSERT_FALSE(MakeConfig(TString("false"))->GetEnableComputationPatternResources());
}

TEST(TEnableComputationPatternResources, AbsentValueIsDisabled)
{
    ASSERT_FALSE(MakeConfig()->GetEnableComputationPatternResources());
}

TEST(TEnableComputationPatternResources, BuildContextStoresResolvedValue)
{
    TExprContext exprContext;
    TTypeAnnotationContext types;
    THashMap<TStringBuf, ui32> computationCounters;
    TUserDataTable userDataBlocks;
    THashMap<TString, TString> secureParams;

    const auto assertContextValue = [&](TYtflowSettings::TConstPtr config, bool enabled) {
        IYtflowGateway::TRunOptions runOptions;
        runOptions
            .Config(std::move(config))
            .Types(&types);
        NPrepare::TContext prepareCtx{exprContext, runOptions, nullptr};
        TBuildPipelineSpecContext buildCtx(
            prepareCtx,
            computationCounters,
            nullptr,
            userDataBlocks,
            secureParams);

        ASSERT_EQ(enabled, buildCtx.EnableComputationPatternResources);
    };

    assertContextValue(MakeConfig(), false);
    assertContextValue(MakeConfig(TString("false")), false);
    assertContextValue(MakeConfig(TString("true")), true);
}

TEST(TEnableComputationPatternResources, NoOperationSpecDoesNotGainResources)
{
    TExprContext exprContext;
    TTypeAnnotationContext types;
    const auto world = exprContext.NewWorld(TPositionHandle{});
    TUserDataTable userDataBlocks;
    THashMap<TString, TString> secureParams;

    TVector<NYT::NFlow::TPipelineSpecPtr> pipelineSpecs;
    for (bool enabled : {false, true}) {
        IYtflowGateway::TRunOptions runOptions;
        runOptions
            .Config(MakeConfig(TString(enabled ? "true" : "false")))
            .Types(&types);
        NPrepare::TContext prepareCtx{exprContext, runOptions, nullptr};
        THashMap<TStringBuf, ui32> computationCounters;
        TBuildPipelineSpecContext buildCtx(
            prepareCtx,
            computationCounters,
            nullptr,
            userDataBlocks,
            secureParams);

        pipelineSpecs.push_back(BuildPipelineSpec(world, buildCtx).PipelineSpec);
    }

    // This guards the pre-planning/no-supported-lambda case. Map-path resource
    // injection is intentionally covered only once a supported map reaches planning.
    AssertNoOperationPipelineSpec(pipelineSpecs[0]);
    AssertNoOperationPipelineSpec(pipelineSpecs[1]);
    ASSERT_TRUE(NYT::NYTree::AreNodesEqual(
        NYT::NYTree::ConvertToNode(pipelineSpecs[0]),
        NYT::NYTree::ConvertToNode(pipelineSpecs[1])));
}

TEST(TPipelineOptLLVMTest, SerializesEffectiveValueForOperationsAndPatterns)
{
    const std::pair<TMaybe<TString>, TString> cases[] = {
        {Nothing(), ""},
        {TString(), ""},
        {TString("OFF"), "OFF"},
        {TString("--compile-options -O1 --dump-stats"), "--compile-options -O1 --dump-stats"},
    };

    for (bool enabled : {false, true}) {
        for (const auto& [optLLVM, expectedOptLLVM] : cases) {
            SCOPED_TRACE(::testing::Message()
                << "Enabled: " << enabled
                << ", OptLLVM: " << optLLVM.GetOrElse("<missing>"));

            TExprContext exprContext;
            TTypeAnnotationContext types;
            types.RuntimeSettings = MakeRuntimeSettings();
            types.OptLLVM = optLLVM;
            types.DataSourceMap.emplace("test", MakeIntrusive<TQueueSourceProvider>());

            TYtflowGatewayConfig gatewayConfig;
            auto* cluster = gatewayConfig.AddClusterMapping();
            cluster->SetName("test");
            cluster->SetRealName("test");
            const std::pair<TString, TString> settings[] = {
                {"EnableComputationPatternResources", enabled ? "true" : "false"},
                {"_FiniteStreams", "true"},
                {"_YtUseSourceWatermark", "false"},
                {"Cluster", "test"},
                {"YtConsumerPath", "//consumer"},
            };
            for (const auto& [name, value] : settings) {
                auto* setting = gatewayConfig.AddDefaultSettings();
                setting->SetName(name);
                setting->SetValue(value);
            }
            auto config = MakeIntrusive<TYtflowConfiguration>();
            config->Init(gatewayConfig);

            IYtflowGateway::TRunOptions runOptions;
            runOptions
                .Config(std::make_shared<const TYtflowSettings>(*config))
                .Types(&types);
            NPrepare::TContext prepareCtx{
                .ExprContext = exprContext,
                .RunOptions = runOptions,
                .ConfigClusters = MakeIntrusive<TConfigClusters>(gatewayConfig),
            };
            auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(
                NKikimr::NMiniKQL::CreateBuiltinRegistry());
            THashMap<TStringBuf, ui32> computationCounters;
            TUserDataTable userDataBlocks;
            THashMap<TString, TString> secureParams;
            TBuildPipelineSpecContext buildCtx(
                prepareCtx,
                computationCounters,
                registry.Get(),
                userDataBlocks,
                secureParams);

            auto pipelineSpec = BuildPipelineSpec(MakeSourceMap(exprContext), buildCtx).PipelineSpec;
            ASSERT_EQ(1, pipelineSpec->Computations.size());
            const auto& computationSpec = pipelineSpec->Computations.begin()->second;
            ASSERT_EQ(
                expectedOptLLVM,
                computationSpec->Parameters->GetChildValueOrThrow<TString>("opt_llvm"));
            if (enabled) {
                AssertComputationPatternResource(
                    pipelineSpec,
                    NYT::NFlow::TResourceId("computation_YtflowSourceMap_0-lambda-computation-pattern"),
                    "computation_YtflowSourceMap_0_lambda",
                    types,
                    expectedOptLLVM);
            } else {
                ASSERT_EQ(1, pipelineSpec->Resources.size());
                ASSERT_EQ(1, computationSpec->RequiredResourceIds.size());
            }
        }
    }
}

TEST(TPipelineUdfPaths, SelectsOnlyUdfAndHasStableOrder)
{
    TUserDataTable first;
    first.emplace(
        TUserDataKey::Udf("/home/z.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));
    first.emplace(
        TUserDataKey::Udf("/home/ignored.txt"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Content}));
    first.emplace(
        TUserDataKey::Udf("/home/a.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Content, EUserDataBlockUsage::Udf}));

    TUserDataTable second;
    second.emplace(
        TUserDataKey::Udf("/home/a.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Content, EUserDataBlockUsage::Udf}));
    second.emplace(
        TUserDataKey::Udf("/home/ignored.txt"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Content}));
    second.emplace(
        TUserDataKey::Udf("/home/z.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));

    const auto firstPaths = BuildPipelineUdfPaths(first);
    const auto secondPaths = BuildPipelineUdfPaths(second);

    AssertUdfPaths(firstPaths, {"a.so", "z.so"});
    ASSERT_EQ(firstPaths, secondPaths);
}

TEST(TPipelineUdfPaths, RemovesDuplicateNormalizedPaths)
{
    TUserDataTable userDataBlocks;
    userDataBlocks.emplace(
        TUserDataKey::Udf("library.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));
    userDataBlocks.emplace(
        TUserDataKey::Udf("/library.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/library.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));

    AssertUdfPaths(BuildPipelineUdfPaths(userDataBlocks), {"library.so"});
}

TEST(TComputationPatternResourcePlanning, DisabledMapIsUnchanged)
{
    TUserDataTable userDataBlocks;
    WithBuildPipelineSpecContext(false, userDataBlocks, [&](auto& buildCtx) {
        const auto pipelineSpec = NYT::New<NYT::NFlow::TPipelineSpec>();
        const auto computationSpec = MakeMapComputationSpec(
            "NYql::NYtflow::TTransformMap",
            "transform-lambda");

        AddComputationPatternResource(
            "computation_YtflowTransformMap_0",
            computationSpec,
            pipelineSpec,
            buildCtx);

        ASSERT_TRUE(pipelineSpec->Resources.empty());
        ASSERT_TRUE(computationSpec->RequiredResourceIds.empty());
        ASSERT_EQ(
            "NYql::NYtflow::TTransformMap",
            computationSpec->ComputationClassName);
        ASSERT_EQ(1, computationSpec->Parameters->GetChildren().size());
    });
}

TEST(TComputationPatternResourcePlanning, EnabledWithoutPlannedMapIsUnchanged)
{
    TUserDataTable userDataBlocks;
    WithBuildPipelineSpecContext(true, userDataBlocks, [](auto&) {
        const auto pipelineSpec = NYT::New<NYT::NFlow::TPipelineSpec>();
        ASSERT_TRUE(pipelineSpec->Resources.empty());
        ASSERT_TRUE(pipelineSpec->Computations.empty());
    });
}

TEST(TComputationPatternResourcePlanning, TransformMapGetsSharedRegistryAndOwnPattern)
{
    TUserDataTable userDataBlocks;
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/z.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/a.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));

    WithBuildPipelineSpecContext(true, userDataBlocks, [&](auto& buildCtx) {
        const auto pipelineSpec = NYT::New<NYT::NFlow::TPipelineSpec>();
        const auto computationSpec = MakeMapComputationSpec(
            "NYql::NYtflow::TTransformMap",
            "transform-lambda");
        const TString computationName = "computation_YtflowTransformMap_0";
        const NYT::NFlow::TResourceId patternResourceId(
            "computation_YtflowTransformMap_0-lambda-computation-pattern");

        AddComputationPatternResource(
            computationName,
            computationSpec,
            pipelineSpec,
            buildCtx);

        ASSERT_EQ(2, pipelineSpec->Resources.size());
        AssertFunctionRegistryResource(pipelineSpec, {"a.so", "z.so"});
        AssertComputationPatternResource(
            pipelineSpec,
            patternResourceId,
            "transform-lambda",
            *buildCtx.RunOptions.Types());
        AssertPatternRequirements(computationSpec, patternResourceId);
        ASSERT_EQ(
            "NYql::NYtflow::TTransformMap",
            computationSpec->ComputationClassName);

        PrepareTransformForPipelineValidation(
            computationSpec,
            *buildCtx.RunOptions.Types());
        pipelineSpec->Computations.emplace(
            NYT::NFlow::TComputationId(computationName),
            computationSpec);
        NYT::NFlow::ValidatePipelineSpec(pipelineSpec);
    });
}

TEST(TComputationPatternResourcePlanning, MultipleMapsShareRegistryAndHaveOwnPatterns)
{
    TUserDataTable userDataBlocks;
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/z.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/a.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));

    WithBuildPipelineSpecContext(true, userDataBlocks, [&](auto& buildCtx) {
        const auto pipelineSpec = NYT::New<NYT::NFlow::TPipelineSpec>();
        const auto transformSpec = MakeMapComputationSpec(
            "NYql::NYtflow::TTransformMap",
            "transform-lambda");
        const auto swiftSpec = MakeMapComputationSpec(
            "NYql::NYtflow::TSwiftMap",
            "swift-lambda");

        const TString transformName = "computation_YtflowTransformMap_0";
        const TString swiftName = "computation_YtflowSwiftMap_0";
        const NYT::NFlow::TResourceId transformPatternId(
            "computation_YtflowTransformMap_0-lambda-computation-pattern");
        const NYT::NFlow::TResourceId swiftPatternId(
            "computation_YtflowSwiftMap_0-lambda-computation-pattern");

        AddComputationPatternResource(
            transformName,
            transformSpec,
            pipelineSpec,
            buildCtx);
        AddComputationPatternResource(
            swiftName,
            swiftSpec,
            pipelineSpec,
            buildCtx);

        pipelineSpec->Computations.emplace(
            NYT::NFlow::TComputationId(transformName),
            transformSpec);
        pipelineSpec->Computations.emplace(
            NYT::NFlow::TComputationId(swiftName),
            swiftSpec);

        ASSERT_EQ(3, pipelineSpec->Resources.size());
        AssertFunctionRegistryResource(pipelineSpec, {"a.so", "z.so"});
        AssertComputationPatternResource(
            pipelineSpec,
            transformPatternId,
            "transform-lambda",
            *buildCtx.RunOptions.Types());
        AssertComputationPatternResource(
            pipelineSpec,
            swiftPatternId,
            "swift-lambda",
            *buildCtx.RunOptions.Types());
        AssertPatternRequirements(transformSpec, transformPatternId);
        AssertPatternRequirements(swiftSpec, swiftPatternId);
        ASSERT_EQ(
            "NYql::NYtflow::TTransformMap",
            transformSpec->ComputationClassName);
        ASSERT_EQ(
            "NYql::NYtflow::TSwiftMap",
            swiftSpec->ComputationClassName);

        THashSet<NYT::NFlow::TResourceId> reachableResources;
        for (const auto& [_, computationSpec] : pipelineSpec->Computations) {
            for (const auto& [resourceId, __] : computationSpec->RequiredResourceIds) {
                reachableResources.insert(resourceId);
            }
        }
        const auto requiredPatternResources = reachableResources;
        for (const auto& patternResourceId : requiredPatternResources) {
            const auto& patternSpec = pipelineSpec->Resources.at(patternResourceId);
            for (const auto& [dependencyId, _] : patternSpec->Dependencies) {
                reachableResources.insert(dependencyId);
            }
        }

        ASSERT_EQ(pipelineSpec->Resources.size(), reachableResources.size());
        for (const auto& [resourceId, _] : pipelineSpec->Resources) {
            ASSERT_TRUE(reachableResources.contains(resourceId));
        }
    });
}

TEST(TComputationPatternResourcePlanning, DisabledHoppingAggregatePatternResourcesAreUnchanged)
{
    TUserDataTable userDataBlocks;
    WithBuildPipelineSpecContext(false, userDataBlocks, [&](auto& buildCtx) {
        const auto pipelineSpec = NYT::New<NYT::NFlow::TPipelineSpec>();
        const auto computationSpec = NYT::New<NYT::NFlow::TComputationSpec>();
        computationSpec->Parameters->AddChild(
            "update_state_lambda_file",
            NYT::NYTree::ConvertToNode("update-state-lambda"));
        computationSpec->Parameters->AddChild(
            "postprocess_lambda_file",
            NYT::NYTree::ConvertToNode("postprocess-lambda"));

        AddHoppingComputationPatternResources(
            "computation_YtflowHoppingAggregate_0",
            computationSpec,
            pipelineSpec,
            buildCtx);

        ASSERT_TRUE(pipelineSpec->Resources.empty());
        ASSERT_TRUE(computationSpec->RequiredResourceIds.empty());
        ASSERT_EQ(2, computationSpec->Parameters->GetChildren().size());
        ASSERT_EQ(
            "update-state-lambda",
            computationSpec->Parameters->GetChildValueOrThrow<TString>(
                "update_state_lambda_file"));
        ASSERT_EQ(
            "postprocess-lambda",
            computationSpec->Parameters->GetChildValueOrThrow<TString>(
                "postprocess_lambda_file"));
    });
}

TEST(TComputationPatternResourcePlanning, HoppingAggregateGetsSharedRegistryAndTwoPatterns)
{
    const TString optLLVM = "--compile-options -O1";
    TUserDataTable userDataBlocks;
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/z.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/a.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));

    WithBuildPipelineSpecContext(/*enabled*/ true, userDataBlocks, [&] (auto& buildCtx) {
        const auto pipelineSpec = NYT::New<NYT::NFlow::TPipelineSpec>();
        const auto computationSpec = NYT::New<NYT::NFlow::TComputationSpec>();
        computationSpec->Parameters->AddChild(
            "update_state_lambda_file",
            NYT::NYTree::ConvertToNode("update-state-lambda"));
        computationSpec->Parameters->AddChild(
            "postprocess_lambda_file",
            NYT::NYTree::ConvertToNode("postprocess-lambda"));

        const TString computationName = "computation_YtflowHoppingAggregate_0";
        const NYT::NFlow::TResourceId updateStatePatternId(
            computationName + "-update_state-computation-pattern");
        const NYT::NFlow::TResourceId postprocessPatternId(
            computationName + "-postprocess-computation-pattern");

        AddHoppingComputationPatternResources(
            computationName,
            computationSpec,
            pipelineSpec,
            buildCtx);

        ASSERT_EQ(3, pipelineSpec->Resources.size());
        AssertFunctionRegistryResource(pipelineSpec, {"a.so", "z.so"});
        AssertComputationPatternResource(
            pipelineSpec,
            updateStatePatternId,
            "update-state-lambda",
            *buildCtx.RunOptions.Types(),
            optLLVM);
        AssertComputationPatternResource(
            pipelineSpec,
            postprocessPatternId,
            "postprocess-lambda",
            *buildCtx.RunOptions.Types(),
            optLLVM);
        AssertPatternRequirements(
            computationSpec,
            updateStatePatternId,
            "update_state_computation_pattern",
            3,
            2);
        AssertPatternRequirements(
            computationSpec,
            postprocessPatternId,
            "postprocess_computation_pattern",
            3,
            2);
    }, optLLVM);
}

} // namespace NYql::NYtflow::NPrivate
