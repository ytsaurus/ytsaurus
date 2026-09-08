#include <yt/yql/providers/ytflow/gateway/yql_ytflow_pipeline_spec.h>

#include <yt/yql/providers/ytflow/provider/yql_ytflow_configuration.h>

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/resources/public.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYql::NYtflow::NPrivate {
namespace {

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
            NCommon::TSettingDispatcher::EStage::STATIC,
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
    TCallback callback)
{
    TExprContext exprContext;
    TTypeAnnotationContext types;
    types.RuntimeSettings = MakeRuntimeSettings();

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
    const TTypeAnnotationContext& types)
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
        "OFF",
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
        "runtime_settings",
        NYT::NYTree::ConvertToNode(SerializeRuntimeSettingsToString(
            *types.RuntimeSettings)));
}

} // anonymous namespace

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
    THashMap<TStringBuf, ui32> computationCounters;
    TUserDataTable userDataBlocks;
    THashMap<TString, TString> secureParams;

    const auto assertContextValue = [&](TYtflowSettings::TConstPtr config, bool enabled) {
        IYtflowGateway::TRunOptions runOptions;
        runOptions.Config(std::move(config));
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
    const auto world = exprContext.NewWorld(TPositionHandle{});
    TUserDataTable userDataBlocks;
    THashMap<TString, TString> secureParams;

    TVector<NYT::NFlow::TPipelineSpecPtr> pipelineSpecs;
    for (bool enabled : {false, true}) {
        IYtflowGateway::TRunOptions runOptions;
        runOptions.Config(MakeConfig(TString(enabled ? "true" : "false")));
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
    TUserDataTable userDataBlocks;
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/z.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));
    userDataBlocks.emplace(
        TUserDataKey::Udf("/home/a.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));

    WithBuildPipelineSpecContext(true, userDataBlocks, [&](auto& buildCtx) {
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
            *buildCtx.RunOptions.Types());
        AssertComputationPatternResource(
            pipelineSpec,
            postprocessPatternId,
            "postprocess-lambda",
            *buildCtx.RunOptions.Types());
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
    });
}

} // namespace NYql::NYtflow::NPrivate
