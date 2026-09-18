#include "yql_ytflow_runtime_node_test_utils.h"

#include <yt/yql/providers/ytflow/job/yql_ytflow_computation_graph_with_codecs_base.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_computation_pattern_resource.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_function_registry_resource.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/common/scope.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/yt/memory/new.h>

#include <util/system/type_name.h>
#include <util/system/tempfile.h>

#include <atomic>

namespace NYql::NYtflow {
namespace {

const NYT::NFlow::TResourceId FunctionRegistryResourceId("FunctionRegistry");
const NYT::NFlow::TResourceId ComputationPatternResourceId("ComputationPattern");
constexpr TStringBuf SecureParamsVariable = "YQL_YTFLOW_SECURE_PARAMS";
constexpr TStringBuf PatternResult = "computation-pattern-resource-result";

const NYT::NProfiling::NProto::TCube* GetSensorCube(
    const NYT::NProfiling::NProto::TSensorDump& dump,
    TStringBuf name)
{
    for (const auto& cube : dump.cubes()) {
        if (cube.name() == name) {
            return &cube;
        }
    }
    ADD_FAILURE() << "Missing sensor " << name;
    return nullptr;
}

i64 ReadCounter(
    const NYT::NProfiling::NProto::TSensorDump& dump,
    TStringBuf name)
{
    const auto* cube = GetSensorCube(dump, name);
    if (!cube) {
        return 0;
    }
    for (const auto& projection : cube->projections()) {
        if (projection.tag_ids().empty() &&
            projection.has_value() &&
            projection.has_counter()) {
            return projection.counter();
        }
    }
    ADD_FAILURE() << "Missing default counter projection for " << name;
    return 0;
}

i64 ReadTaggedCounter(
    const NYT::NProfiling::NProto::TSensorDump& dump,
    TStringBuf name,
    TStringBuf tagKey,
    TStringBuf tagValue)
{
    const auto* cube = GetSensorCube(dump, name);
    if (!cube) {
        return 0;
    }
    for (const auto& projection : cube->projections()) {
        if (projection.tag_ids_size() != 1 ||
            !projection.has_value() ||
            !projection.has_counter()) {
            continue;
        }
        const auto& tag = dump.tags().Get(projection.tag_ids().Get(0));
        if (tag.key() == tagKey && tag.value() == tagValue) {
            return projection.counter();
        }
    }
    ADD_FAILURE()
        << "Missing counter projection for " << name << " with tag "
        << tagKey << "=" << tagValue;
    return 0;
}

i64 ReadTimeCounter(
    const NYT::NProfiling::NProto::TSensorDump& dump,
    TStringBuf name)
{
    const auto* cube = GetSensorCube(dump, name);
    if (!cube) {
        return 0;
    }
    for (const auto& projection : cube->projections()) {
        if (projection.tag_ids().empty() &&
            projection.has_value() &&
            projection.has_duration()) {
            return projection.duration();
        }
    }
    ADD_FAILURE() << "Missing default time counter projection for " << name;
    return 0;
}

NYT::NProfiling::TSolomonRegistryPtr MakeSolomonRegistry()
{
    auto registry = NYT::New<NYT::NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(2);
    return registry;
}

NYT::NProfiling::TProfiler MakeTestProfiler(
    const NYT::NProfiling::TSolomonRegistryPtr& registry,
    TStringBuf prefix)
{
    return NYT::NProfiling::TProfiler(registry, prefix, /*namespace*/ "");
}

// Observes that the resource manager invokes the production Load implementation exactly once.
class TCountingFunctionRegistryResource
    : public TFunctionRegistryResource {
public:
    using TFunctionRegistryResource::TFunctionRegistryResource;

    NYT::TFuture<void> Load(
        const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& dependencies) override
    {
        ++LoadCount_;
        return TFunctionRegistryResource::Load(dependencies);
    }

    int GetLoadCount() const
    {
        return LoadCount_.load();
    }

private:
    std::atomic<int> LoadCount_ = 0;
};

// Models a future pattern resource by retaining only the shared holder, not its resource dependency.
class TFunctionRegistryConsumerResource
    : public NYT::NFlow::TResourceBase {
public:
    using TResourceBase::TResourceBase;

    NYT::TFuture<void> Load(
        const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& dependencies) override
    {
        const auto functionRegistryResource =
            dependencies.at(NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias))
                ->As<TFunctionRegistryResource>();
        FunctionRegistryHolder_ = functionRegistryResource->GetFunctionRegistryHolder();
        return NYT::OKFuture;
    }

    const TIntrusivePtr<TFunctionRegistryHolder>& GetFunctionRegistryHolder() const
    {
        return FunctionRegistryHolder_;
    }

private:
    TIntrusivePtr<TFunctionRegistryHolder> FunctionRegistryHolder_;
};

YT_FLOW_DEFINE_RESOURCE(TCountingFunctionRegistryResource);
YT_FLOW_DEFINE_RESOURCE(TFunctionRegistryConsumerResource);

TString GetTestUdfPath()
{
    return BinaryPath("yql/essentials/udfs/test/simple/libsimple_udf.so");
}

TString GetConcurrentPatternBuildUdfPath()
{
    return BinaryPath(
        "yt/yql/providers/ytflow/job/ut/concurrent_pattern_build_udf/"
        "libconcurrent_pattern_build_udf.so");
}

template <class TResource = TFunctionRegistryResource>
NYT::NFlow::TResourceSpecPtr MakeResourceSpec(
    TVector<TString> udfPaths,
    int recipeVersion = FunctionRegistryResourceRecipeVersion)
{
    auto parameters = NYT::New<TFunctionRegistryResourceParameters>();
    parameters->RecipeVersion = recipeVersion;
    parameters->UdfPaths = std::move(udfPaths);

    auto spec = NYT::New<NYT::NFlow::TResourceSpec>();
    spec->ResourceClassName = TypeName<TResource>();
    spec->Parameters = NYT::NYTree::ConvertToNode(parameters)->AsMap();
    return spec;
}

NYT::NFlow::TResourceSpecPtr MakeConsumerResourceSpec(
    NYT::NFlow::TResourceId functionRegistryResourceId)
{
    auto dependency = NYT::New<NYT::NFlow::TResourceDescription>();
    dependency->Alias = NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias);
    dependency->Worker = true;
    dependency->Controller = false;

    auto spec = NYT::New<NYT::NFlow::TResourceSpec>();
    spec->ResourceClassName = TypeName<TFunctionRegistryConsumerResource>();
    spec->Dependencies.emplace(functionRegistryResourceId, std::move(dependency));
    return spec;
}

NYT::TIntrusivePtr<TFunctionRegistryResource> MakeResource(
    TVector<TString> udfPaths,
    int recipeVersion = FunctionRegistryResourceRecipeVersion,
    NYT::NProfiling::TProfiler profiler = {})
{
    auto spec = MakeResourceSpec(std::move(udfPaths), recipeVersion);

    auto context = NYT::New<NYT::NFlow::TResourceContext>();
    context->ResourceId = NYT::NFlow::TResourceId("FunctionRegistry");
    context->ResourceSpec = spec;
    context->Profiler = std::move(profiler);

    auto dynamicContext = NYT::New<NYT::NFlow::TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = NYT::New<NYT::NFlow::TDynamicResourceSpec>();

    return NYT::NFlow::TRegistry::Get()
        ->CreateResource(std::move(context), std::move(dynamicContext))
        ->As<TFunctionRegistryResource>();
}

NYT::NFlow::IResourceManagerPtr MakeResourceManager(
    THashMap<NYT::NFlow::TResourceId, NYT::NFlow::TResourceSpecPtr> resources,
    bool isController,
    NYT::NProfiling::TProfiler profiler = {},
    const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::TResourceInstanceState>&
        predecessorInstanceStates = {})
{
    auto managerContext = NYT::New<NYT::NFlow::TResourceManagerContext>();
    managerContext->Invoker = NYT::GetCurrentInvoker();
    managerContext->StatusProfiler = NYT::NFlow::CreateSyncStatusProfiler();
    managerContext->IsController = isController;
    managerContext->Profiler = std::move(profiler);
    return NYT::NFlow::CreateResourceManager(
        std::move(managerContext),
        resources,
        {},
        {},
        predecessorInstanceStates);
}

NYT::NFlow::TResourceSpecPtr MakeComputationPatternResourceSpec(
    TString lambdaFile,
    int recipeVersion = ComputationPatternResourceRecipeVersion,
    TString runtimeSettings = {})
{
    auto parameters = NYT::New<TComputationPatternResourceParameters>();
    parameters->RecipeVersion = recipeVersion;
    parameters->LambdaFile = std::move(lambdaFile);
    parameters->LangVersion = UnknownLangVersion;
    parameters->OptLLVM = "OFF";
    parameters->RuntimeSettings = std::move(runtimeSettings);

    auto dependency = NYT::New<NYT::NFlow::TResourceDescription>();
    dependency->Alias = NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias);
    dependency->Worker = true;
    dependency->Controller = false;

    auto spec = NYT::New<NYT::NFlow::TResourceSpec>();
    spec->ResourceClassName = TypeName<TComputationPatternResource>();
    spec->Parameters = NYT::NYTree::ConvertToNode(parameters)->AsMap();
    spec->Dependencies.emplace(FunctionRegistryResourceId, std::move(dependency));
    return spec;
}

NYT::TIntrusivePtr<TComputationPatternResource> MakeComputationPatternResource(
    TString lambdaFile,
    int recipeVersion = ComputationPatternResourceRecipeVersion)
{
    auto context = NYT::New<NYT::NFlow::TResourceContext>();
    context->ResourceId = ComputationPatternResourceId;
    context->ResourceSpec =
        MakeComputationPatternResourceSpec(std::move(lambdaFile), recipeVersion);

    auto dynamicContext = NYT::New<NYT::NFlow::TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = NYT::New<NYT::NFlow::TDynamicResourceSpec>();

    return NYT::NFlow::TRegistry::Get()
        ->CreateResource(std::move(context), std::move(dynamicContext))
        ->As<TComputationPatternResource>();
}

NYT::NFlow::IResourceManagerPtr MakeComputationPatternResourceManager(
    NYT::NFlow::TResourceSpecPtr patternSpec,
    bool isController = false,
    NYT::NProfiling::TProfiler profiler = {},
    const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::TResourceInstanceState>&
        predecessorInstanceStates = {})
{
    return MakeResourceManager(
        {
            {FunctionRegistryResourceId, MakeResourceSpec({})},
            {ComputationPatternResourceId, std::move(patternSpec)},
        },
        isController,
        std::move(profiler),
        predecessorInstanceStates);
}

class TPatternClone {
public:
    explicit TPatternClone(TIntrusivePtr<TComputationPatternHolder> holder)
        : Alloc_(__LOCATION__)
        , TypeEnv_(Alloc_)
        , RandomProvider_(CreateDefaultRandomProvider())
        , TimeProvider_(CreateDefaultTimeProvider())
        , Graph_(CloneComputationGraph(
              std::move(holder),
              Alloc_,
              TypeEnv_,
              *RandomProvider_,
              *TimeProvider_))
    {
        Graph_->Prepare();
        Alloc_.Release();
    }

    ~TPatternClone()
    {
        Alloc_.Acquire();
        Graph_.Reset();
    }

    TString GetResult()
    {
        auto guard = Guard(Alloc_);
        const auto result = Graph_->GetValue();
        const auto value = result.AsStringRef();
        return TString(value.Data(), value.Size());
    }

    const NKikimr::NMiniKQL::TComputationContext& GetContext() const
    {
        return Graph_->GetContext();
    }

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc_;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv_;
    TIntrusivePtr<IRandomProvider> RandomProvider_;
    TIntrusivePtr<ITimeProvider> TimeProvider_;
    THolder<NKikimr::NMiniKQL::IComputationGraph> Graph_;
};

class TInspectableComputationGraph
    : public TComputationGraphWithCodecsBase {
public:
    TInspectableComputationGraph(
        const TString& lambdaFile,
        TComputationGraphResources resources)
        : TComputationGraphWithCodecsBase(
              lambdaFile,
              /*udfPaths*/ {},
              UnknownLangVersion,
              "OFF",
              MakeRuntimeSettings(),
              std::move(resources))
    { }

    const TFunctionRegistryHolder* GetRetainedFunctionRegistryHolder() const
    {
        return FunctionRegistryHolder.Get();
    }

    TString GetResult()
    {
        auto guard = Guard(Alloc);
        const auto result = ComputationGraph->GetValue();
        const auto value = result.AsStringRef();
        return TString(value.Data(), value.Size());
    }
};

TString ReadSecureParam(
    const NKikimr::NMiniKQL::TComputationContext& context,
    TStringBuf key)
{
    NUdf::TStringRef value;
    if (!context.Builder) {
        ADD_FAILURE() << "Missing computation context value builder";
        return {};
    }
    if (!context.Builder->GetSecureParam(
            NUdf::TStringRef(key.data(), key.size()),
            value))
    {
        ADD_FAILURE() << "Missing secure parameter " << key;
        return {};
    }
    return TString(value.Data(), value.Size());
}

TEST(TFunctionRegistryResourceTest, CreatesVersionOneRecipeThroughGlobalRegistry)
{
    const TVector<TString> udfPaths = {"first.so", "second.so"};
    auto resource = MakeResource(udfPaths);

    ASSERT_EQ(
        FunctionRegistryResourceRecipeVersion,
        resource->GetParameters()->RecipeVersion);
    ASSERT_EQ(udfPaths, resource->GetParameters()->UdfPaths);

    const auto& rawParameters = resource->GetSpec()->Parameters;
    ASSERT_EQ(2, rawParameters->GetChildCount());
    ASSERT_TRUE(rawParameters->FindChild("recipe_version"));
    ASSERT_TRUE(rawParameters->FindChild("udf_paths"));
}

TEST(TFunctionRegistryResourceTest, RejectsUnknownRecipeVersionDuringCreation)
{
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        MakeResource({}, FunctionRegistryResourceRecipeVersion + 1),
        NYT::TErrorException,
        "Unsupported function registry recipe version 2; expected 1");
}

TEST(TFunctionRegistryResourceTest, LoadsBothUdfsOnceAndSharesHolder)
{
    const auto testUdfPath = GetTestUdfPath();
    const auto concurrentUdfPath = GetConcurrentPatternBuildUdfPath();
    auto resource = MakeResource({testUdfPath, concurrentUdfPath});

    // Loading either path twice would fail with a duplicate UDF module, so
    // success also verifies that each unique recipe path was consumed once.
    NYT::NConcurrency::WaitFor(resource->Load({})).ThrowOnError();

    auto firstConsumerHolder = resource->GetFunctionRegistryHolder();
    auto secondConsumerHolder = resource->GetFunctionRegistryHolder();
    ASSERT_TRUE(firstConsumerHolder);
    ASSERT_TRUE(secondConsumerHolder);
    ASSERT_EQ(firstConsumerHolder.Get(), secondConsumerHolder.Get());

    const auto& firstRegistry = firstConsumerHolder->GetFunctionRegistry();
    const auto& secondRegistry = secondConsumerHolder->GetFunctionRegistry();
    ASSERT_EQ(&firstRegistry, &secondRegistry);
    ASSERT_TRUE(firstRegistry.IsLoadedUdfModule("SimpleUdf"));
    ASSERT_TRUE(firstRegistry.IsLoadedUdfModule("ConcurrentPatternBuild"));

    const auto simplePath = firstRegistry.FindUdfPath("SimpleUdf");
    const auto concurrentPath = firstRegistry.FindUdfPath("ConcurrentPatternBuild");
    ASSERT_TRUE(simplePath);
    ASSERT_TRUE(concurrentPath);
    ASSERT_EQ(testUdfPath, *simplePath);
    ASSERT_EQ(concurrentUdfPath, *concurrentPath);
}

TEST(TFunctionRegistryResourceTest, MissingUdfResourceMetrics)
{
    auto registry = MakeSolomonRegistry();
    auto resource = MakeResource(
        {"missing-ytflow-resource-test-udf.so"},
        FunctionRegistryResourceRecipeVersion,
        MakeTestProfiler(registry, "/resource_metrics"));
    ASSERT_FALSE(resource->GetFunctionRegistryHolder());

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        NYT::NConcurrency::WaitFor(resource->Load({})).ThrowOnError(),
        yexception,
        "missing-ytflow-resource-test-udf.so");
    ASSERT_FALSE(resource->GetFunctionRegistryHolder());

    registry->Collect();
    const auto dump = registry->DumpSensors();
    ASSERT_EQ(
        1,
        ReadCounter(
            dump,
            "/resource_metrics/custom/function_registry/load"));
    ASSERT_EQ(
        1,
        ReadCounter(
            dump,
            "/resource_metrics/custom/function_registry/load_errors"));
}

TEST(TFunctionRegistryResourceTest, ControllerConstructionIsLightweight)
{
    const TString sentinelPath =
        "/ytflow-test-nonexistent/controller-construction-must-not-load-udf.so";
    const NYT::NFlow::TResourceId resourceId("FunctionRegistry");

    // This guaranteed-missing path is a fail-fast sentinel: constructor-side LoadUdfs/dlopen
    // would turn the object into TFailedResource. TResourceBase's contract requires resource
    // constructors to stay light on both controllers and workers.
    auto manager = MakeResourceManager(
        {{resourceId, MakeResourceSpec({sentinelPath})}},
        /*isController*/ true);

    auto resource = manager->Get(resourceId)->As<TFunctionRegistryResource>();
    ASSERT_TRUE(resource);
    ASSERT_EQ(
        TVector<TString>({sentinelPath}),
        resource->GetParameters()->UdfPaths);
    ASSERT_FALSE(resource->GetFunctionRegistryHolder());

    // Production controller code passes only Controller=true resource ids. The empty set models
    // filtering out this worker-only resource, while the manager still constructs every static
    // resource above.
    NYT::NConcurrency::WaitFor(manager->LoadRequiredResources({})).ThrowOnError();

    ASSERT_FALSE(resource->GetFunctionRegistryHolder());
}

TEST(TFunctionRegistryResourceTest, LoadsRequestedResource)
{
    const auto udfPath = GetTestUdfPath();
    const NYT::NFlow::TResourceId resourceId("FunctionRegistry");
    auto manager = MakeResourceManager(
        {{resourceId, MakeResourceSpec({udfPath})}},
        /*isController*/ false);

    auto resource = manager->Get(resourceId)->As<TFunctionRegistryResource>();
    ASSERT_TRUE(resource);
    ASSERT_FALSE(resource->GetFunctionRegistryHolder());

    NYT::NConcurrency::WaitFor(manager->LoadRequiredResources({resourceId})).ThrowOnError();

    const auto holder = resource->GetFunctionRegistryHolder();
    ASSERT_TRUE(holder);
    const auto& registry = holder->GetFunctionRegistry();
    ASSERT_TRUE(registry.IsLoadedUdfModule("SimpleUdf"));
    const auto loadedPath = registry.FindUdfPath("SimpleUdf");
    ASSERT_TRUE(loadedPath);
    ASSERT_EQ(udfPath, *loadedPath);
}

TEST(TSharedFunctionRegistryResourceTest, LoadsOnceForTwoConsumers)
{
    const auto udfPath = GetTestUdfPath();
    const NYT::NFlow::TResourceId registryId("FunctionRegistry");
    const NYT::NFlow::TResourceId firstConsumerId("FirstConsumer");
    const NYT::NFlow::TResourceId secondConsumerId("SecondConsumer");

    auto manager = MakeResourceManager(
        {
            {registryId, MakeResourceSpec<TCountingFunctionRegistryResource>({udfPath})},
            {firstConsumerId, MakeConsumerResourceSpec(registryId)},
            {secondConsumerId, MakeConsumerResourceSpec(registryId)},
        },
        /*isController*/ false);

    auto registryResource = manager->Get(registryId)->As<TCountingFunctionRegistryResource>();
    auto firstConsumer = manager->Get(firstConsumerId)->As<TFunctionRegistryConsumerResource>();
    auto secondConsumer = manager->Get(secondConsumerId)->As<TFunctionRegistryConsumerResource>();

    {
        auto firstLoad = manager->Load(firstConsumerId);
        auto secondLoad = manager->Load(secondConsumerId);
        NYT::NConcurrency::WaitFor(firstLoad).ThrowOnError();
        NYT::NConcurrency::WaitFor(secondLoad).ThrowOnError();
    }

    ASSERT_EQ(1, registryResource->GetLoadCount());
    TFunctionRegistryHolder* rawHolder = nullptr;
    {
        const auto& firstHolder = firstConsumer->GetFunctionRegistryHolder();
        const auto& secondHolder = secondConsumer->GetFunctionRegistryHolder();
        ASSERT_TRUE(firstHolder);
        ASSERT_TRUE(secondHolder);
        ASSERT_EQ(firstHolder.Get(), secondHolder.Get());

        const auto& firstRegistry = firstHolder->GetFunctionRegistry();
        const auto& secondRegistry = secondHolder->GetFunctionRegistry();
        ASSERT_EQ(&firstRegistry, &secondRegistry);
        ASSERT_TRUE(firstRegistry.IsLoadedUdfModule("SimpleUdf"));
        const auto loadedPath = firstRegistry.FindUdfPath("SimpleUdf");
        ASSERT_TRUE(loadedPath);
        ASSERT_EQ(udfPath, *loadedPath);
        rawHolder = firstHolder.Get();
    }

    registryResource.Reset();
    manager.Reset();

    // The manager and R_def are gone: each P_def-like consumer owns exactly one holder
    // reference, and completed load futures own none.
    ASSERT_EQ(2, rawHolder->RefCount());
    ASSERT_TRUE(rawHolder->GetFunctionRegistry().IsLoadedUdfModule("SimpleUdf"));

    firstConsumer.Reset();
    ASSERT_EQ(1, rawHolder->RefCount());
    ASSERT_TRUE(rawHolder->GetFunctionRegistry().IsLoadedUdfModule("SimpleUdf"));

    // Releasing the second consumer destroys the last holder and its UDF state.
    secondConsumer.Reset();
}

TEST(TSharedFunctionRegistryResourceTest, PropagatesFailureToBothConsumers)
{
    const TString missingPath = "missing-shared-ytflow-resource-test-udf.so";
    const NYT::NFlow::TResourceId registryId("FunctionRegistry");
    const NYT::NFlow::TResourceId firstConsumerId("FirstConsumer");
    const NYT::NFlow::TResourceId secondConsumerId("SecondConsumer");

    auto manager = MakeResourceManager(
        {
            {registryId, MakeResourceSpec<TCountingFunctionRegistryResource>({missingPath})},
            {firstConsumerId, MakeConsumerResourceSpec(registryId)},
            {secondConsumerId, MakeConsumerResourceSpec(registryId)},
        },
        /*isController*/ false);

    auto registryResource = manager->Get(registryId)->As<TCountingFunctionRegistryResource>();
    auto firstConsumer = manager->Get(firstConsumerId)->As<TFunctionRegistryConsumerResource>();
    auto secondConsumer = manager->Get(secondConsumerId)->As<TFunctionRegistryConsumerResource>();

    auto firstLoad = manager->Load(firstConsumerId);
    auto secondLoad = manager->Load(secondConsumerId);
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        NYT::NConcurrency::WaitFor(firstLoad).ThrowOnError(),
        NYT::TErrorException,
        missingPath);
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        NYT::NConcurrency::WaitFor(secondLoad).ThrowOnError(),
        NYT::TErrorException,
        missingPath);

    ASSERT_EQ(1, registryResource->GetLoadCount());
    ASSERT_FALSE(firstConsumer->GetFunctionRegistryHolder());
    ASSERT_FALSE(secondConsumer->GetFunctionRegistryHolder());
    ASSERT_FALSE(registryResource->GetFunctionRegistryHolder());

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        NYT::NConcurrency::WaitFor(manager->Load(registryId)).ThrowOnError(),
        NYT::TErrorException,
        missingPath);
    ASSERT_EQ(1, registryResource->GetLoadCount());
}

TEST(TComputationGraphResourceResolutionTest, ResolvesSuitablePatternAndRegistryIndependently)
{
    constexpr TStringBuf patternAlias = "test_computation_pattern";
    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);
    auto manager = MakeComputationPatternResourceManager(
        MakeComputationPatternResourceSpec(lambdaFile.GetName()));
    NYT::NConcurrency::WaitFor(manager->Load(ComputationPatternResourceId)).ThrowOnError();

    auto patternResource = manager->Get(ComputationPatternResourceId)
        ->As<TComputationPatternResource>();
    auto registryResource = manager->Get(FunctionRegistryResourceId)
        ->As<TFunctionRegistryResource>();
    const auto expectedPatternHolder = patternResource->GetResult().GetPatternHolder();
    const auto expectedRegistryHolder = registryResource->GetFunctionRegistryHolder();

    const auto resources = ResolveComputationGraphResources(
        {
            {
                NYT::NFlow::TResourceId(patternAlias),
                patternResource,
            },
            {
                NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias),
                registryResource,
            },
        },
        patternAlias);

    ASSERT_EQ(expectedPatternHolder.Get(), resources.PatternHolder.Get());
    ASSERT_EQ(
        expectedRegistryHolder.Get(),
        resources.FunctionRegistryHolder.Get());
    ASSERT_EQ(
        &expectedRegistryHolder->GetFunctionRegistry(),
        &resources.PatternHolder->GetFunctionRegistry());
    ASSERT_FALSE(resources.PatternUnsuitabilityReason);
}

TEST(TComputationGraphResourceResolutionTest, ResolvesUnsuitableResultToRegistryFallback)
{
    const auto expectedRegistryHolder = CreateFunctionRegistryHolder({});
    const auto patternResult = TComputationPatternResult::Unsuitable(
        EComputationPatternUnsuitabilityReason::YtflowCallableDenied);
    const auto resources = MakeComputationGraphResources(
        patternResult,
        expectedRegistryHolder);

    ASSERT_FALSE(resources.PatternHolder);
    ASSERT_EQ(
        expectedRegistryHolder.Get(),
        resources.FunctionRegistryHolder.Get());
    ASSERT_EQ(
        EComputationPatternUnsuitabilityReason::YtflowCallableDenied,
        resources.PatternUnsuitabilityReason);
}

TEST(TComputationGraphResourceResolutionTest, RegistryOnlyResourcesBuildGraphWithThatRegistry)
{
    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);
    auto registryResource = MakeResource({});
    NYT::NConcurrency::WaitFor(registryResource->Load({})).ThrowOnError();
    auto expectedRegistryHolder = registryResource->GetFunctionRegistryHolder();
    const auto* expectedRegistryHolderPtr = expectedRegistryHolder.Get();

    auto resources = ResolveComputationGraphResources(
        {
            {
                NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias),
                registryResource,
            },
        },
        ComputationPatternResourceAlias);
    ASSERT_FALSE(resources.PatternHolder);
    ASSERT_EQ(
        expectedRegistryHolderPtr,
        resources.FunctionRegistryHolder.Get());
    ASSERT_FALSE(resources.PatternUnsuitabilityReason);

    TInspectableComputationGraph graph(lambdaFile.GetName(), std::move(resources));
    registryResource.Reset();
    expectedRegistryHolder.Reset();

    ASSERT_EQ(
        expectedRegistryHolderPtr,
        graph.GetRetainedFunctionRegistryHolder());
    ASSERT_EQ(PatternResult, graph.GetResult());
}

TEST(TComputationGraphResourceResolutionTest, RejectsPatternWithoutRegistry)
{
    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);
    auto manager = MakeComputationPatternResourceManager(
        MakeComputationPatternResourceSpec(lambdaFile.GetName()));
    NYT::NConcurrency::WaitFor(manager->Load(ComputationPatternResourceId)).ThrowOnError();

    auto patternResource = manager->Get(ComputationPatternResourceId)
        ->As<TComputationPatternResource>();
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        ResolveComputationGraphResources(
            {
                {
                    NYT::NFlow::TResourceId(ComputationPatternResourceAlias),
                    patternResource,
                },
        },
        ComputationPatternResourceAlias),
        yexception,
        "Computation with a pattern resource requires a direct function registry resource");
}

TEST(TComputationPatternResourceTest, RecipeHasOnlyExecutionIndependentFields)
{
    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);
    auto resource = MakeComputationPatternResource(lambdaFile.GetName());

    ASSERT_EQ(
        ComputationPatternResourceRecipeVersion,
        resource->GetParameters()->RecipeVersion);
    ASSERT_EQ(
        lambdaFile.GetName(),
        resource->GetParameters()->LambdaFile);
    ASSERT_EQ(
        UnknownLangVersion,
        resource->GetParameters()->LangVersion);
    ASSERT_EQ("OFF", resource->GetParameters()->OptLLVM);
    ASSERT_TRUE(resource->GetParameters()->RuntimeSettings.empty());

    const auto& parameters = resource->GetSpec()->Parameters;
    ASSERT_EQ(5, parameters->GetChildCount());
    ASSERT_TRUE(parameters->FindChild("recipe_version"));
    ASSERT_TRUE(parameters->FindChild("lambda_file"));
    ASSERT_TRUE(parameters->FindChild("lang_version"));
    ASSERT_TRUE(parameters->FindChild("opt_llvm"));
    ASSERT_TRUE(parameters->FindChild("runtime_settings"));
    ASSERT_FALSE(parameters->FindChild("udf_paths"));
    ASSERT_FALSE(parameters->FindChild("function_registry_id"));
    ASSERT_FALSE(parameters->FindChild("secure_params"));
    ASSERT_FALSE(parameters->FindChild("input_codecs"));
    ASSERT_FALSE(parameters->FindChild("output_codecs"));
    ASSERT_FALSE(parameters->FindChild("outputs"));
    ASSERT_FALSE(parameters->FindChild("input_mode"));
}

TEST(TComputationPatternResourceTest, RejectsUnknownRecipeVersion)
{
    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        MakeComputationPatternResource(
            lambdaFile.GetName(),
            ComputationPatternResourceRecipeVersion + 1),
        NYT::TErrorException,
        "Unsupported computation pattern recipe version 2; expected 1");
}

TEST(TComputationPatternResourceTest, MaterializationAndCloneResourceMetrics)
{
    NTesting::TScopedEnvironment cpuToVCpuFactor("YT_CPU_TO_VCPU_FACTOR", "1");
    auto registry = MakeSolomonRegistry();
    const auto resourceProfiler = MakeTestProfiler(registry, "/resource_metrics");

    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);
    auto manager = MakeComputationPatternResourceManager(
        MakeComputationPatternResourceSpec(lambdaFile.GetName()),
        /*isController*/ false,
        resourceProfiler);

    auto firstLoad = manager->Load(ComputationPatternResourceId);
    auto secondLoad = manager->Load(ComputationPatternResourceId);
    NYT::NConcurrency::WaitFor(firstLoad).ThrowOnError();
    NYT::NConcurrency::WaitFor(secondLoad).ThrowOnError();

    auto resource = manager->Get(ComputationPatternResourceId)
        ->As<TComputationPatternResource>();
    const auto& result = resource->GetResult();
    ASSERT_TRUE(result.IsSuitable());

    auto registryResource = manager->Get(FunctionRegistryResourceId)
        ->As<TFunctionRegistryResource>();
    const auto registryHolder = registryResource->GetFunctionRegistryHolder();

    const auto graphProfiler = MakeTestProfiler(
        registry,
        "/resource_metrics/computation");
    TComputationGraphWithCodecsBase firstGraph(
        {},
        {},
        UnknownLangVersion,
        "OFF",
        MakeRuntimeSettings(),
        TComputationGraphResources{
            result.GetPatternHolder(),
            registryHolder,
            {},
        },
        graphProfiler);
    TComputationGraphWithCodecsBase secondGraph(
        {},
        {},
        UnknownLangVersion,
        "OFF",
        MakeRuntimeSettings(),
        TComputationGraphResources{
            result.GetPatternHolder(),
            registryHolder,
            {},
        },
        graphProfiler);

    TComputationGraphWithCodecsBase fallbackGraph(
        lambdaFile.GetName(),
        {},
        UnknownLangVersion,
        "OFF",
        MakeRuntimeSettings(),
        TComputationGraphResources{
            {},
            registryHolder,
            EComputationPatternUnsuitabilityReason::YtflowCallableDenied,
        },
        graphProfiler);

    // Load/clone/prepare guards have already been destroyed. Keeping their owning
    // resources and graphs alive must keep the Solomon sensors observable.
    registry->Collect();
    const auto dump = registry->DumpSensors();
    constexpr TStringBuf resourcePrefix = "/resource_metrics/resource/custom";
    constexpr TStringBuf graphPrefix = "/resource_metrics/computation/custom";

    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(resourcePrefix) + "/function_registry/load"));
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(resourcePrefix) + "/computation_pattern/load"));
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(resourcePrefix) + "/computation_pattern/suitable"));
    ASSERT_GT(
        ReadCounter(
            dump,
            TString(resourcePrefix) + "/computation_pattern/lambda_file_bytes"),
        0);
    ASSERT_GT(
        ReadCounter(dump, TString(resourcePrefix) + "/computation_pattern/nodes"),
        0);
    ASSERT_EQ(
        2,
        ReadCounter(dump, TString(graphPrefix) + "/computation_graph/clone"));
    ASSERT_EQ(
        3,
        ReadCounter(dump, TString(graphPrefix) + "/computation_graph/prepare"));
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(graphPrefix) + "/computation_graph/fallback"));
    ASSERT_EQ(
        1,
        ReadTaggedCounter(
            dump,
            TString(graphPrefix) + "/computation_graph/fallback",
            "reason",
            "ytflow_callable_denied"));
    ASSERT_EQ(
        0,
        ReadTaggedCounter(
            dump,
            TString(graphPrefix) + "/computation_graph/fallback",
            "reason",
            "unknown_ytflow_callable"));
    ASSERT_EQ(
        0,
        ReadTaggedCounter(
            dump,
            TString(graphPrefix) + "/computation_graph/fallback",
            "reason",
            "minikql_pattern_not_suitable"));

    for (const auto& operationPath : {
             TString(resourcePrefix) + "/function_registry/load",
             TString(resourcePrefix) + "/computation_pattern/load",
             TString(graphPrefix) + "/computation_graph/clone",
             TString(graphPrefix) + "/computation_graph/prepare",
         })
    {
        TString cpuTimePath(operationPath);
        cpuTimePath += "/cpu_time";
        TString vcpuTimePath(operationPath);
        vcpuTimePath += "/vcpu_time";

        const auto cpuTime = ReadTimeCounter(dump, cpuTimePath);
        ASSERT_EQ(cpuTime, ReadTimeCounter(dump, vcpuTimePath));
    }
}

TEST(TComputationPatternResourceTest, SuccessorResourceManagerReloadsResourcesAndKeepsOldGraphAlive)
{
    auto registry = MakeSolomonRegistry();
    const auto resourceProfiler = MakeTestProfiler(registry, "/resource_metrics");

    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);

    auto makeManager = [&] (const auto& predecessorInstanceStates) {
        return MakeComputationPatternResourceManager(
            MakeComputationPatternResourceSpec(lambdaFile.GetName()),
            /*isController*/ false,
            resourceProfiler,
            predecessorInstanceStates);
    };

    auto firstManager = makeManager(
        THashMap<NYT::NFlow::TResourceId, NYT::NFlow::TResourceInstanceState>{});
    auto firstLoad = firstManager->Load(ComputationPatternResourceId);
    auto repeatedFirstLoad = firstManager->Load(ComputationPatternResourceId);
    NYT::NConcurrency::WaitFor(firstLoad).ThrowOnError();
    NYT::NConcurrency::WaitFor(repeatedFirstLoad).ThrowOnError();

    auto firstPatternResource = firstManager->Get(ComputationPatternResourceId)
        ->As<TComputationPatternResource>();
    auto firstRegistryResource = firstManager->Get(FunctionRegistryResourceId)
        ->As<TFunctionRegistryResource>();
    ASSERT_NE(
        NYT::NFlow::TResourceInstanceId{},
        firstPatternResource->GetContext()->ResourceInstanceId);
    ASSERT_NE(
        NYT::NFlow::TResourceInstanceId{},
        firstRegistryResource->GetContext()->ResourceInstanceId);
    ASSERT_EQ(
        0,
        firstPatternResource->GetContext()->ResourceIncarnationGeneration);
    ASSERT_EQ(
        0,
        firstRegistryResource->GetContext()->ResourceIncarnationGeneration);

    TPatternClone firstGraph(firstPatternResource->GetResult().GetPatternHolder());
    ASSERT_EQ(PatternResult, firstGraph.GetResult());

    registry->Collect();
    constexpr TStringBuf resourcePrefix = "/resource_metrics/resource/custom";
    auto dump = registry->DumpSensors();
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(resourcePrefix) + "/function_registry/load"));
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(resourcePrefix) + "/computation_pattern/load"));

    const auto predecessorInstanceStates = firstManager->GetResourceInstanceStates();
    auto secondManager = makeManager(predecessorInstanceStates);
    auto secondPatternResource = secondManager->Get(ComputationPatternResourceId)
        ->As<TComputationPatternResource>();
    auto secondRegistryResource = secondManager->Get(FunctionRegistryResourceId)
        ->As<TFunctionRegistryResource>();
    ASSERT_NE(
        NYT::NFlow::TResourceInstanceId{},
        secondPatternResource->GetContext()->ResourceInstanceId);
    ASSERT_NE(
        NYT::NFlow::TResourceInstanceId{},
        secondRegistryResource->GetContext()->ResourceInstanceId);
    ASSERT_NE(
        firstPatternResource->GetContext()->ResourceInstanceId,
        secondPatternResource->GetContext()->ResourceInstanceId);
    ASSERT_NE(
        firstRegistryResource->GetContext()->ResourceInstanceId,
        secondRegistryResource->GetContext()->ResourceInstanceId);
    ASSERT_EQ(
        1,
        secondPatternResource->GetContext()->ResourceIncarnationGeneration);
    ASSERT_EQ(
        1,
        secondRegistryResource->GetContext()->ResourceIncarnationGeneration);

    firstLoad.Reset();
    repeatedFirstLoad.Reset();
    firstPatternResource.Reset();
    firstRegistryResource.Reset();
    firstManager.Reset();

    // A graph from the superseded generation owns its pattern and registry
    // holders independently of the resource manager that materialized them.
    ASSERT_EQ(PatternResult, firstGraph.GetResult());

    auto secondLoad = secondManager->Load(ComputationPatternResourceId);
    auto repeatedSecondLoad = secondManager->Load(ComputationPatternResourceId);
    NYT::NConcurrency::WaitFor(secondLoad).ThrowOnError();
    NYT::NConcurrency::WaitFor(repeatedSecondLoad).ThrowOnError();

    TPatternClone secondGraph(secondPatternResource->GetResult().GetPatternHolder());
    ASSERT_EQ(PatternResult, secondGraph.GetResult());

    // Resource metrics are owned by the live incarnation. Together with the
    // first snapshot above, this verifies one materialization in each generation.
    registry->Collect();
    dump = registry->DumpSensors();
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(resourcePrefix) + "/function_registry/load"));
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(resourcePrefix) + "/computation_pattern/load"));
    ASSERT_EQ(PatternResult, firstGraph.GetResult());
    ASSERT_EQ(PatternResult, secondGraph.GetResult());
}

TEST(TComputationPatternResourceTest, LoadsDependencyByAliasAndClonesGraphs)
{
    ASSERT_EQ(
        TStringBuf("function_registry"),
        FunctionRegistryDependencyAlias);

    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);
    auto patternSpec = MakeComputationPatternResourceSpec(lambdaFile.GetName());
    ASSERT_EQ(1, patternSpec->Dependencies.size());
    const auto& dependency = patternSpec->Dependencies.at(FunctionRegistryResourceId);
    ASSERT_TRUE(dependency->Alias);
    ASSERT_EQ(
        NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias),
        *dependency->Alias);

    auto manager = MakeComputationPatternResourceManager(std::move(patternSpec));
    NYT::NConcurrency::WaitFor(manager->Load(ComputationPatternResourceId)).ThrowOnError();

    auto resource = manager->Get(ComputationPatternResourceId)
        ->As<TComputationPatternResource>();
    const auto& result = resource->GetResult();
    ASSERT_TRUE(result.IsSuitable());
    TPatternClone first(result.GetPatternHolder());
    TPatternClone second(result.GetPatternHolder());
    ASSERT_NE(&first.GetContext(), &second.GetContext());
    ASSERT_EQ(PatternResult, first.GetResult());
    ASSERT_EQ(PatternResult, second.GetResult());
}

TEST(TComputationPatternResourceTest, UnsuitableResultMetrics)
{
    auto registry = MakeSolomonRegistry();
    auto context = NYT::New<NYT::NFlow::TResourceContext>();
    context->Profiler = MakeTestProfiler(registry, "/resource_metrics/resource");
    TComputationPatternMetrics metrics(*context);
    for (const auto reason : {
             EComputationPatternUnsuitabilityReason::YtflowCallableDenied,
             EComputationPatternUnsuitabilityReason::UnknownYtflowCallable,
             EComputationPatternUnsuitabilityReason::MiniKqlPatternNotSuitable,
         })
    {
        const auto result = TComputationPatternResult::Unsuitable(reason);
        metrics.RecordResult(result);
    }

    registry->Collect();
    const auto dump = registry->DumpSensors();
    constexpr TStringBuf prefix = "/resource_metrics/resource/custom/computation_pattern";
    ASSERT_EQ(
        3,
        ReadCounter(dump, TString(prefix) + "/unsuitable"));
    ASSERT_EQ(
        1,
        ReadTaggedCounter(
            dump,
            TString(prefix) + "/unsuitable",
            "reason",
            "ytflow_callable_denied"));
    ASSERT_EQ(
        1,
        ReadTaggedCounter(
            dump,
            TString(prefix) + "/unsuitable",
            "reason",
            "unknown_ytflow_callable"));
    ASSERT_EQ(
        1,
        ReadTaggedCounter(
            dump,
            TString(prefix) + "/unsuitable",
            "reason",
            "minikql_pattern_not_suitable"));
}

TEST(TComputationPatternResourceTest, MalformedLookupJoinResourceReportsLoadErrorMetrics)
{
    auto registry = MakeSolomonRegistry();
    TTempFileHandle lambdaFile;
    NTest::WriteZeroInputCallable(lambdaFile, "YtflowLookupJoin");
    auto manager = MakeComputationPatternResourceManager(
        MakeComputationPatternResourceSpec(lambdaFile.GetName()),
        /*isController*/ false,
        MakeTestProfiler(registry, "/resource_metrics"));
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        NYT::NConcurrency::WaitFor(manager->Load(ComputationPatternResourceId))
            .ThrowOnError(),
        NYT::TErrorException,
        "Unexpected inputs count: 0");

    registry->Collect();
    const auto dump = registry->DumpSensors();
    constexpr TStringBuf prefix = "/resource_metrics/resource/custom/computation_pattern";
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(prefix) + "/load"));
    ASSERT_EQ(
        1,
        ReadCounter(dump, TString(prefix) + "/load_errors"));
}

TEST(TComputationPatternResourceTest, InvalidLambdaResourceMetrics)
{
    {
        auto registry = MakeSolomonRegistry();
        const TString missingFile =
            "/ytflow-test-nonexistent/computation-pattern-lambda";
        auto manager = MakeComputationPatternResourceManager(
            MakeComputationPatternResourceSpec(missingFile),
            /*isController*/ false,
            MakeTestProfiler(registry, "/resource_metrics"));
        ASSERT_THROW(
            NYT::NConcurrency::WaitFor(manager->Load(ComputationPatternResourceId))
                .ThrowOnError(),
            NYT::TErrorException);

        registry->Collect();
        const auto dump = registry->DumpSensors();
        ASSERT_EQ(
            1,
            ReadCounter(
                dump,
                "/resource_metrics/resource/custom/computation_pattern/load_errors"));
    }

    {
        auto registry = MakeSolomonRegistry();
        TTempFileHandle malformedFile;
        constexpr TStringBuf malformed = "not a serialized MiniKQL program";
        malformedFile.Write(malformed.data(), malformed.size());
        malformedFile.Flush();
        auto manager = MakeComputationPatternResourceManager(
            MakeComputationPatternResourceSpec(malformedFile.GetName()),
            /*isController*/ false,
            MakeTestProfiler(registry, "/resource_metrics"));
        ASSERT_THROW(
            NYT::NConcurrency::WaitFor(manager->Load(ComputationPatternResourceId))
                .ThrowOnError(),
            NYT::TErrorException);
        registry->Collect();
        const auto dump = registry->DumpSensors();
        ASSERT_EQ(
            1,
            ReadCounter(
                dump,
                "/resource_metrics/resource/custom/computation_pattern/load_errors"));
    }
}

TEST(TComputationPatternResourceTest, ControllerConstructionDoesNotReadLambda)
{
    const TString missingFile =
        "/ytflow-test-nonexistent/controller-must-not-read-pattern";
    auto manager = MakeComputationPatternResourceManager(
        MakeComputationPatternResourceSpec(missingFile),
        /*isController*/ true);

    auto resource = manager->Get(ComputationPatternResourceId)
        ->As<TComputationPatternResource>();
    ASSERT_TRUE(resource);
    ASSERT_EQ(missingFile, resource->GetParameters()->LambdaFile);
    NYT::NConcurrency::WaitFor(manager->LoadRequiredResources({})).ThrowOnError();
}

TEST(TComputationPatternResourceTest, SecureParamsAreWorkerSnapshotNotRecipeData)
{
    TTempFileHandle lambdaFile;
    NTest::WriteConditionalStringLambda(lambdaFile, PatternResult);
    auto patternSpec = MakeComputationPatternResourceSpec(lambdaFile.GetName());
    ASSERT_FALSE(patternSpec->Parameters->FindChild("secure_params"));

    NYT::NFlow::IResourceManagerPtr manager;
    {
        NTesting::TScopedEnvironment constructionEnvironment(
            TString(SecureParamsVariable),
            R"({"secret"="construction-secret";})");
        manager = MakeComputationPatternResourceManager(std::move(patternSpec));
    }
    {
        NTesting::TScopedEnvironment loadEnvironment(
            TString(SecureParamsVariable),
            R"({"secret"="resource-secret-A";})");
        NYT::NConcurrency::WaitFor(manager->Load(ComputationPatternResourceId))
            .ThrowOnError();
    }
    auto resource = manager->Get(ComputationPatternResourceId)
        ->As<TComputationPatternResource>();
    const auto holder = resource->GetResult().GetPatternHolder();

    NTesting::TScopedEnvironment secondEnvironment(
        TString(SecureParamsVariable),
        R"({"secret"="resource-secret-B";})");
    TPatternClone first(holder);
    TPatternClone second(holder);
    ASSERT_EQ(
        "resource-secret-A",
        ReadSecureParam(first.GetContext(), "secret"));
    ASSERT_EQ(
        "resource-secret-A",
        ReadSecureParam(second.GetContext(), "secret"));
}

} // namespace
} // namespace NYql::NYtflow
