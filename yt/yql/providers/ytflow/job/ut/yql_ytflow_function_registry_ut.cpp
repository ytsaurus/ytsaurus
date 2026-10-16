#include "yql_ytflow_runtime_node_test_utils.h"

#include <yt/yql/providers/ytflow/job/yql_ytflow_computation_graph_with_codecs_base.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_computation_pattern.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_function_registry.h>

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/system/tempfile.h>

#include <array>
#include <atomic>
#include <future>
#include <thread>
#include <type_traits>

namespace NYql::NYtflow {
namespace {

constexpr ui32 TestResult = 42;

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

class TTestComputationGraph
    : public TComputationGraphWithCodecsBase
{
public:
    explicit TTestComputationGraph(const TString& lambdaFile)
        : TComputationGraphWithCodecsBase(
            lambdaFile,
            /*udfPaths*/ {},
            UnknownLangVersion,
            "OFF",
            MakeRuntimeSettings())
    { }

    const NKikimr::NMiniKQL::IFunctionRegistry* GetRegistryIdentity() const
    {
        return &FunctionRegistryHolder->GetFunctionRegistry();
    }

    ui32 GetResult()
    {
        auto guard = Guard(Alloc);
        return ComputationGraph->GetValue().Get<ui32>();
    }
};

void WriteTestLambda(TTempFileHandle& file)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [](TProgramBuilder& programBuilder) {
        return programBuilder.Add(
            programBuilder.NewDataLiteral<ui32>(40),
            programBuilder.NewDataLiteral<ui32>(2));
    });
}

void WriteUdfLambda(TTempFileHandle& file, TStringBuf functionName)
{
    using namespace NKikimr::NMiniKQL;

    NTest::WriteRuntimeNode(file, [functionName](TProgramBuilder& programBuilder) {
        const auto& env = programBuilder.GetTypeEnvironment();
        auto returnType = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        auto functionType = TCallableTypeBuilder(env, "UDF", returnType).Build();
        auto function = programBuilder.TypedUdf(
            functionName,
            functionType,
            programBuilder.NewVoid(),
            env.GetVoidLazy()->GetType());
        return programBuilder.Apply(function, {});
    });
}

template <class F>
auto RunConcurrently(F&& operation)
{
    using TResult = std::invoke_result_t<F, size_t>;

    std::atomic<ui32> ready = 0;
    std::promise<void> startPromise;
    auto start = startPromise.get_future().share();
    auto run = [&](size_t index) {
        ready.fetch_add(1);
        start.wait();
        return operation(index);
    };

    std::array<std::future<TResult>, 2> futures = {
        std::async(std::launch::async, run, 0),
        std::async(std::launch::async, run, 1),
    };
    while (ready.load() != futures.size()) {
        std::this_thread::yield();
    }
    startPromise.set_value();

    return std::array<TResult, 2>{futures[0].get(), futures[1].get()};
}

class TConcurrentTestGraph {
public:
    explicit TConcurrentTestGraph(TIntrusivePtr<TComputationPatternHolder> patternHolder)
        : Alloc_(__LOCATION__)
        , TypeEnv_(Alloc_)
        , RandomProvider_(CreateDefaultRandomProvider())
        , TimeProvider_(CreateDefaultTimeProvider())
        , Graph_(CloneComputationGraph(
            std::move(patternHolder),
            Alloc_,
            TypeEnv_,
            *RandomProvider_,
            *TimeProvider_))
    {
        Graph_->Prepare();
        Alloc_.Release();
    }

    ~TConcurrentTestGraph()
    {
        Alloc_.Acquire();
        Graph_.Reset();
    }

    ui32 GetResult()
    {
        auto guard = Guard(Alloc_);
        return Graph_->GetValue().Get<ui32>();
    }

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc_;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv_;
    TIntrusivePtr<IRandomProvider> RandomProvider_;
    TIntrusivePtr<ITimeProvider> TimeProvider_;
    THolder<NKikimr::NMiniKQL::IComputationGraph> Graph_;
};

TEST(TFunctionRegistryHolderTest, LoadsUdfAndFindsFunctionThroughImmutableRegistry)
{
    auto holder = CreateFunctionRegistryHolder({GetTestUdfPath()});
    const NKikimr::NMiniKQL::IFunctionRegistry& registry = holder->GetFunctionRegistry();

    ASSERT_TRUE(registry.IsLoadedUdfModule("SimpleUdf"));

    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TTypeEnvironment env(alloc);
    NYql::NUdf::ITypeInfoHelper::TPtr typeInfoHelper(
        new NKikimr::NMiniKQL::TTypeInfoHelper);
    NKikimr::NMiniKQL::TFunctionTypeInfo functionInfo;
    auto runtimeSettings = MakeRuntimeSettings();
    auto status = registry.FindFunctionTypeInfo(
        UnknownLangVersion,
        *runtimeSettings,
        env,
        typeInfoHelper,
        /*countersProvider*/ nullptr,
        "SimpleUdf.Echo",
        /*userType*/ nullptr,
        /*typeConfig*/ {},
        /*flags*/ 0,
        NYql::NUdf::TSourcePosition(),
        /*secureParamsProvider*/ nullptr,
        /*logProvider*/ nullptr,
        &functionInfo);

    ASSERT_TRUE(status.IsOk()) << status.GetError();
    ASSERT_TRUE(functionInfo.FunctionType);
}

TEST(TFunctionRegistryHolderTest, FailsToCreateForMissingUdf)
{
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        CreateFunctionRegistryHolder({"missing-ytflow-test-udf.so"}),
        yexception,
        "missing-ytflow-test-udf.so");
}

TEST(TFunctionRegistryHolderTest, LegacyGraphsOwnSeparateRegistriesAndPreserveResult)
{
    TTempFileHandle lambdaFile;
    WriteTestLambda(lambdaFile);

    TTestComputationGraph first(lambdaFile.GetName());
    TTestComputationGraph second(lambdaFile.GetName());

    ASSERT_NE(first.GetRegistryIdentity(), second.GetRegistryIdentity());
    ASSERT_EQ(TestResult, first.GetResult());
    ASSERT_EQ(TestResult, second.GetResult());
}

TEST(TConcurrentPatternBuildTest, BuildsPatternsAndExecutesGraphsConcurrently)
{
    TTempFileHandle firstLambdaFile;
    TTempFileHandle secondLambdaFile;
    WriteUdfLambda(firstLambdaFile, "ConcurrentPatternBuild.First");
    WriteUdfLambda(secondLambdaFile, "ConcurrentPatternBuild.Second");

    const std::array<TString, 2> lambdaFiles = {
        firstLambdaFile.GetName(),
        secondLambdaFile.GetName(),
    };
    auto registryHolder = CreateFunctionRegistryHolder(
        {GetConcurrentPatternBuildUdfPath()});

    auto patterns = RunConcurrently([&](size_t index) {
        return BuildComputationPatternHolder(
            lambdaFiles[index],
            registryHolder,
            UnknownLangVersion,
            "OFF",
            MakeRuntimeSettings());
    });

    auto results = RunConcurrently([&](size_t index) {
        TConcurrentTestGraph graph(patterns[index]);
        return graph.GetResult();
    });

    ASSERT_EQ(1, results[0]);
    ASSERT_EQ(2, results[1]);
}

} // namespace
} // namespace NYql::NYtflow
