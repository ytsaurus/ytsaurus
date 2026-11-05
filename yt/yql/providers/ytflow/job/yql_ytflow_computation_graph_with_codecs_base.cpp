#include "yql_ytflow_computation_graph_with_codecs_base.h"

#include "yql_ytflow_computation_pattern.h"
#include "yql_ytflow_computation_pattern_resource.h"
#include "yql_ytflow_function_registry_resource.h"
#include "yql_ytflow_metrics.h"
#include "yql_ytflow_node_factory.h"
#include "yql_ytflow_secure_params.h"

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yql/providers/yt/mkql_ytflow/yql_yt_ytflow_lookup_provider.h>
#include <yt/yql/providers/ytflow/integration/mkql_interface/yql_ytflow_lookup_provider.h>
#include <yt/yql/providers/ytflow/lambda_builder/yql_ytflow_lambda_builder.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <util/generic/string.h>
#include <util/stream/file.h>


namespace NYql::NYtflow {

TComputationGraphWithCodecsBase::TComputationGraphWithCodecsBase(
    TString lambdaFile,
    TVector<TString> udfPaths,
    TLangVersion langVersion,
    TString optLLVM,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings,
    TComputationGraphResources resources,
    NYT::NProfiling::TProfiler profiler)
    : Alloc(__LOCATION__)
    , TypeEnv(Alloc)
    , TypeBuilder(TypeEnv)
    , MemUsage("TComputationGraphWithCodecs")
    , HolderFactory(Alloc.Ref(), MemUsage)
    , ValueBuilder(MakeHolder<NKikimr::NMiniKQL::TDefaultValueBuilder>(HolderFactory))
    , TypeInfoHelper(new NKikimr::NMiniKQL::TTypeInfoHelper{})
    , RuntimeSettings(std::move(runtimeSettings))
    , FunctionTypeInfoBuilder(
        new NKikimr::NMiniKQL::TFunctionTypeInfoBuilder(
            langVersion, *RuntimeSettings, TypeEnv, TypeInfoHelper, "TComputationGraphWithCodecs", nullptr,
            NYql::NUdf::TSourcePosition()))
    , Metrics(profiler)
    , RandomProvider(CreateDefaultRandomProvider())
    , TimeProvider(CreateDefaultTimeProvider())
{
    if (resources.PatternUnsuitabilityReason) {
        Metrics.RecordFallback(*resources.PatternUnsuitabilityReason);
    }

    if (resources.PatternHolder) {
        // FunctionRegistryHolder intentionally stays empty: the cloned graph retains
        // the pattern holder, which transitively owns the function registry.
    } else if (resources.FunctionRegistryHolder) {
        FunctionRegistryHolder = std::move(resources.FunctionRegistryHolder);
    } else {
        FunctionRegistryHolder = CreateFunctionRegistryHolder(std::move(udfPaths));
    }

    if (resources.PatternHolder) {
        InputTypes = resources.PatternHolder->GetInputTypes();
        YtflowInputNodes = resources.PatternHolder->GetYtflowInputNodes();
        OutputType = resources.PatternHolder->GetOutputType();
        auto cloneGuard = Metrics.ProfileClone();
        ComputationGraph = CloneComputationGraph(
            std::move(resources.PatternHolder),
            Alloc,
            TypeEnv,
            *RandomProvider,
            *TimeProvider);
    } else {
        YtflowLookupProviderRegistry = CreateYtflowLookupProviderRegistry();
        RegisterYtYtflowLookupProvider(*YtflowLookupProviderRegistry);

        SecureParamsProvider = CreateSecureParamsProvider();
        InitComputationGraph(lambdaFile, langVersion, optLLVM);
    }

    {
        auto prepareGuard = Metrics.ProfilePrepare();
        ComputationGraph->Prepare();
    }

    Alloc.Release();
}

TComputationGraphResources MakeComputationGraphResources(
    const TComputationPatternResult& patternResult,
    TIntrusivePtr<TFunctionRegistryHolder> functionRegistryHolder)
{
    YQL_ENSURE(
        functionRegistryHolder,
        "Computation pattern result requires function registry holder");
    TComputationGraphResources result{
        .FunctionRegistryHolder = std::move(functionRegistryHolder),
    };
    if (patternResult.IsSuitable()) {
        result.PatternHolder = patternResult.GetPatternHolder();
    } else {
        result.PatternUnsuitabilityReason =
            patternResult.GetUnsuitabilityReason();
    }
    return result;
}

TComputationGraphResources ResolveComputationGraphResources(
    const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& staticResources,
    TStringBuf computationPatternResourceAlias)
{
    TIntrusivePtr<TFunctionRegistryHolder> functionRegistryHolder;
    if (const auto it = staticResources.find(
            NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias));
        it != staticResources.end())
    {
        functionRegistryHolder =
            it->second->As<TFunctionRegistryResource>()->GetFunctionRegistryHolder();
    }

    // Resource dependencies are not propagated to computation static resources.
    // A computation receiving a pattern must require the registry directly for fallback.
    if (const auto it = staticResources.find(
            NYT::NFlow::TResourceId(computationPatternResourceAlias));
        it != staticResources.end())
    {
        YQL_ENSURE(
            functionRegistryHolder,
            "Computation with a pattern resource requires a direct function registry resource");
        return MakeComputationGraphResources(
            it->second->As<TComputationPatternResource>()->GetResult(),
            std::move(functionRegistryHolder));
    }

    return TComputationGraphResources{
        .FunctionRegistryHolder = std::move(functionRegistryHolder),
    };
}

TComputationGraphWithCodecsBase::~TComputationGraphWithCodecsBase()
{
    Alloc.Acquire();

    ComputationGraph.Reset();
}

void TComputationGraphWithCodecsBase::InitComputationGraph(
    TStringBuf lambdaFile,
    NYql::TLangVersion langVer,
    const TString& optLLVM)
{
    auto serializedNode = TFileInput(TString(lambdaFile)).ReadAll();

    auto lambdaBuilder = NYql::NYtflow::TYtflowLambdaBuilder(
        &FunctionRegistryHolder->GetFunctionRegistry(),
        Alloc,
        &TypeEnv,
        RandomProvider,
        TimeProvider,
        /*jobStats*/ nullptr,
        /*counters*/ nullptr,
        SecureParamsProvider.get(),
        /*logProvider*/ nullptr,
        langVer,
        RuntimeSettings);

    auto rootNode = lambdaBuilder.Deserialize(serializedNode);

    const auto nodeFactoryMetadata = TNodeFactoryMetadata{
        .YtflowLookupProviderRegistry = *YtflowLookupProviderRegistry,
    };
    TNodeFactoryResult nodeFactoryResult;

    auto compositeNodeFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
        ::NYql::NYtflow::GetNodeFactory(nodeFactoryMetadata, nodeFactoryResult)
    });

    NKikimr::NMiniKQL::TExploringNodeVisitor explorer;

    // Fallback execution builds the pattern and graph in the same allocator;
    // suitable resources clone graphs from a separately allocated shared pattern.
    ComputationGraph = lambdaBuilder.BuildGraph(
        std::move(compositeNodeFactory),
        NKikimr::NUdf::EValidateMode::None,
        NKikimr::NUdf::EValidatePolicy::Fail,
        optLLVM,
        NKikimr::NMiniKQL::EGraphPerProcess::Multi,
        explorer,
        rootNode);

    InputTypes = std::move(nodeFactoryResult.InputTypes);
    YtflowInputNodes = std::move(nodeFactoryResult.YtflowInputNodes);

    OutputType = rootNode.GetStaticType();
}

THolder<NYql::NYtflow::NCodec::IRowInputCodec>
TComputationGraphWithCodecsBase::CreateRowInputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TTableSchemaPtr ytSchema,
    const NYql::NYtflow::NCodec::TConvertOptions& convertOptions)
{
    return NYql::NYtflow::NCodec::CreateRowInputCodec(
        type,
        std::move(ytSchema),
        *ValueBuilder,
        *FunctionTypeInfoBuilder,
        convertOptions);
}

THolder<NYql::NYtflow::NCodec::IValueInputCodec>
TComputationGraphWithCodecsBase::CreateValueInputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TLogicalTypePtr ytType,
    const NYql::NYtflow::NCodec::TConvertOptions& convertOptions)
{
    return NYql::NYtflow::NCodec::CreateValueInputCodec(
        type,
        std::move(ytType),
        *ValueBuilder,
        *FunctionTypeInfoBuilder,
        convertOptions);
}

THolder<NYql::NYtflow::NCodec::IRowOutputCodec>
TComputationGraphWithCodecsBase::CreateRowOutputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TTableSchemaPtr ytSchema,
    NYT::NTableClient::TRowBufferPtr rowBuffer,
    const NYql::NYtflow::NCodec::TConvertOptions& convertOptions)
{
    return NYql::NYtflow::NCodec::CreateRowOutputCodec(
        type,
        std::move(ytSchema),
        std::move(rowBuffer),
        convertOptions);
}

THolder<NYql::NYtflow::NCodec::IValueOutputCodec>
TComputationGraphWithCodecsBase::CreateValueOutputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TLogicalTypePtr ytType,
    NYT::NTableClient::TRowBufferPtr rowBuffer,
    const NYql::NYtflow::NCodec::TConvertOptions& convertOptions)
{
    return NYql::NYtflow::NCodec::CreateValueOutputCodec(
        type,
        std::move(ytType),
        std::move(rowBuffer),
        convertOptions);
}

void TComputationGraphWithCodecsBase::CheckConsumedLinear()
{
    if (auto position = ComputationGraph->GetNotConsumedLinear()) {
        MKQL_ENSURE(
            false,
            *position << " Linear value is not consumed");
    }
}

} // namespace NYql::NYtflow
