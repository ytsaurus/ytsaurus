#pragma once

#include "yql_ytflow_computation_pattern.h"
#include "yql_ytflow_function_registry.h"
#include "yql_ytflow_metrics.h"
#include "yql_ytflow_node_factory.h"

#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <yt/yql/providers/ytflow/codec/yql_ytflow_convert_options.h>
#include <yt/yql/providers/ytflow/codec/yql_ytflow_input_codec.h>
#include <yt/yql/providers/ytflow/codec/yql_ytflow_output_codec.h>
#include <yt/yql/providers/ytflow/integration/mkql_interface/yql_ytflow_lookup_provider.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/flow/library/cpp/resources/public.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>


namespace NYql::NYtflow {

class TComputationPatternResult;

struct TComputationGraphResources {
    TIntrusivePtr<TComputationPatternHolder> PatternHolder;
    TIntrusivePtr<TFunctionRegistryHolder> FunctionRegistryHolder;
    TMaybe<EComputationPatternUnsuitabilityReason> PatternUnsuitabilityReason;
};

TComputationGraphResources MakeComputationGraphResources(
    const TComputationPatternResult& patternResult,
    TIntrusivePtr<TFunctionRegistryHolder> functionRegistryHolder);

TComputationGraphResources ResolveComputationGraphResources(
    const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& staticResources,
    TStringBuf computationPatternResourceAlias);

class TComputationGraphWithCodecsBase
{
public:
    TComputationGraphWithCodecsBase(
        TString lambdaFile,
        TVector<TString> udfPaths,
        TLangVersion langVersion,
        TString optLLVM,
        NYql::TRuntimeSettings::TConstPtr runtimeSettings,
        TComputationGraphResources resources = {},
        NYT::NProfiling::TProfiler profiler = {});

    ~TComputationGraphWithCodecsBase();

protected:
    THolder<NYql::NYtflow::NCodec::IRowInputCodec> CreateRowInputCodec(
        const NKikimr::NMiniKQL::TType* type,
        NYT::NTableClient::TTableSchemaPtr ytSchema,
        const NYql::NYtflow::NCodec::TConvertOptions& convertOptions = {});

    THolder<NYql::NYtflow::NCodec::IValueInputCodec> CreateValueInputCodec(
        const NKikimr::NMiniKQL::TType* type,
        NYT::NTableClient::TLogicalTypePtr ytType,
        const NYql::NYtflow::NCodec::TConvertOptions& convertOptions = {});

    THolder<NYql::NYtflow::NCodec::IRowOutputCodec> CreateRowOutputCodec(
        const NKikimr::NMiniKQL::TType* type,
        NYT::NTableClient::TTableSchemaPtr ytSchema,
        NYT::NTableClient::TRowBufferPtr rowBuffer,
        const NYql::NYtflow::NCodec::TConvertOptions& convertOptions = {});

    THolder<NYql::NYtflow::NCodec::IValueOutputCodec> CreateValueOutputCodec(
        const NKikimr::NMiniKQL::TType* type,
        NYT::NTableClient::TLogicalTypePtr ytType,
        NYT::NTableClient::TRowBufferPtr rowBuffer,
        const NYql::NYtflow::NCodec::TConvertOptions& convertOptions = {});

    void CheckConsumedLinear();

private:
    void InitComputationGraph(
        TStringBuf lambdaFile,
        NYql::TLangVersion langVer,
        const TString& optLLVM);

protected:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;
    NKikimr::NMiniKQL::TTypeBuilder TypeBuilder;
    NKikimr::NMiniKQL::TMemoryUsageInfo MemUsage;
    NKikimr::NMiniKQL::THolderFactory HolderFactory;

    THolder<NYql::NUdf::IValueBuilder> ValueBuilder;
    NYql::NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    NYql::TRuntimeSettings::TConstPtr RuntimeSettings;
    NYql::NUdf::IFunctionTypeInfoBuilderPtr FunctionTypeInfoBuilder;
    TComputationGraphMetrics Metrics;

    TIntrusivePtr<TFunctionRegistryHolder> FunctionRegistryHolder;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    THolder<IYtflowLookupProviderRegistry> YtflowLookupProviderRegistry;
    std::unique_ptr<NUdf::ISecureParamsProvider> SecureParamsProvider;

    THashMap<TString, const NKikimr::NMiniKQL::TType*> InputTypes;
    THashMap<TString, NKikimr::NMiniKQL::IComputationExternalNode*> YtflowInputNodes;

    const NKikimr::NMiniKQL::TType* OutputType = nullptr;

    THolder<NKikimr::NMiniKQL::IComputationGraph> ComputationGraph;
};

} // namespace NYql::NYtflow
