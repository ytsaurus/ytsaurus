#include "yql_ytflow_map_computation_graph_with_codecs.h"
#include "yql_ytflow_common_parameters.h"
#include "yql_ytflow_timing_guard.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yql/providers/ytflow/job/yql_ytflow_source_transformer.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NYql::NYtflow {

namespace {

struct TSourceParameters
    : public NYT::NFlow::TSwiftOrderedSourceComputation::TParameters
    , public TCommonMapParameters
{
    NYT::NTableClient::TTableSchemaPtr SourceSchema;

    REGISTER_YSON_STRUCT(TSourceParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("source_schema", &TThis::SourceSchema);
    }
};

} // anonymous namespace

class TSourceMap
    : public NYT::NFlow::TSwiftOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TSourceParameters);

    TSourceMap(
        NYT::NFlow::TComputationContextPtr context,
        NYT::NFlow::TDynamicComputationContextPtr dynamicContext)

        : NYT::NFlow::TSwiftOrderedSourceComputation(std::move(context), std::move(dynamicContext))
        , CpuToVCpuFactor(TryGetCpuToVCpuFactor())
        , ProcessCpuTimeCounter(GetContext()->Profiler.TimeCounter("/custom/process/cpu_time"))
        , ProcessVCpuTimeCounter(GetContext()->Profiler.TimeCounter("/custom/process/vcpu_time"))
        , ProcessCpuVCpuTimeCounter(
            ProcessCpuTimeCounter, ProcessVCpuTimeCounter, CpuToVCpuFactor)
        , InputMessagesCounter(GetContext()->Profiler.Counter("/custom/process/input_messages"))
        , OutputMessagesCounter(GetContext()->Profiler.Counter("/custom/process/output_messages"))
    {
        NKikimr::NMiniKQL::TThrowingBindTerminator bindTerminator;

        const auto& parameters = GetParameters();

        const auto& sourceStreams = GetSpec()->SourceStreams;
        YQL_ENSURE(sourceStreams.size() == 1);
        const auto& sourceParameters = sourceStreams.begin()->second->Parameters;
        auto sourceType = sourceParameters->GetChildValueOrThrow<ESourceType>("source_type");
        SourceTransformer = CreateSourceTransformer(sourceType);
        SourceSchema = parameters->SourceSchema;

        const auto& outputIndicesByOutputStreamId = parameters->OutputIndicesByOutputStreamId;

        const auto& streamSpecStorage = GetContext()->StreamSpecStorage;

        THashMap<ui32, TVector<TOutputStreamInfo>> outputStreamInfosByOutputIndex;
        for (const auto& [outputStreamId, outputIndex] : outputIndicesByOutputStreamId) {
            outputStreamInfosByOutputIndex[FromString<ui32>(outputIndex)].push_back(
                TOutputStreamInfo{
                    .StreamId = outputStreamId,
                    .OutputSchema = streamSpecStorage->GetSchema(outputStreamId),
                });
        }

        auto runtimeSettings = parameters->RuntimeSettings.empty()
            ? MakeRuntimeSettings()
            : CreateRuntimeSettingsFromString(parameters->RuntimeSettings);

        MapComputationGraphWithCodecs = CreateMapComputationGraphWithCodecs(
            parameters->LambdaFile,
            parameters->SourceSchema,
            std::move(outputStreamInfosByOutputIndex),
            parameters->UdfPaths,
            EInputMode::SingleMessage,
            parameters->LangVersion,
            parameters->OptLLVM,
            std::move(runtimeSettings),
            parameters->InjectInputMessageId,
            GetContext()->Profiler,
            GetContext()->ConverterCache,
            ResolveMapComputationGraphResources(GetContext()->StaticResources));
    }

public:
    void DoProcessMessage(
        const NYT::NFlow::TInputMessageConstPtr& message,
        NYT::NFlow::IOutputCollectorPtr output) override
    {
        auto processGuard = TSimpleTimingGuard(ProcessCpuVCpuTimeCounter);
        InputMessagesCounter.Increment();

        NKikimr::NMiniKQL::TThrowingBindTerminator bindTerminator;

        auto messageHolder = SourceTransformer->Transform(message, SourceSchema);

        MapComputationGraphWithCodecs->SetInput(messageHolder);

        TVector<NYT::NFlow::TMessage> outputMessages;
        ui64 outputMessageCount = 0;

        while (MapComputationGraphWithCodecs->FetchOutput(outputMessages)) {
            for (auto& outputMessage : outputMessages) {
                output->AddMessage(std::move(outputMessage));
                ++outputMessageCount;
            }
            outputMessages.clear();
        }

        OutputMessagesCounter.Increment(outputMessageCount);

        MapComputationGraphWithCodecs->ResetInput();
    }

private:
    THolder<IMapComputationGraphWithCodecs> MapComputationGraphWithCodecs;
    THolder<ISourceTransformer> SourceTransformer;
    NYT::NTableClient::TTableSchemaPtr SourceSchema;

    std::optional<double> CpuToVCpuFactor;

    NYT::NProfiling::TTimeCounter ProcessCpuTimeCounter;
    NYT::NProfiling::TTimeCounter ProcessVCpuTimeCounter;
    TCpuVCpuTimeCounter ProcessCpuVCpuTimeCounter;

    NYT::NProfiling::TCounter InputMessagesCounter;
    NYT::NProfiling::TCounter OutputMessagesCounter;
};

YT_FLOW_DEFINE_COMPUTATION(TSourceMap);

} // namespace NYql::NYtflow
