#include "yql_ytflow_update_state_computation_graph_with_codecs.h"
#include "yql_ytflow_postprocess_computation_graph_with_codecs.h"
#include "yql_ytflow_common_parameters.h"
#include "yql_ytflow_message_holder.h"
#include "yql_ytflow_timing_guard.h"
#include "yql_ytflow_utils.h"

#include <yt/yql/providers/ytflow/common/yql_ytflow_constants.h>

#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>


namespace NYql::NYtflow {

namespace {

struct THoppingAggregateParameters
    : public NYT::NFlow::TTransformComputation::TParameters
    , public TCommonOperationParameters
{
    TString UpdateStateLambdaFile;
    TString PostprocessLambdaFile;

    ui64 Interval;
    ui64 Delay;

    REGISTER_YSON_STRUCT(THoppingAggregateParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("update_state_lambda_file", &TThis::UpdateStateLambdaFile);
        registrar.Parameter("postprocess_lambda_file", &TThis::PostprocessLambdaFile);

        registrar.Parameter("interval", &TThis::Interval);
        registrar.Parameter("delay", &TThis::Delay);
    }
};

struct TAggregationState
    : public NYT::NYTree::TYsonStruct
{
    std::optional<TString> Frames;
    std::optional<ui64> Version;
    std::optional<ui64> FormatVersion;

    REGISTER_YSON_STRUCT(TAggregationState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("frames", &TThis::Frames)
            .Default();

        registrar.Parameter("version", &TThis::Version)
            .Default(0);

        registrar.Parameter("format_version", &TThis::FormatVersion)
            .Default(0);
    }
};

} // anonymous namespace

class THoppingAggregate
    : public NYT::NFlow::TTransformComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(THoppingAggregateParameters);

    THoppingAggregate(
        NYT::NFlow::TComputationContextPtr context,
        NYT::NFlow::TDynamicComputationContextPtr dynamicContext)

        : NYT::NFlow::TTransformComputation(std::move(context), std::move(dynamicContext))
        , CpuToVCpuFactor(TryGetCpuToVCpuFactor())
        , ProcessCpuTimeCounter(GetContext()->Profiler.TimeCounter("/custom/process/cpu_time"))
        , ProcessVCpuTimeCounter(GetContext()->Profiler.TimeCounter("/custom/process/vcpu_time"))
        , ProcessCpuVCpuTimeCounter(
            ProcessCpuTimeCounter, ProcessVCpuTimeCounter, CpuToVCpuFactor)
        , InputMessagesCounter(GetContext()->Profiler.Counter("/custom/process/input_messages"))
        , OutputMessagesCounter(GetContext()->Profiler.Counter("/custom/process/output_messages"))
    {
        NKikimr::NMiniKQL::TThrowingBindTerminator bindTerminator;

        const auto& inputStreamIds = GetSpec()->InputStreamIds;
        YQL_ENSURE(inputStreamIds.size() == 1);

        const auto& streamSpecStorage = GetContext()->StreamSpecStorage;

        auto sourceSchema = streamSpecStorage->GetSchema(*inputStreamIds.begin());

        const auto& parameters = GetParameters();

        TriggerTimestampToFrameStartDelta = TDuration::MicroSeconds(
            parameters->Interval + parameters->Delay)
            .Seconds();

        const auto& outputIndicesByOutputStreamId = parameters->OutputIndicesByOutputStreamId;

        TVector<TOutputStreamInfo> outputStreamInfos;
        for (const auto& [outputStreamId, outputIndex] : outputIndicesByOutputStreamId) {
            outputStreamInfos.push_back(
                TOutputStreamInfo{
                    .StreamId = outputStreamId,
                    .OutputSchema = streamSpecStorage->GetSchema(outputStreamId),
                });
        }

        auto runtimeSettings = parameters->RuntimeSettings.empty()
            ? MakeRuntimeSettings()
            : CreateRuntimeSettingsFromString(parameters->RuntimeSettings);

        auto updateStateResources = ResolveComputationGraphResources(
            GetContext()->StaticResources,
            UpdateStateComputationPatternResourceAlias);
        auto postprocessResources = ResolveComputationGraphResources(
            GetContext()->StaticResources,
            PostprocessComputationPatternResourceAlias);

        UpdateStateComputationGraphWithCodecs = CreateUpdateStateComputationGraphWithCodecs(
            parameters->UpdateStateLambdaFile,
            parameters->UdfPaths,
            parameters->LangVersion,
            parameters->OptLLVM,
            runtimeSettings,
            std::move(sourceSchema),
            GetContext()->Profiler,
            GetContext()->ConverterCache,
            std::move(updateStateResources));

        PostprocessComputationGraphWithCodecs = CreatePostprocessComputationGraphWithCodecs(
            parameters->PostprocessLambdaFile,
            outputStreamInfos,
            parameters->UdfPaths,
            parameters->LangVersion,
            parameters->OptLLVM,
            std::move(runtimeSettings),
            GetContext()->Profiler,
            GetContext()->ConverterCache,
            std::move(postprocessResources));
    }

public:
    void DoInit(NYT::NFlow::IJobInitContextPtr initContext) override
    {
        initContext->InitClient(AggregationStateClient, "aggregation_state");
    }

    void DoProcessKey(
        NYT::NFlow::IInputContextPtr input,
        NYT::NFlow::IOutputCollectorPtr output) override
    {
        auto processGuard = TSimpleTimingGuard(ProcessCpuVCpuTimeCounter);

        NKikimr::NMiniKQL::TThrowingBindTerminator bindTerminator;

        std::vector<TMessageHolder> messageHolders;
        messageHolders.reserve(input->GetMessages().size());

        const auto& messages = input->GetMessages();

        for (const auto& message : messages) {
            messageHolders.push_back(TMessageHolder(message));
        }

        if (!messageHolders.empty()) {
            const auto& key = messages[0]->Key;

            auto aggregationState = AggregationStateClient.GetState(key);

            UpdateStateComputationGraphWithCodecs->SetInput(
                messageHolders,
                aggregationState->Frames);

            auto updateStateOutput = UpdateStateComputationGraphWithCodecs->GetOutput();

            aggregationState->Frames = std::move(updateStateOutput.State);

            for (const auto& timerInfo : updateStateOutput.TimerInfos) {
                output->AddTimer(
                    NYT::NFlow::TSystemTimestamp(timerInfo.TriggerTimestamp),
                    NYT::NFlow::TSystemTimestamp(timerInfo.EventTimestamp));
            }

            UpdateStateComputationGraphWithCodecs->ResetInput();
        }

        for (const auto& timer : input->GetTimers()) {
            DoProcessTimer(*timer, output->SetParents({}, {timer}, {}));
        }
    }

    void DoProcessTimer(
        const NYT::NFlow::TTimer& timer,
        NYT::NFlow::IOutputCollectorPtr output) override
    {
        NKikimr::NMiniKQL::TThrowingBindTerminator bindTerminator;

        auto aggregationState = AggregationStateClient.GetState(timer.Key);

        YT_VERIFY(
            aggregationState->Frames,
            "Unexpected empty per-key aggregation state on timer trigger");

        auto processGuard = TSimpleTimingGuard(ProcessCpuVCpuTimeCounter);

        // all timestamps in YQL are in milliseconds, and flow works with seconds
        PostprocessComputationGraphWithCodecs->SetInput(
            timer.Key,
            *aggregationState->Frames,
            TDuration::Seconds(
                timer.TriggerTimestamp.Underlying() - TriggerTimestampToFrameStartDelta)
                .MicroSeconds());

        auto postprocessOutput = PostprocessComputationGraphWithCodecs->GetOutput();

        PostprocessComputationGraphWithCodecs->ResetInput();

        for (auto& message : postprocessOutput.Messages) {
            output->AddMessage(std::move(message));
        }

        if (postprocessOutput.CleanupState) {
            aggregationState.Clear();
        } else {
            aggregationState->Frames = postprocessOutput.State;
        }
    }

private:
    ui64 TriggerTimestampToFrameStartDelta = 0;

    THolder<IUpdateStateComputationGraphWithCodecs> UpdateStateComputationGraphWithCodecs;
    THolder<IPostprocessComputationGraphWithCodecs> PostprocessComputationGraphWithCodecs;

    NYT::NFlow::TMutableStateKeyClient<TAggregationState> AggregationStateClient;

    std::optional<double> CpuToVCpuFactor;

    NYT::NProfiling::TTimeCounter ProcessCpuTimeCounter;
    NYT::NProfiling::TTimeCounter ProcessVCpuTimeCounter;
    TCpuVCpuTimeCounter ProcessCpuVCpuTimeCounter;

    NYT::NProfiling::TCounter InputMessagesCounter;
    NYT::NProfiling::TCounter OutputMessagesCounter;
};

YT_FLOW_DEFINE_COMPUTATION(THoppingAggregate);

} // namespace NYql::NYtflow
