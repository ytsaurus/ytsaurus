#include "yql_ytflow_update_state_computation_graph_with_codecs.h"
#include "yql_ytflow_computation_graph_with_codecs_base.h"
#include "yql_ytflow_stream_value.h"
#include "yql_ytflow_timing_guard.h"
#include "yql_ytflow_utils.h"

#include <yt/yql/providers/yt/mkql_ytflow/yql_yt_ytflow_schema.h>
#include <yt/yql/providers/ytflow/codec/yql_ytflow_input_codec.h>
#include <yt/yql/providers/ytflow/codec/yql_ytflow_output_codec.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/core/profiling/timing.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/guard.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>


namespace NYql::NYtflow::NPrivate {

constexpr TStringBuf YtflowInputStreamCallableName = "YtflowInputStream";
constexpr TStringBuf YtflowInputStateCallableName = "YtflowInputState";

class TUpdateStateComputationGraphWithCodecs
    : public TComputationGraphWithCodecsBase
    , public IUpdateStateComputationGraphWithCodecs
{
public:
    TUpdateStateComputationGraphWithCodecs(
        TString lambdaFile,
        TVector<TString> udfPaths,
        TLangVersion langVersion,
        TString optLLVM,
        NYql::TRuntimeSettings::TConstPtr runtimeSettings,
        NYT::NTableClient::TTableSchemaPtr inputSchema,
        NYT::NProfiling::TProfiler profiler,
        NYT::NFlow::IPayloadConverterCachePtr converterCache,
        TComputationGraphResources resources)
        : TComputationGraphWithCodecsBase(
            std::move(lambdaFile),
            std::move(udfPaths),
            langVersion,
            std::move(optLLVM),
            std::move(runtimeSettings),
            std::move(resources),
            profiler)
        , InputSchema(std::move(inputSchema))
        , ConverterCache(std::move(converterCache))
        , Profiler(std::move(profiler))
        , CpuToVcpuFactor(TryGetCpuToVCpuFactor())
        , InputCodecCpuTimeCounter(Profiler.TimeCounter("/custom/input_codec/cpu_time"))
        , InputCodecVCpuTimeCounter(Profiler.TimeCounter("/custom/input_codec/vcpu_time"))
        , InputCodecCpuVCpuTimeCounter(
            InputCodecCpuTimeCounter, InputCodecVCpuTimeCounter, CpuToVcpuFactor)
    {
        auto guard = Guard(Alloc);

        InitCodecs();

        SetupProcessing();
    }

    ~TUpdateStateComputationGraphWithCodecs()
    {
        auto guard = Guard(Alloc);
        ValueFetcher.Reset();
    }

public:
    void SetInput(
        const std::vector<TMessageHolder>& messageHolders,
        std::optional<TString> maybeState) override
    {
        auto guard = Guard(Alloc);

        ValueFetcher->SetInput(messageHolders);

        TString state = maybeState
            ? std::move(*maybeState)
            : TString("[];");

        auto stateValue = NYT::NTableClient::MakeUnversionedCompositeValue(state);

        StateNode->SetValue(
            ComputationGraph->GetContext(),
            StateInputCodec->Convert(stateValue));
    }

    TUpdateStateOutput GetOutput() override
    {
        auto guard = Guard(Alloc);

        auto outputValue = ComputationGraph->GetValue();

        // TODO(ngc224): add validation for element count
        auto stateValue = outputValue.GetElement(0);
        auto timerTimestampsValue = outputValue.GetElement(1);

        auto unversionedValue = OutputCodec->Convert(stateValue);

        YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Composite);

        auto updateStateOutput = TUpdateStateOutput{
            .State = TString(unversionedValue.AsStringBuf()),
        };

        RowBuffer->Clear();

        auto timerTimestampsIterator = timerTimestampsValue.GetListIterator();

        NKikimr::NUdf::TUnboxedValue item;

        while (timerTimestampsIterator.Next(item)) {
            auto& timerInfo = updateStateOutput.TimerInfos.emplace_back();

            timerInfo.TriggerTimestamp = TDuration::MicroSeconds(
                item.GetElement(0).Get<ui64>())
                .Seconds();

            timerInfo.EventTimestamp = TDuration::MicroSeconds(
                item.GetElement(1).Get<ui64>())
                .Seconds();
        }

        return updateStateOutput;
    }

    void ResetInput() override {
        MKQL_ENSURE(
            !ValueFetcher->HasMore(),
            "Input values are not fully consumed");

        CheckConsumedLinear();

        StreamValue->Reset();
    }

private:
    const NKikimr::NMiniKQL::TType* GetStreamType() const {
        return InputTypes.at(TString(YtflowInputStreamCallableName));
    }

    const NKikimr::NMiniKQL::TType* GetStateType() const {
        return InputTypes.at(TString(YtflowInputStateCallableName));
    }

    void InitCodecs() {
        MKQL_ENSURE(
            OutputType->GetKind() == NKikimr::NMiniKQL::TTypeBase::EKind::Tuple,
            "Unexpected update state output type: " << OutputType->GetKindAsStr());

        auto* outputTupleType = static_cast<const NKikimr::NMiniKQL::TTupleType*>(
            OutputType);

        MKQL_ENSURE(
            outputTupleType->GetElementsCount() == 2,
            "Unexpected update state output tuple type size: " <<
                outputTupleType->GetElementsCount());

        auto* outputStateType = outputTupleType->GetElementType(0);

        MKQL_ENSURE(
            GetStateType()->IsSameType(*outputStateType),
            "Input state type does not match output state type");

        auto* streamItemType = static_cast<const NKikimr::NMiniKQL::TStreamType*>(
            GetStreamType())->GetItemType();

        StreamInputCodec = CreateRowInputCodec(
            streamItemType,
            InputSchema,
            NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true));

        StateInputCodec = CreateValueInputCodec(
            GetStateType(),
            ConvertType(GetStateType()),
            NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true));

        RowBuffer = NYT::New<NYT::NTableClient::TRowBuffer>();
        OutputCodec = CreateValueOutputCodec(
            GetStateType(),
            ConvertType(GetStateType()),
            RowBuffer,
            NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true));
    }

    void SetupProcessing() {
        auto* streamNode = YtflowInputNodes.at(TString(YtflowInputStreamCallableName));
        StateNode = YtflowInputNodes.at(TString(YtflowInputStateCallableName));

        ValueFetcher = MakeHolder<TMessageSequenceValueFetcher>(
            InputSchema, StreamInputCodec.Get(), Profiler, ConverterCache);

        streamNode->SetValue(
            ComputationGraph->GetContext(),
            ComputationGraph->GetHolderFactory().Create<TStreamValue>(
                ValueFetcher.Get(), EInputMode::MessageSequenceWithFinish, ComputationGraph.Get()));

        StreamValue = static_cast<TStreamValue*>(
            streamNode->GetValue(ComputationGraph->GetContext())
                .AsBoxed().Get());
    }

private:
    NYT::NTableClient::TTableSchemaPtr InputSchema;
    NYT::NTableClient::TLogicalTypePtr YtInputType;
    NYT::NTableClient::TLogicalTypePtr YtOutputType;
    NYT::NFlow::IPayloadConverterCachePtr ConverterCache;
    NYT::NProfiling::TProfiler Profiler;

    std::optional<double> CpuToVcpuFactor;

    NYT::NProfiling::TTimeCounter InputCodecCpuTimeCounter;
    NYT::NProfiling::TTimeCounter InputCodecVCpuTimeCounter;
    TCpuVCpuTimeCounter InputCodecCpuVCpuTimeCounter;

    THolder<NYql::NYtflow::NCodec::IRowInputCodec> StreamInputCodec;
    THolder<NYql::NYtflow::NCodec::IValueInputCodec> StateInputCodec;
    THolder<NYql::NYtflow::NCodec::IValueOutputCodec> OutputCodec;
    NYT::NTableClient::TRowBufferPtr RowBuffer;

    THolder<TMessageSequenceValueFetcher> ValueFetcher;
    TStreamValue* StreamValue = nullptr;
    NKikimr::NMiniKQL::IComputationExternalNode* StateNode = nullptr;
};

} // namespace NYql::NYtflow::NPrivate

namespace NYql::NYtflow {

THolder<IUpdateStateComputationGraphWithCodecs> CreateUpdateStateComputationGraphWithCodecs(
    TString lambdaFile,
    TVector<TString> udfPaths,
    TLangVersion langVersion,
    TString optLLVM,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings,
    NYT::NTableClient::TTableSchemaPtr inputSchema,
    NYT::NProfiling::TProfiler profiler,
    NYT::NFlow::IPayloadConverterCachePtr converterCache,
    TComputationGraphResources resources)
{
    return MakeHolder<NPrivate::TUpdateStateComputationGraphWithCodecs>(
        std::move(lambdaFile),
        std::move(udfPaths),
        langVersion,
        std::move(optLLVM),
        std::move(runtimeSettings),
        std::move(inputSchema),
        std::move(profiler),
        std::move(converterCache),
        std::move(resources));
}

} // namespace NYql::NYtflow
