#include "yql_ytflow_postprocess_computation_graph_with_codecs.h"
#include "yql_ytflow_computation_graph_with_codecs_base.h"
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
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/string/join.h>
#include <util/system/guard.h>

#include <memory>
#include <optional>
#include <utility>


namespace NYql::NYtflow::NPrivate {

constexpr TStringBuf YtflowInputKeyCallableName = "YtflowInputKey";
constexpr TStringBuf YtflowInputStateCallableName = "YtflowInputState";
constexpr TStringBuf YtflowInputMaxHopStartTimeCallableName = "YtflowInputMaxHopStartTime";

class TPostprocessComputationGraphWithCodecs
    : public TComputationGraphWithCodecsBase
    , public IPostprocessComputationGraphWithCodecs
{
public:
    TPostprocessComputationGraphWithCodecs(
        TString lambdaFile,
        TVector<TOutputStreamInfo> outputStreamInfos,
        TVector<TString> udfPaths,
        TLangVersion langVersion,
        TString optLLVM,
        NYql::TRuntimeSettings::TConstPtr runtimeSettings,
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
        , OutputStreamInfos(std::move(outputStreamInfos))
        , ConverterCache(std::move(converterCache))
        , Profiler(std::move(profiler))
        , CpuToVcpuFactor(TryGetCpuToVCpuFactor())
        // TODO(ngc224): check whether so-named metrics do clash
        , OutputCodecCpuTimeCounter(Profiler.TimeCounter("/custom/output_codec/cpu_time"))
        , OutputCodecVCpuTimeCounter(Profiler.TimeCounter("/custom/output_codec/vcpu_time"))
        , OutputCodecCpuVCpuTimeCounter(
            OutputCodecCpuTimeCounter, OutputCodecVCpuTimeCounter, CpuToVcpuFactor)
    {
        auto guard = Guard(Alloc);

        InitCodecs();

        SetupProcessing();
    }

public:
    void SetInput(
        const NYT::NFlow::TKey& key,
        TString state,
        ui64 maxHopStartTime) override
    {
        auto guard = Guard(Alloc);

        const auto& compositeKey = key.Underlying().Elements();

        KeyNode->SetValue(
            ComputationGraph->GetContext(),
            // first key item is hash, hence slicing is needed
            ConvertKey(compositeKey.Slice(1, compositeKey.Size())));

        auto stateValue = NYT::NTableClient::MakeUnversionedCompositeValue(state);

        StateNode->SetValue(
            ComputationGraph->GetContext(),
            StateInputCodec->Convert(stateValue));

        MaxHopStartTimeNode->SetValue(
            ComputationGraph->GetContext(),
            NYql::NUdf::TUnboxedValuePod(maxHopStartTime));
    }

    TPostprocessOutput GetOutput() override
    {
        auto guard = Guard(Alloc);

        auto outputValue = ComputationGraph->GetValue();

        // TODO(ngc224): add validation for element count
        auto streamValue = outputValue.GetElement(0);
        auto stateValue = outputValue.GetElement(1);
        auto cleanupStateValue = outputValue.GetElement(2);

        TPostprocessOutput output;
        output.CleanupState = cleanupStateValue.Get<bool>();

        FetchMessages(streamValue, output.Messages);

        {
            auto outputCodecGuard = TSimpleTimingGuard(OutputCodecCpuVCpuTimeCounter);

            auto unversionedValue = StateOutputCodec->Convert(stateValue);

            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Composite);

            output.State = TString(unversionedValue.AsStringBuf());
        }

        StateRowBuffer->Clear();
        MessageRowBuffer->Clear();

        return output;
    }

    void ResetInput() override {
        CheckConsumedLinear();
    }

private:
    NYql::NUdf::TUnboxedValue ConvertKey(
        NYT::NTableClient::TUnversionedValueRange compositeKey)
    {
        if (compositeKey.Size() == 1) {
            return KeyInputCodec->Convert(compositeKey.Front());
        }

        return KeyInputCodec->Convert(compositeKey);
    }

    void FetchMessages(
        NYql::NUdf::TUnboxedValue streamValue,
        TVector<NYT::NFlow::TMessage>& messages)
    {
        NYql::NUdf::TUnboxedValue item;
        NYT::NTableClient::TUnversionedRow unversionedRow;

        while (true) {
            auto status = streamValue.Fetch(item);
            MKQL_ENSURE(status != NKikimr::NUdf::EFetchStatus::Yield,
                "Unexpected fetch status: " << status);

            if (status == NKikimr::NUdf::EFetchStatus::Finish) {
                break;
            }

            {
                auto outputCodecGuard = TSimpleTimingGuard(OutputCodecCpuVCpuTimeCounter);
                unversionedRow = MessageOutputCodec->Convert(std::move(item));
            }

            NYT::NFlow::TMessage message;
            message.Payload = NYT::NFlow::TPayload(
                NYT::NFlow::TCompactUnversionedOwningRow(unversionedRow));
            message.PayloadSchema = OutputSchema;

            for (const auto& outputStreamInfo : OutputStreamInfos) {
                message.StreamId = outputStreamInfo.StreamId;
                messages.push_back(message);
            }

            MessageRowBuffer->Clear();
        }
    }

    void InitCodecs() {
        MKQL_ENSURE(
            OutputType->IsTuple(),
            "Unexpected output type: " << OutputType->GetKindAsStr());

        auto outputElements = static_cast<
            const NKikimr::NMiniKQL::TTupleType*>(OutputType)->GetElements();
        MKQL_ENSURE(
            outputElements.size() == 3,
            "Unexpected output type element count: " << outputElements.size());

        const auto* streamType = outputElements.at(0);
        const auto* streamItemType = static_cast<
            const NKikimr::NMiniKQL::TStreamType*>(streamType)->GetItemType();

        const auto* keyType = InputTypes.at(TString(YtflowInputKeyCallableName));

        KeyInputCodec = CreateValueInputCodec(
            keyType,
            ConvertType(keyType),
            NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true));

        const auto* stateType = InputTypes.at(TString(YtflowInputStateCallableName));

        StateInputCodec = CreateValueInputCodec(
            stateType,
            ConvertType(stateType),
            NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true));

        MKQL_ENSURE(
            streamType->IsStream(),
            "Unexpected output messages type: " << streamType->GetKindAsStr());

        TVector<NYT::NTableClient::TTableSchemaPtr> outputSchemas;
        for (const auto& outputStreamInfo : OutputStreamInfos) {
            outputSchemas.push_back(outputStreamInfo.OutputSchema);
        }

        outputSchemas.erase(
            UniqueBy(
                outputSchemas.begin(),
                outputSchemas.end(),
                [](const auto& schema) {
                    return *schema;
                }),
            outputSchemas.end());

        MKQL_ENSURE(outputSchemas.size() == 1, "Expected one unique output schema, but got "
            << outputSchemas.size() << " (" << JoinSeq(", ", outputSchemas) << ")");

        OutputSchema = outputSchemas.front();

        MessageRowBuffer = NYT::New<NYT::NTableClient::TRowBuffer>();
        MessageOutputCodec = CreateRowOutputCodec(
            streamItemType,
            OutputSchema,
            MessageRowBuffer,
            NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true));

        StateRowBuffer = NYT::New<NYT::NTableClient::TRowBuffer>();
        StateOutputCodec = CreateValueOutputCodec(
            stateType,
            ConvertType(stateType),
            StateRowBuffer,
            NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true));
    }

    void SetupProcessing() {
        KeyNode = YtflowInputNodes.at(TString(YtflowInputKeyCallableName));
        StateNode = YtflowInputNodes.at(TString(YtflowInputStateCallableName));
        MaxHopStartTimeNode = YtflowInputNodes.at(
            TString(YtflowInputMaxHopStartTimeCallableName));
    }

private:
    TVector<TOutputStreamInfo> OutputStreamInfos;
    NYT::NFlow::IPayloadConverterCachePtr ConverterCache;
    NYT::NProfiling::TProfiler Profiler;

    std::optional<double> CpuToVcpuFactor;

    NYT::NProfiling::TTimeCounter OutputCodecCpuTimeCounter;
    NYT::NProfiling::TTimeCounter OutputCodecVCpuTimeCounter;
    TCpuVCpuTimeCounter OutputCodecCpuVCpuTimeCounter;

    NYT::NTableClient::TTableSchemaPtr OutputSchema;

    THolder<NYql::NYtflow::NCodec::IValueInputCodec> KeyInputCodec;
    THolder<NYql::NYtflow::NCodec::IValueInputCodec> StateInputCodec;
    THolder<NYql::NYtflow::NCodec::IRowOutputCodec> MessageOutputCodec;
    THolder<NYql::NYtflow::NCodec::IValueOutputCodec> StateOutputCodec;
    NYT::NTableClient::TRowBufferPtr MessageRowBuffer;
    NYT::NTableClient::TRowBufferPtr StateRowBuffer;

    NKikimr::NMiniKQL::IComputationExternalNode* KeyNode = nullptr;
    NKikimr::NMiniKQL::IComputationExternalNode* StateNode = nullptr;
    NKikimr::NMiniKQL::IComputationExternalNode* MaxHopStartTimeNode = nullptr;
};

} // namespace NYql::NYtflow::NPrivate

namespace NYql::NYtflow {

THolder<IPostprocessComputationGraphWithCodecs> CreatePostprocessComputationGraphWithCodecs(
    TString lambdaFile,
    TVector<TOutputStreamInfo> outputStreamInfos,
    TVector<TString> udfPaths,
    TLangVersion langVersion,
    TString optLLVM,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings,
    NYT::NProfiling::TProfiler profiler,
    NYT::NFlow::IPayloadConverterCachePtr converterCache,
    TComputationGraphResources resources)
{
    return MakeHolder<NPrivate::TPostprocessComputationGraphWithCodecs>(
        std::move(lambdaFile),
        std::move(outputStreamInfos),
        std::move(udfPaths),
        langVersion,
        std::move(optLLVM),
        std::move(runtimeSettings),
        std::move(profiler),
        std::move(converterCache),
        std::move(resources));
}

} // namespace NYql::NYtflow
