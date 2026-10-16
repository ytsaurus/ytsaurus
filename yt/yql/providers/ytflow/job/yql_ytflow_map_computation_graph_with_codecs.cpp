#include "yql_ytflow_map_computation_graph_with_codecs.h"
#include "yql_ytflow_computation_pattern_resource.h"
#include "yql_ytflow_stream_value.h"
#include "yql_ytflow_timing_guard.h"
#include "yql_ytflow_utils.h"

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/yt/memory/new.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <yt/yql/providers/ytflow/codec/yql_ytflow_input_codec.h>
#include <yt/yql/providers/ytflow/codec/yql_ytflow_output_codec.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/string/join.h>

#include <util/system/guard.h>

#include <vector>


namespace NYql::NYtflow::NPrivate {

namespace {

constexpr TStringBuf YtflowInputStreamCallableName = "YtflowInputStream";

} // namespace

class TMapComputationGraphWithCodecs
    : public TComputationGraphWithCodecsBase
    , public IMapComputationGraphWithCodecs
{
public:
    TMapComputationGraphWithCodecs(
        TString lambdaFile,
        NYT::NTableClient::TTableSchemaPtr inputSchema,
        THashMap<ui32, TVector<TOutputStreamInfo>> outputStreamInfosByOutputIndex,
        TVector<TString> udfPaths,
        EInputMode inputMode,
        TLangVersion langVersion,
        TString optLLVM,
        NYql::TRuntimeSettings::TConstPtr runtimeSettings,
        bool injectInputMessageId,
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
        , OutputStreamInfosByOutputIndex(std::move(outputStreamInfosByOutputIndex))
        , InputMode(inputMode)
        , InjectInputMessageId(injectInputMessageId)
        , ConverterCache(std::move(converterCache))
        , Profiler(std::move(profiler))
        , CpuToVcpuFactor(TryGetCpuToVCpuFactor())
        , FetchOutputCpuTimeCounter(Profiler.TimeCounter("/custom/fetch_output/cpu_time"))
        , FetchOutputVCpuTimeCounter(Profiler.TimeCounter("/custom/fetch_output/vcpu_time"))
        , FetchOutputCpuVCpuTimeCounter(
            FetchOutputCpuTimeCounter, FetchOutputVCpuTimeCounter, CpuToVcpuFactor)
        , OutputCodecCpuTimeCounter(Profiler.TimeCounter("/custom/output_codec/cpu_time"))
        , OutputCodecVCpuTimeCounter(Profiler.TimeCounter("/custom/output_codec/vcpu_time"))
        , OutputCodecCpuVCpuTimeCounter(
            OutputCodecCpuTimeCounter, OutputCodecVCpuTimeCounter, CpuToVcpuFactor)
    {
        auto guard = Guard(Alloc);

        InitCodecs();

        SetupProcessing();
    }

    ~TMapComputationGraphWithCodecs()
    {
        auto guard = Guard(Alloc);

        OutputUnboxedValue.Clear();
        ValueFetcher.Reset();
    }

public:
    void SetInput(const TMessageHolder& messageHolder) override {
        MKQL_ENSURE(
            InputMode == EInputMode::SingleMessage,
            "Method is not supported for provided input mode");

        static_cast<TSingleMessageValueFetcher*>(ValueFetcher.Get())
            ->SetInput(messageHolder);
    }

    void SetInput(const std::vector<TMessageHolder>& messageHolders) override {
        MKQL_ENSURE(
            InputMode == EInputMode::MessageSequence || InputMode == EInputMode::MessageSequenceWithFinish,
            "Method is not supported for provided input mode");

        static_cast<TMessageSequenceValueFetcher*>(ValueFetcher.Get())
            ->SetInput(messageHolders);
    }

    bool FetchOutput(TVector<NYT::NFlow::TMessage>& messages) override {
        auto fetchOutputGuard = TSimpleTimingGuard(FetchOutputCpuVCpuTimeCounter);
        auto guard = Guard(Alloc);

        NYql::NUdf::TUnboxedValue unboxedValue;

        while (true) {
            auto status = OutputUnboxedValue.Fetch(unboxedValue);
            MKQL_ENSURE(status != NKikimr::NUdf::EFetchStatus::Finish,
                "Unexpected 'Finish' fetch status");

            if (status == NKikimr::NUdf::EFetchStatus::Ok) {
                break;
            }

            if (status == NKikimr::NUdf::EFetchStatus::Yield) {
                if (InputMode == EInputMode::MessageSequence &&
                    !ComputationGraph->GetFlushingMode()
                ) {
                    continue;
                }

                return false;
            }
        }

        ui32 index = 0;
        bool isVariantOutput = static_cast<const NKikimr::NMiniKQL::TStreamType*>(OutputType)
            ->GetItemType()
            ->IsVariant();
        NYql::NUdf::TUnboxedValue resultValue;
        if (isVariantOutput) {
            index = unboxedValue.GetVariantIndex();
            resultValue = unboxedValue.GetVariantItem();
        } else {
            resultValue = unboxedValue;
        }

        NYT::NTableClient::TUnversionedRow unversionedRow;

        {
            auto outputCodecGuard = TSimpleTimingGuard(OutputCodecCpuVCpuTimeCounter);
            unversionedRow = OutputCodecs[index]->Convert(std::move(resultValue));
        }

        NYT::NFlow::TMessage message;

        if (InjectInputMessageId) {
            ui32 injectInputMessageIdColumnIndex = OutputInjectedInputMessageIdColumnIndexByOutputIndex.at(index);

            NYT::NTableClient::TUnversionedOwningRowBuilder builder(
                unversionedRow.GetCount());

            for (const auto& [index, item] : Enumerate(unversionedRow)) {
                if (index == injectInputMessageIdColumnIndex) {
                    builder.AddValue(
                        NYT::NTableClient::MakeUnversionedStringValue(
                            ValueFetcher->GetLastConsumedInputMessageId(),
                            injectInputMessageIdColumnIndex));
                } else {
                    builder.AddValue(item);
                }
            }

            message.Payload = NYT::NFlow::TPayload(
                NYT::NFlow::TCompactUnversionedOwningRow(
                    builder.FinishRow()));
        } else {
            message.Payload = NYT::NFlow::TPayload(
                NYT::NFlow::TCompactUnversionedOwningRow(unversionedRow));
        }

        auto outputSchemaIterator = OutputSchemasByOutputIndex.find(index);
        MKQL_ENSURE(outputSchemaIterator != OutputSchemasByOutputIndex.end(),
            "Unknown output schema for output index: " << index);
        message.PayloadSchema = outputSchemaIterator->second;

        auto outputStreamInfosIterator = OutputStreamInfosByOutputIndex.find(index);
        MKQL_ENSURE(outputStreamInfosIterator != OutputStreamInfosByOutputIndex.end(),
            "Unknown streams for output index: " << index);

        for (const auto& outputStreamInfo : outputStreamInfosIterator->second) {
            message.StreamId = outputStreamInfo.StreamId;
            messages.push_back(message);
        }

        RowBuffers[index]->Clear();

        return true;
    }

    void ResetInput() override {
        MKQL_ENSURE(
            !ValueFetcher->HasMore(),
            "Input values are not fully consumed");

        if (InputMode == EInputMode::MessageSequence) {
            MKQL_ENSURE(ComputationGraph->GetFlushingMode(),
                "Flushing mode is set to false, some values may not be consumed");

            ComputationGraph->SetFlushingMode(false);
        }

        CheckConsumedLinear();

        StreamValue->Reset();
    }

private:
    void InitCodecs() {
        auto inputTypeIterator = InputTypes.find(TString(YtflowInputStreamCallableName));
        MKQL_ENSURE(inputTypeIterator != InputTypes.end(),
            "Missing input type for callable: " << YtflowInputStreamCallableName);

        const auto* inputType = inputTypeIterator->second;

        MKQL_ENSURE(
            inputType->IsStream(),
            "Unexpected input type: " << inputType->GetKindAsStr());

        auto inputItemType = static_cast<
            const NKikimr::NMiniKQL::TStreamType*>(inputType)->GetItemType();

        StreamInputCodec = CreateRowInputCodec(
            inputItemType,
            InputSchema,
            NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true));

        MKQL_ENSURE(
            OutputType->IsStream(),
            "Unexpected output type: " << OutputType->GetKindAsStr());

        for (auto& [outputIndex, outputStreamInfos] : OutputStreamInfosByOutputIndex) {
            TVector<NYT::NTableClient::TTableSchemaPtr> outputSchemas;
            for (const auto& outputStreamInfo : outputStreamInfos) {
                outputSchemas.push_back(outputStreamInfo.OutputSchema);
            }

            outputSchemas.erase(
                UniqueBy(outputSchemas.begin(), outputSchemas.end(), [](const auto& schema) { return *schema; }),
                outputSchemas.end());

            MKQL_ENSURE(outputSchemas.size() == 1, "Expected one unique output schema, but got "
                << outputSchemas.size() << " (" << JoinSeq(", ", outputSchemas) << ")");

            auto outputSchema = outputStreamInfos.begin()->OutputSchema;

            if (InjectInputMessageId) {
                std::optional<ui32> injectedInputMessageIdColumnIndex;

                for (const auto& [index, column] : Enumerate(outputSchema->Columns())) {
                    if (column.Name() == "$input_message_id") {
                        injectedInputMessageIdColumnIndex = index;
                        break;
                    }
                }

                MKQL_ENSURE(
                    injectedInputMessageIdColumnIndex,
                    "$input_message_id is not present in table schema");

                OutputInjectedInputMessageIdColumnIndexByOutputIndex.emplace(
                    outputIndex, *injectedInputMessageIdColumnIndex);
            }

            OutputSchemasByOutputIndex.emplace(outputIndex, outputSchema);
        }

        auto outputItemType = static_cast<
            const NKikimr::NMiniKQL::TStreamType*>(OutputType)->GetItemType();

        TArrayRef<const NKikimr::NMiniKQL::TType* const> outputTypes;
        if (outputItemType->IsVariant()) {
            auto* outputVariantType = static_cast<
                const NKikimr::NMiniKQL::TVariantType*>(outputItemType);
            auto* underlyingType = outputVariantType->GetUnderlyingType();

            MKQL_ENSURE(
                underlyingType->IsTuple(),
                "Unexpected underlying output row type: "
                    << underlyingType->GetKindAsStr());

            outputTypes = static_cast<
                const NKikimr::NMiniKQL::TTupleType*>(underlyingType)->GetElements();
        } else {
            MKQL_ENSURE(OutputSchemasByOutputIndex.size() == 1,
                "Expected one output index, but got: " << OutputSchemasByOutputIndex.size());

            auto& [outputIndex, outputStreamInfos] = *OutputSchemasByOutputIndex.begin();
            MKQL_ENSURE(outputIndex == 0, "Expected '0' output index, but got: " << outputIndex);

            outputTypes = TArrayRef(&outputItemType, 1);
        }

        OutputCodecs.reserve(outputTypes.size());
        RowBuffers.reserve(outputTypes.size());

        for (auto [outputTypeIndex, outputType] : Enumerate(outputTypes)) {
            auto outputSchemaIterator = OutputSchemasByOutputIndex.find(outputTypeIndex);
            MKQL_ENSURE(outputSchemaIterator != OutputSchemasByOutputIndex.end(),
                "Unknown output schema for output index: " << outputTypeIndex);

            RowBuffers.push_back(NYT::New<NYT::NTableClient::TRowBuffer>());
            OutputCodecs.push_back(CreateRowOutputCodec(
                outputType,
                outputSchemaIterator->second,
                RowBuffers.back(),
                NYql::NYtflow::NCodec::TConvertOptions().WithAllowExtraYtFields(true)));
        }
    }

    void SetupProcessing() {
        auto inputNodeIterator =
            YtflowInputNodes.find(TString(YtflowInputStreamCallableName));

        MKQL_ENSURE(inputNodeIterator != YtflowInputNodes.end(),
            "Missing input node for callable: " << YtflowInputStreamCallableName);

        auto* ytflowInputNode = inputNodeIterator->second;

        switch (InputMode) {
        case EInputMode::SingleMessage:
            ValueFetcher = MakeHolder<TSingleMessageValueFetcher>(
                InputSchema, StreamInputCodec.Get(), Profiler, ConverterCache);

            break;

        case EInputMode::MessageSequence:
        case EInputMode::MessageSequenceWithFinish:
            ValueFetcher = MakeHolder<TMessageSequenceValueFetcher>(
                InputSchema, StreamInputCodec.Get(), Profiler, ConverterCache);

            break;
        }

        ytflowInputNode->SetValue(
            ComputationGraph->GetContext(),
            ComputationGraph->GetHolderFactory().Create<TStreamValue>(
                ValueFetcher.Get(), InputMode, ComputationGraph.Get()));

        StreamValue = static_cast<TStreamValue*>(
            ytflowInputNode->GetValue(ComputationGraph->GetContext())
                .AsBoxed().Get());

        OutputUnboxedValue = ComputationGraph->GetValue();
    }

private:
    NYT::NTableClient::TTableSchemaPtr InputSchema;
    THashMap<ui32, TVector<TOutputStreamInfo>> OutputStreamInfosByOutputIndex;
    THashMap<ui32, NYT::NTableClient::TTableSchemaPtr> OutputSchemasByOutputIndex;
    THashMap<ui32, ui32> OutputInjectedInputMessageIdColumnIndexByOutputIndex;
    EInputMode InputMode;
    bool InjectInputMessageId;

    NYT::NFlow::IPayloadConverterCachePtr ConverterCache;

    TVector<NYT::NTableClient::TRowBufferPtr> RowBuffers;

    THolder<IValueFetcher> ValueFetcher;
    TStreamValue* StreamValue = nullptr;

    THolder<NYql::NYtflow::NCodec::IRowInputCodec> StreamInputCodec;
    TVector<THolder<NYql::NYtflow::NCodec::IRowOutputCodec>> OutputCodecs;

    NYql::NUdf::TUnboxedValue OutputUnboxedValue;

    NYT::NProfiling::TProfiler Profiler;

    std::optional<double> CpuToVcpuFactor;

    NYT::NProfiling::TTimeCounter FetchOutputCpuTimeCounter;
    NYT::NProfiling::TTimeCounter FetchOutputVCpuTimeCounter;
    TCpuVCpuTimeCounter FetchOutputCpuVCpuTimeCounter;

    NYT::NProfiling::TTimeCounter OutputCodecCpuTimeCounter;
    NYT::NProfiling::TTimeCounter OutputCodecVCpuTimeCounter;
    TCpuVCpuTimeCounter OutputCodecCpuVCpuTimeCounter;
};

} // namespace NYql::NYtflow::NPrivate

namespace NYql::NYtflow {

TComputationGraphResources ResolveMapComputationGraphResources(
    const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& staticResources)
{
    return ResolveComputationGraphResources(
        staticResources,
        ComputationPatternResourceAlias);
}

THolder<IMapComputationGraphWithCodecs> CreateMapComputationGraphWithCodecs(
    TString lambdaFile,
    NYT::NTableClient::TTableSchemaPtr inputSchema,
    THashMap<ui32, TVector<TOutputStreamInfo>> outputStreamInfosByOutputIndex,
    TVector<TString> udfPaths,
    EInputMode inputMode,
    TLangVersion langVersion,
    TString optLLVM,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings,
    bool injectInputMessageId,
    NYT::NProfiling::TProfiler profiler,
    NYT::NFlow::IPayloadConverterCachePtr converterCache,
    TComputationGraphResources resources)
{
    return MakeHolder<NPrivate::TMapComputationGraphWithCodecs>(
        std::move(lambdaFile),
        std::move(inputSchema),
        std::move(outputStreamInfosByOutputIndex),
        std::move(udfPaths),
        inputMode,
        langVersion,
        std::move(optLLVM),
        std::move(runtimeSettings),
        injectInputMessageId,
        std::move(profiler),
        std::move(converterCache),
        std::move(resources));
}

} // namespace NYql::NYtflow
