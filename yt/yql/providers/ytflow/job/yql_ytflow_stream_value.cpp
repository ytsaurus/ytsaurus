#include "yql_ytflow_stream_value.h"
#include "yql_ytflow_timing_guard.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <yt/yql/providers/ytflow/codec/yql_ytflow_input_codec.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <vector>


namespace NYql::NYtflow {

TSingleMessageValueFetcher::TSingleMessageValueFetcher(
    NYT::NTableClient::TTableSchemaPtr inputSchema,
    NYql::NYtflow::NCodec::IRowInputCodec* inputCodec,
    NYT::NProfiling::TProfiler profiler,
    NYT::NFlow::IPayloadConverterCachePtr converterCache
)
    : InputSchema(std::move(inputSchema))
    , InputCodec(inputCodec)
    , ConverterCache(std::move(converterCache))
    , CpuToVcpuFactor(TryGetCpuToVCpuFactor())
    , InputCodecCpuTimeCounter(profiler.TimeCounter("/custom/input_codec/cpu_time"))
    , InputCodecVCpuTimeCounter(profiler.TimeCounter("/custom/input_codec/vcpu_time"))
    , InputCodecCpuVCpuTimeCounter(
        InputCodecCpuTimeCounter, InputCodecVCpuTimeCounter, CpuToVcpuFactor)
{
}

void TSingleMessageValueFetcher::SetInput(const TMessageHolder& messageHolder) {
    MessageHolder = &messageHolder;
    FetchedValue = false;
}

bool TSingleMessageValueFetcher::FetchValue(NKikimr::NUdf::TUnboxedValue& value) {
    if (FetchedValue) {
        return false;
    }

    MKQL_ENSURE(MessageHolder, "MessageHolder was not set");

    // TODO(ngc224): request columns from queue source directly
    auto convertedPayload = NYT::NFlow::ConvertPayloadToNewSchema(
        MessageHolder->GetMessage().Payload,
        MessageHolder->GetMessage().PayloadSchema,
        InputSchema,
        ConverterCache);

    {
        auto inputCodecGuard = TSimpleTimingGuard(InputCodecCpuVCpuTimeCounter);
        value = InputCodec->Convert(convertedPayload.Underlying());
    }

    LastConsumedInputMessageId = MessageHolder->GetMessage().MessageId.Underlying();

    FetchedValue = true;

    return true;
}

bool TSingleMessageValueFetcher::HasMore() const {
    return !FetchedValue;
}

const TString& TSingleMessageValueFetcher::GetLastConsumedInputMessageId() const {
    return LastConsumedInputMessageId;
}

TMessageSequenceValueFetcher::TMessageSequenceValueFetcher(
    NYT::NTableClient::TTableSchemaPtr inputSchema,
    NYql::NYtflow::NCodec::IRowInputCodec* inputCodec,
    NYT::NProfiling::TProfiler profiler,
    NYT::NFlow::IPayloadConverterCachePtr converterCache
)
    : UnderlyingValueFetcher(
        MakeHolder<TSingleMessageValueFetcher>(
            std::move(inputSchema), inputCodec, std::move(profiler), std::move(converterCache)))
{
}

void TMessageSequenceValueFetcher::SetInput(const std::vector<TMessageHolder>& messageHolders) {
    Current = messageHolders.begin();
    End = messageHolders.end();
}

bool TMessageSequenceValueFetcher::FetchValue(NKikimr::NUdf::TUnboxedValue& value) {
    if (Current == End) {
        return false;
    }

    UnderlyingValueFetcher->SetInput(*Current);
    bool fetchedValue = UnderlyingValueFetcher->FetchValue(value);

    ++Current;

    return fetchedValue;
}

bool TMessageSequenceValueFetcher::HasMore() const {
    return Current != End;
}

const TString& TMessageSequenceValueFetcher::GetLastConsumedInputMessageId() const {
    return UnderlyingValueFetcher->GetLastConsumedInputMessageId();
}

TStreamValue::TStreamValue(
    NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
    IValueFetcher* valueFetcher,
    EInputMode inputMode,
    NKikimr::NMiniKQL::IComputationGraph* computationGraph
)
    : TComputationValue(memInfo)
    , ValueFetcher(valueFetcher)
    , InputMode(inputMode)
    , ComputationGraph(computationGraph)
{
}

NYql::NUdf::EFetchStatus TStreamValue::Fetch(NYql::NUdf::TUnboxedValue& value) {
    if (PendingYield) {
        PendingYield = false;
        return NYql::NUdf::EFetchStatus::Yield;
    }

    if (ValueFetcher->FetchValue(value)) {
        if (InputMode == EInputMode::SingleMessage) {
            PendingYield = true;
        }

        return NYql::NUdf::EFetchStatus::Ok;
    }

    switch (InputMode) {
    case EInputMode::SingleMessage:
    case EInputMode::MessageSequenceWithFinish:
        break;
    case EInputMode::MessageSequence:
        ComputationGraph->SetFlushingMode(true);
        break;
    }

    return InputMode == EInputMode::MessageSequenceWithFinish
        ? NYql::NUdf::EFetchStatus::Finish
        : NYql::NUdf::EFetchStatus::Yield;
}

void TStreamValue::Reset() {
    PendingYield = false;
}

} // namespace NYql::NYtflow
