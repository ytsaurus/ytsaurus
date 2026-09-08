#pragma once

#include "yql_ytflow_message_holder.h"
#include "yql_ytflow_timing_guard.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <yt/yql/providers/ytflow/codec/yql_ytflow_input_codec.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/library/profiling/sensor.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

#include <optional>
#include <vector>


namespace NKikimr::NMiniKQL {

class TMemoryUsageInfo;
class IComputationGraph;

} // namespace NKikimr::NMiniKQL


namespace NYql::NYtflow {

enum class EInputMode {
    SingleMessage,
    MessageSequence,
    MessageSequenceWithFinish,
};


class IValueFetcher
{
public:
    virtual bool FetchValue(NKikimr::NUdf::TUnboxedValue& value) = 0;
    virtual bool HasMore() const = 0;
    virtual const TString& GetLastConsumedInputMessageId() const = 0;

    virtual ~IValueFetcher() = default;
};


class TSingleMessageValueFetcher final
    : public IValueFetcher
{
public:
    TSingleMessageValueFetcher(
        NYT::NTableClient::TTableSchemaPtr inputSchema,
        NYql::NYtflow::NCodec::IRowInputCodec* inputCodec,
        NYT::NProfiling::TProfiler profiler,
        NYT::NFlow::IPayloadConverterCachePtr converterCache);

    void SetInput(const TMessageHolder& messageHolder);

    bool FetchValue(NKikimr::NUdf::TUnboxedValue& value) override;

    bool HasMore() const override;

    const TString& GetLastConsumedInputMessageId() const override;

private:
    NYT::NTableClient::TTableSchemaPtr InputSchema;
    NYql::NYtflow::NCodec::IRowInputCodec* InputCodec = nullptr;
    NYT::NFlow::IPayloadConverterCachePtr ConverterCache;

    const TMessageHolder* MessageHolder = nullptr;
    bool FetchedValue = false;

    TString LastConsumedInputMessageId;

    std::optional<double> CpuToVcpuFactor;

    NYT::NProfiling::TTimeCounter InputCodecCpuTimeCounter;
    NYT::NProfiling::TTimeCounter InputCodecVCpuTimeCounter;
    TCpuVCpuTimeCounter InputCodecCpuVCpuTimeCounter;
};


class TMessageSequenceValueFetcher final
    : public IValueFetcher
{
public:
    TMessageSequenceValueFetcher(
        NYT::NTableClient::TTableSchemaPtr inputSchema,
        NYql::NYtflow::NCodec::IRowInputCodec* inputCodec,
        NYT::NProfiling::TProfiler profiler,
        NYT::NFlow::IPayloadConverterCachePtr converterCache);

    void SetInput(const std::vector<TMessageHolder>& messageHolders);

    bool FetchValue(NKikimr::NUdf::TUnboxedValue& value) override;

    bool HasMore() const override;

    const TString& GetLastConsumedInputMessageId() const override;

private:
    THolder<TSingleMessageValueFetcher> UnderlyingValueFetcher;

    using TMessageHolderIterator = std::vector<TMessageHolder>::const_iterator;

    TMessageHolderIterator Current;
    TMessageHolderIterator End;
};


class TStreamValue final
    : public NKikimr::NMiniKQL::TComputationValue<TStreamValue>
{
public:
    TStreamValue(
        NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
        IValueFetcher* valueFetcher,
        EInputMode inputMode,
        NKikimr::NMiniKQL::IComputationGraph* computationGraph);

    NYql::NUdf::EFetchStatus Fetch(NYql::NUdf::TUnboxedValue& value) override;

    void Reset();

private:
    IValueFetcher* ValueFetcher = nullptr;
    EInputMode InputMode;
    NKikimr::NMiniKQL::IComputationGraph* ComputationGraph = nullptr;

    NYql::NUdf::TUnboxedValue NextValue = NYql::NUdf::TUnboxedValue::Invalid();
    TMaybe<bool> HasNextValue;
    bool PendingYield = false;
};

} // namespace NYql::NYtflow
