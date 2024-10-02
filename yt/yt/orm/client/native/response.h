#pragma once

#include "public.h"

#include "payload.h"
#include "attribute.h"

#include <yt/yt/orm/client/objects/transaction_context.h>

#include <yt/yt/client/query_client/query_statistics.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TAttributeList
{
    std::vector<TPayload> ValuePayloads;
    std::vector<NObjects::TTimestamp> Timestamps;
};

template <typename TAttributeListProto>
void FromProto(
    TAttributeList* attributeList,
    const TAttributeListProto& protoAttributeList);

////////////////////////////////////////////////////////////////////////////////

struct TPerformanceStatistics
{
    i64 ReadPhaseCount{0};
    THashMap<TString, std::vector<NQueryClient::TQueryStatistics>> SelectQueryStatistics;
};

template <typename TProtoPerformanceStatistics>
void FromProto(
    TPerformanceStatistics* performanceStatistics,
    const TProtoPerformanceStatistics& protoPerformanceStatistics);

struct TCommonResult
{
    TPerformanceStatistics PerformanceStatistics;
};

////////////////////////////////////////////////////////////////////////////////

struct TGenerateTimestampResult
{
    NObjects::TTimestamp Timestamp;
};

struct TSelectObjectsResult
    : TCommonResult
{
    std::vector<TAttributeList> Results;
    NObjects::TTimestamp Timestamp;
    std::optional<TString> ContinuationToken;
};

struct TGetObjectResult
    : TCommonResult
{
    TAttributeList Result;
    NObjects::TTimestamp Timestamp;
};

struct TGetObjectsResult
    : TCommonResult
{
    std::vector<TAttributeList> Subresults;
    NObjects::TTimestamp Timestamp;
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateObjectsSubresult
{
    TObjectId ObjectId;
    TObjectId Fqid;
    TPayload Meta;
};

struct TRemoveObjectsSubresult
{
    TInstant FinalizationStartTime;
};

template <typename TProtoRemoveObjectsSubresult>
void FromProto(TRemoveObjectsSubresult* subresult, const TProtoRemoveObjectsSubresult& protoSubresult);

struct TUpdateObjectResult
    : TCommonResult
{
    NObjects::TTimestamp CommitTimestamp;
};

struct TUpdateObjectsResult
    : TCommonResult
{
    NObjects::TTimestamp CommitTimestamp;
};

struct TCreateObjectResult
    : TCommonResult
{
    TObjectId ObjectId;
    NObjects::TTimestamp CommitTimestamp;
    TPayload Meta;
};

struct TCreateObjectsResult
    : TCommonResult
{
    std::vector<TCreateObjectsSubresult> Subresults;
    NObjects::TTimestamp CommitTimestamp;
};

struct TRemoveObjectResult
    : TCommonResult
{
    NObjects::TTimestamp CommitTimestamp;
    TInstant FinalizationStartTime;
};

struct TRemoveObjectsResult
    : TCommonResult
{
    NObjects::TTimestamp CommitTimestamp;
    std::vector<TRemoveObjectsSubresult> Subresults;
};

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionResult
{
    TTransactionId TransactionId;
    NObjects::TTimestamp StartTimestamp;
    TInstant StartTime;
};

struct TAbortTransactionResult
{ };

struct TCommitTransactionResult
    : public TCommonResult
{
    NObjects::TTimestamp CommitTimestamp;
    TInstant StartTime;
    TInstant FinishTime;
    TPerformanceStatistics TotalPerformanceStatistics;
};

////////////////////////////////////////////////////////////////////////////////

struct TWatchObjectsEventIndex
{
    int Tablet = 0;
    i64 Row = 0;
    i64 Event = 0;
};

struct TWatchObjectsEvent
{
    NObjects::TTimestamp Timestamp;
    EEventType EventType;
    NObjects::TObjectId ObjectId;
    TPayload Meta;
    TTransactionContext TransactionContext;
    TString ChangedAttributesSummary;
    NObjects::TTimestamp HistoryTimestamp;
    TInstant HistoryTime;
    TWatchObjectsEventIndex Index;
    std::vector<i32> ChangedTags;
};

struct TWatchObjectsResult
    : TCommonResult
{
    NObjects::TTimestamp Timestamp = NObjects::NullTimestamp;
    TString ContinuationToken;
    std::vector<TWatchObjectsEvent> Events;
    ui64 SkippedRowCount = 0;
    std::vector<TString> ChangedAttributesIndex;
};

struct TSelectObjectHistoryResult
    : TCommonResult
{
    struct TEvent
    {
        TInstant Time;
        NObjects::TTimestamp Timestamp;
        EEventType EventType;
        NRpc::TAuthenticationIdentity UserIdentity;
        TAttributeList Results;
        std::vector<TString> HistoryEnabledAttributes;
        TTransactionContext TransactionContext;
    };
    std::vector<TEvent> Events;
    TString ContinuationToken;
    TInstant LastTrimTime;
};

struct TMasterInfo
{
    TString Fqdn;
    TString GrpcAddress;
    TString GrpcIP6Address;
    TString HttpAddress;
    TString RpcProxyAddress;
    TMasterInstanceTag InstanceTag;
    bool Alive;
    bool Leading;
};

struct TGetMastersResult
{
    std::vector<TMasterInfo> MasterInfos;
    int ClusterTag;
};

////////////////////////////////////////////////////////////////////////////////

using TParsePayloadsOptions = TParseAttributeOptions;

////////////////////////////////////////////////////////////////////////////////

template <class... TAttributes>
void ParsePayloads(
    std::vector<TPayload> payloads,
    const TParsePayloadsOptions& options,
    TAttributes*... attributes);

template <std::derived_from<::google::protobuf::Message> TProtoMessage>
TProtoMessage ParseRootObject(const std::vector<TPayload>& payloads);

////////////////////////////////////////////////////////////////////////////////

void ValidatePayloadFormat(
    EPayloadFormat expectedFormat,
    const TPayload& payload);

////////////////////////////////////////////////////////////////////////////////

template <class TProtoResponse>
void FillCommonResult(TCommonResult& result, const TProtoResponse& response);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative

#define RESPONSE_INL_H_
#include "response-inl.h"
#undef RESPONSE_INL_H_
