#ifndef RESPONSE_INL_H_
#error "Direct inclusion of this file is not allowed, include response.h"
// For the sake of sane code completion.
#include "response.h"
#endif

#include "attribute.h"

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ytree/convert.h>

#include <google/protobuf/timestamp.pb.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TAttribute>
void ParseYsonPayload(
    TYsonPayload yson,
    TAttribute* attribute,
    const TParsePayloadsOptions& options)
{
    if constexpr (std::derived_from<TAttribute, google::protobuf::Message>) {
        ParseProtobufAttribute(attribute, NYson::ReflectProtobufMessageType<TAttribute>(), yson, options);
        return;
    }
    auto node = NYTree::ConvertTo<NYTree::INodePtr>(std::move(yson.Yson));
    if constexpr (std::same_as<TAttribute, NYTree::INodePtr>) {
        *attribute = std::move(node);
    } else {
        if (node->GetType() == NYTree::ENodeType::Entity) {
            if (!node->Attributes().ListKeys().empty()) {
                THROW_ERROR_EXCEPTION("Error parsing yson payload: entity with attributes is not supported yet");
            }
            *attribute = TAttribute();
        } else {
            ParseAttribute(*attribute, std::move(node), options);
        }
    }
}

template <>
void ParseYsonPayload(
    TYsonPayload yson,
    NYT::NYson::TYsonString* attribute,
    const TParseAttributeOptions& options);

////////////////////////////////////////////////////////////////////////////////

template <class TAttribute>
void ParseProtobufPayload(
    TProtobufPayload protobuf,
    TAttribute* attribute,
    const TParsePayloadsOptions& /*options*/)
{
    if constexpr (std::derived_from<TAttribute, google::protobuf::Message>) {
        if (!attribute->ParseFromString(protobuf.Protobuf)) {
            THROW_ERROR_EXCEPTION("Error parsing %Qv from protobuf payload",
                attribute->GetTypeName());
        }
    } else {
        THROW_ERROR_EXCEPTION("Protobuf payload parsing is not supported yet");
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TAttribute>
void ParsePayload(
    TPayload payload,
    TAttribute* attribute,
    const TParsePayloadsOptions& options)
{
    NYT::Visit(payload,
        [&] (TNullPayload& /*null*/) {
            THROW_ERROR_EXCEPTION("Unexpected null payload");
        },
        [&] (TYsonPayload& yson) {
            ParseYsonPayload(std::move(yson), attribute, options);
        },
        [&] (TProtobufPayload& protobuf) {
            ParseProtobufPayload(std::move(protobuf), attribute, options);
        });
}

////////////////////////////////////////////////////////////////////////////////

template <size_t Index, class... TAttributes>
struct TParsePayloadsHelper;

template <size_t Index>
struct TParsePayloadsHelper<Index>
{
    static void Run(std::vector<TPayload>& /*payloads*/, const TParsePayloadsOptions& /*options*/)
    { }
};

template <size_t Index, class TAttribute, class... TAttributes>
struct TParsePayloadsHelper<Index, TAttribute, TAttributes...>
{
    static void Run(
        std::vector<TPayload>& payloads,
        const TParsePayloadsOptions& options,
        TAttribute* attribute,
        TAttributes*... attributes)
    {
        ParsePayload(std::move(payloads[Index]), attribute, options);
        TParsePayloadsHelper<Index + 1, TAttributes...>::Run(payloads, options, attributes...);
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class... TAttributes>
void ParsePayloads(
    std::vector<TPayload> payloads,
    const TParsePayloadsOptions& options,
    TAttributes*... attributes)
{
    if (sizeof...(attributes) != payloads.size()) {
        THROW_ERROR_EXCEPTION("Incorrect number of payloads: expected %v, but got %v",
            sizeof...(attributes),
            payloads.size());
    }
    NDetail::TParsePayloadsHelper<0, TAttributes...>::Run(
        payloads,
        options,
        attributes...);
}

template <class TAttribute, size_t ArraySize>
void ParsePayloads(
    std::vector<TPayload> payloads,
    const TParsePayloadsOptions& options,
    std::array<TAttribute, ArraySize>* attributes)
{
    if (attributes->size() != payloads.size()) {
        THROW_ERROR_EXCEPTION("Incorrect number of payloads: expected %v, but got %v",
            attributes->size(),
            payloads.size());
    }
    for (int index = 0; index < std::ssize(payloads); ++index) {
        NDetail::ParsePayload(std::move(payloads[index]), &(*attributes)[index], options);
    }
}

template <>
void ParsePayloads<NObjects::TObjectKey>(
    std::vector<TPayload> payloads,
    const TParsePayloadsOptions& options,
    NObjects::TObjectKey* attribute);

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<::google::protobuf::Message> TProtoMessage>
TProtoMessage ParseRootObject(const std::vector<TPayload>& payloads)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        payloads.size() == 1,
        "Expected exactly one payload to parse root object from, got %v",
        payloads.size());

    const auto* protoPayload = Visit(
        payloads[0],
        [] (const TProtobufPayload& protoPayload) {
            return &protoPayload;
        },
        [] (const TYsonPayload&) -> const TProtobufPayload* {
            THROW_ERROR_EXCEPTION("Unexpected Yson payload when parsing root object");
        },
        [] (const TNullPayload&) -> const TProtobufPayload* {
            THROW_ERROR_EXCEPTION("Unexpected Null payload when parsing root object");
        });

    TProtoMessage result{};
    THROW_ERROR_EXCEPTION_UNLESS(
        result.ParseFromString(protoPayload->Protobuf),
        "Error parsing root object from Protobuf payload");

    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TAttributeListProto>
void FromProto(
    TAttributeList* attributeList,
    const TAttributeListProto& protoAttributeList)
{
    attributeList->ValuePayloads.reserve(protoAttributeList.value_payloads_size());
    for (const auto& protoPayload : protoAttributeList.value_payloads()) {
        auto& payload = attributeList->ValuePayloads.emplace_back();
        FromProto(&payload, protoPayload);
    }

    attributeList->Timestamps.reserve(protoAttributeList.timestamps_size());
    for (auto protoTimestamp : protoAttributeList.timestamps()) {
        attributeList->Timestamps.push_back(protoTimestamp);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoPerformanceStatistics>
void FromProto(
    TPerformanceStatistics* performanceStatistics,
    const TProtoPerformanceStatistics& protoPerformanceStatistics)
{
    performanceStatistics->ReadPhaseCount = protoPerformanceStatistics.read_phase_count();
    performanceStatistics->SelectQueryStatistics.reserve(
        protoPerformanceStatistics.select_query_statistics_size());
    for (const auto& protoEntry : protoPerformanceStatistics.select_query_statistics()) {
        auto& queryStatistics = performanceStatistics->SelectQueryStatistics[protoEntry.table_name()];
        queryStatistics.reserve(protoEntry.statistics_size());
        for (const auto& protoStatistics : protoEntry.statistics()) {
            NQueryClient::TQueryStatistics statistics;
            FromProto(&statistics, protoStatistics);
            queryStatistics.push_back(std::move(statistics));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template<typename TProtoWatchObjectsEventIndex>
void FromProto(
    TWatchObjectsEventIndex* index,
    const TProtoWatchObjectsEventIndex& protoIndex)
{
    index->Tablet = protoIndex.tablet();
    index->Row = protoIndex.row();
    index->Event = protoIndex.event();
}

template<typename TRspWatchObjects>
void FromProto(
    std::vector<TWatchObjectsEvent>* result,
    const TRspWatchObjects& rsp)
{
    result->reserve(rsp.events_size());
    for (const auto& event : rsp.events()) {
        TPayload meta;
        FromProto(&meta, event.meta());
        result->emplace_back(TWatchObjectsEvent{
            .Timestamp = event.timestamp(),
            .EventType = static_cast<EEventType>(event.event_type()),
            .ObjectId = event.object_id(),
            .Meta = std::move(meta),
            .ChangedAttributesSummary = event.changed_attributes_summary(),
            .HistoryTimestamp = event.history_timestamp(),
            .HistoryTime = NProtoInterop::CastFromProto(event.history_time()),
        });
        FromProto(&result->back().Index, event.index());
        FromProto(
            &result->back().TransactionContext,
            event.transaction_context());
        NYT::FromProto(&result->back().ChangedTags, event.changed_tags());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoRemoveObjectsSubresult>
void FromProto(
    TRemoveObjectsSubresult* subresult,
    const TProtoRemoveObjectsSubresult& protoSubresult)
{
    subresult->FinalizationStartTime = NYT::FromProto<TInstant>(protoSubresult.finalization_start_time());
}

////////////////////////////////////////////////////////////////////////////////

template<typename TRspSelectObjectHistory>
void FromProto(
    std::vector<TSelectObjectHistoryResult::TEvent>* result,
    const TRspSelectObjectHistory& rsp)
{
    result->reserve(rsp.events_size());
    for (const auto& event : rsp.events()) {
        TAttributeList attributeList;
        FromProto(&attributeList, event.results());
        std::vector<TString> attributes;
        const auto& proto_attributes = event.history_enabled_attributes();
        attributes.reserve(proto_attributes.size());
        std::copy(proto_attributes.begin(), proto_attributes.end(), std::back_insert_iterator(attributes));

        const TInstant timeValue = NProtoInterop::CastFromProto(event.time());
        const NObjects::TTimestamp timestampValue = event.timestamp();

        TTransactionContext transactionContext;

        FromProto(
            &transactionContext,
            event.transaction_context());

        result->push_back(TSelectObjectHistoryResult::TEvent{
            .Time = timeValue,
            .Timestamp = timestampValue,
            .EventType = static_cast<EEventType>(event.event_type()),
            .UserIdentity = NRpc::TAuthenticationIdentity(event.user(), event.user_tag()),
            .Results = std::move(attributeList),
            .HistoryEnabledAttributes = std::move(attributes),
            .TransactionContext = std::move(transactionContext),
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TProtoResponse>
void FillCommonResult(TCommonResult& result, const TProtoResponse& response)
{
    FromProto(&result.PerformanceStatistics, response->performance_statistics());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
