#include "history_events.h"

#include <yt/yt/orm/server/objects/proto/history_event_etc.pb.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_resolver.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NServer::NObjects {

namespace {

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString GetAttributeFromHistoryEventValue(
    const TStringBuf& historyEventValue,
    const NYPath::TYPath& attributePath)
{
    auto attributeValue = NYTree::TryGetAny(historyEventValue, attributePath);
    if (attributeValue) {
        return NYson::TYsonString(*attributeValue);
    } else {
        return NYson::ConvertToYsonString(NYTree::GetEphemeralNodeFactory()->CreateEntity());
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

ui64 GetRawHistoryTime(THistoryTime time)
{
    return Visit(time,
        [] (const std::monostate&) {
            THROW_ERROR_EXCEPTION("History time is not initialized");
            return ui64{};
        },
        [] (const TInstant& time) { return time.MicroSeconds(); },
        [] (const TTimestamp& time) { return time; });
}

ui64 GetRawHistoryTime(THistoryTime time, bool dbOptimizedForAscendingTime)
{
    ui64 rawHistoryTime = GetRawHistoryTime(time);
    if (!dbOptimizedForAscendingTime) {
        rawHistoryTime = ~rawHistoryTime;
    }
    return rawHistoryTime;
}

bool HasValue(const THistoryTime& time)
{
    return time.index();
}

////////////////////////////////////////////////////////////////////////////////

const TString OtherHistoryIndexEventType = "";

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const THistoryEvent& event, TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder);

    builder->AppendChar('{');
    wrapper->AppendFormat("Uuid: %v", event.Uuid);
    wrapper->AppendFormat("Time: %v", event.Time);
    wrapper->AppendFormat("EventTypeValue: %v", event.EventTypeValue);
    wrapper->AppendFormat("UserIdentity: %v", event.UserIdentity);
    wrapper->AppendFormat("Attributes: %v", event.Attributes);
    wrapper->AppendFormat("DistinctByAttributes: %v", event.DistinctByAttributes);
    wrapper->AppendFormat("HistoryEnabledAttributes: %v", event.HistoryEnabledAttributes);
    wrapper->AppendFormat("TransactionContext: %v", event.TransactionContext);
    wrapper->AppendFormat("RowIndex: %v", event.RowIndex);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

ui64 ExtractHistoryTime(const TNode& row)
{
    if (row.HasKey("time")) {
        return row["time"].AsUint64();
    }

    if (row.HasKey("inverted_time")) {
        return ~row["inverted_time"].AsUint64();
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

void ParseHistoryEventAttributes(
    THistoryEvent& event,
    TStringBuf ysonObject,
    const TAttributeSelector& attributeSelector,
    const std::optional<TAttributeSelector>& distinctBySelector)
{
    THashMap<NYPath::TYPath, NYson::TYsonString> parsedAttributes;
    auto parseByPath = [&] (const NYPath::TYPath& path) -> const NYson::TYsonString& {
        return GetOrInsert(parsedAttributes, path, [&] {
            return GetAttributeFromHistoryEventValue(ysonObject, path);
        });
    };

    for (const auto& attributePath : attributeSelector.Paths) {
        event.Attributes.Values.push_back(parseByPath(attributePath));
    }

    if (distinctBySelector) {
        for (const auto& attributePath : distinctBySelector->Paths) {
            event.DistinctByAttributes.Values.push_back(parseByPath(attributePath));
        }
    }
}

THistoryEvent ParseHistoryEvent(
    NTableClient::TUnversionedRow row,
    size_t startingIndex,
    const TAttributeSelector& attributeSelector,
    const std::optional<TAttributeSelector>& distinctBySelector,
    bool historyEnabledAttributePathsEnabled,
    bool historyEnabledAttributePathsField,
    EHistoryTimeMode dbTimeMode,
    bool invertTime,
    bool positiveEventTypes)
{
    auto parseNextValue = [&] (auto* value) {
        YT_VERIFY(startingIndex < row.GetCount());
        FromUnversionedValue(value, row[startingIndex]);
        ++startingIndex;
    };

    NYson::TYsonString objectValue;
    NProto::THistoryEventEtc etc;
    THistoryEvent event;

    parseNextValue(&event.Uuid);

    // Parsing event time.
    ui64 rawTime;
    parseNextValue(&rawTime);
    if (invertTime) {
        rawTime = ~rawTime;
    }

    switch (dbTimeMode) {
        case EHistoryTimeMode::Logical:
            event.Time.emplace<TTimestamp>(rawTime);
            break;
        case EHistoryTimeMode::Physical:
            event.Time.emplace<TInstant>() = TInstant::MicroSeconds(rawTime);
            break;
    }

    // Parsing event type.
    int eventTypeRaw;
    parseNextValue(&eventTypeRaw);
    event.EventTypeValue = positiveEventTypes ? eventTypeRaw : -eventTypeRaw;

    parseNextValue(&event.UserIdentity.User);
    parseNextValue(&objectValue);
    parseNextValue(&etc);
    FromProto(
        &event.TransactionContext,
        etc.transaction_context());
    event.UserIdentity.UserTag = etc.user_tag();

    if (historyEnabledAttributePathsEnabled) {
        // History enabled attributes are non-empty by definition.
        // If `etc` does not contain it, try parse separate field.
        NYT::FromProto(
            &event.HistoryEnabledAttributes,
            *etc.mutable_history_enabled_attributes());

        if (historyEnabledAttributePathsField && event.HistoryEnabledAttributes.empty()) {
            NYson::TYsonString historyEnabledAttributePaths;
            parseNextValue(&historyEnabledAttributePaths);

            event.HistoryEnabledAttributes =
                NYTree::ConvertTo<std::vector<NYPath::TYPath>>(historyEnabledAttributePaths);
        }

        THROW_ERROR_EXCEPTION_IF(event.HistoryEnabledAttributes.empty(),
            "Either `etc/history_enabled_attributes` or `history_enabled_attributes` fields must be present");
    }

    ParseHistoryEventAttributes(event, objectValue.AsStringBuf(), attributeSelector, distinctBySelector);

    return event;
}

THistoryEvent ParseHistoryEvent(
    TNode row,
    const TAttributeSelector& selector,
    bool historyEnabledAttributePathsEnabled,
    bool historyEnabledAttributePathsField,
    EHistoryTimeMode dbTimeMode)
{
    auto event = NOrm::NServer::NObjects::THistoryEvent{
        .Uuid = row["uuid"].AsString(),
        .EventTypeValue = static_cast<TEventTypeValue>(row["event_type"].AsInt64()),
        .UserIdentity = NRpc::TAuthenticationIdentity(row["user"].AsString()),
    };

    if (event.EventTypeValue < 0) {
        event.EventTypeValue = -event.EventTypeValue;
    }

    switch (dbTimeMode) {
        case EHistoryTimeMode::Logical:
            event.Time.emplace<TTimestamp>(ExtractHistoryTime(row));
            break;
        case EHistoryTimeMode::Physical:
            event.Time.emplace<TInstant>(TInstant::MicroSeconds(ExtractHistoryTime(row)));
            break;
    }
    auto value = NodeToYsonString(row["value"], NYson::EYsonFormat::Binary);
    ParseHistoryEventAttributes(event, value, /*attributeSelector*/ selector, /*distinctBySelector*/ selector);

    if (!historyEnabledAttributePathsEnabled) {
        return event;
    }

    auto convertNodeStringListTo = [] (const TNode::TListType& stringList, std::vector<TString>& result) {
        result.reserve(stringList.size());
        for (const auto& str : stringList) {
            result.push_back(str.AsString());
        }
    };

    if (historyEnabledAttributePathsEnabled) {
        auto keyExists = [] (const auto& map, const auto& key) {
            return map.HasKey(key) && !map[key].IsNull();
        };
        if (keyExists(row, "etc") && keyExists(row["etc"], "history_enabled_attributes")) {
            convertNodeStringListTo(row["etc"]["history_enabled_attributes"].AsList(), event.HistoryEnabledAttributes);
        }
        if (historyEnabledAttributePathsField && event.HistoryEnabledAttributes.empty()) {
            convertNodeStringListTo(row["history_enabled_attributes"].AsList(), event.HistoryEnabledAttributes);
        }
        THROW_ERROR_EXCEPTION_IF(event.HistoryEnabledAttributes.empty(),
            "Either `etc/history_enabled_attributes` or `history_enabled_attributes` fields must be present");
    }

    return event;
}

////////////////////////////////////////////////////////////////////////////////

// Physical time is converted to logical without relative order loss.
// Extra microseconds (<= 10^6) is transferred to counter of timestamp (<= 2^30).
NTransactionClient::TTimestamp ConvertPhysicalToLogicalTime(TInstant time)
{
    auto seconds = time.Seconds();
    auto microseconds = time.MicroSeconds() - seconds * 1'000'000;
    YT_VERIFY(microseconds < 1'000'000);
    return NTransactionClient::TimestampFromUnixTime(seconds) + microseconds;
}

////////////////////////////////////////////////////////////////////////////////

class THistoryEventCollector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        bool distinct,
        std::optional<int> limit,
        std::function<void(THistoryEvent)> onEvent)
        : Distinct_(distinct)
        , Limit_(limit)
        , OnEvent_(std::move(onEvent))
        , AcceptedEvents_(0)
    { }

    void OnEvent(THistoryEvent event)
    {
        if (IsEnough()) {
            return;
        }
        // COMPAT(dgolear): switch to new state machine and add unit tests.
        if (Distinct_ && LastEvent_ && event.EventTypeValue != ObjectRemovedEventTypeValue) {
            if (!HaveAttributesChanged(*LastEvent_, event)) {
                if (LastEvent_->Time > event.Time) {
                    // This workarounds YP-2515.
                    // Affects only descending time order, effectively reversing each group.
                    LastEvent_ = std::move(event);
                }
                return;
            }
        }
        if (!Distinct_) {
            AcceptEvent(std::move(event));
        } else {
            if (LastEvent_) {
                AcceptEvent(std::move(*LastEvent_));
            }
            LastEvent_ = std::move(event);
        }
    }

    bool IsEnough() const
    {
        // If distinct enabled, one more event is needed for two reasons:
        // - to correctly determine first event within the last group when descending is enabled;
        // - to skip events from this group after running another select with continuation.
        return Limit_ && (AcceptedEvents_ + LastEvent_.has_value()) >= (*Limit_ + Distinct_);
    }

    void Flush()
    {
        if (Limit_ && AcceptedEvents_ == *Limit_ && LastEvent_) {
            LastEvent_.reset();
        }
        if (LastEvent_) {
            AcceptEvent(std::move(*LastEvent_));
            LastEvent_.reset();
        }
    }

private:
    const bool Distinct_;
    const std::optional<int> Limit_;
    const std::function<void(THistoryEvent)> OnEvent_;

    std::optional<THistoryEvent> LastEvent_;
    int AcceptedEvents_;

    void AcceptEvent(THistoryEvent event)
    {
        ++AcceptedEvents_;
        OnEvent_(std::move(event));
    }

    bool AreEventsDistinct(
        const THistoryEvent& firstEvent,
        const THistoryEvent& secondEvent) const
    {
        return firstEvent.EventTypeValue != secondEvent.EventTypeValue ||
            HaveAttributesChanged(firstEvent, secondEvent) ||
            firstEvent.Uuid != secondEvent.Uuid;
    }

    bool HaveAttributesChanged(const THistoryEvent& firstEvent, const THistoryEvent& secondEvent) const
    {
        return firstEvent.DistinctByAttributes.Values != secondEvent.DistinctByAttributes.Values;
    }
};

////////////////////////////////////////////////////////////////////////////////

THistoryEventCollector::THistoryEventCollector(
    bool distinct,
    std::function<void(THistoryEvent)> onEvent,
    std::optional<int> limit)
    : Impl_(New<TImpl>(
        distinct,
        limit,
        std::move(onEvent)))
{ }

THistoryEventCollector::~THistoryEventCollector()
{ }

void THistoryEventCollector::OnEvent(THistoryEvent event)
{
    Impl_->OnEvent(std::move(event));
}

bool THistoryEventCollector::IsEnough() const
{
    return Impl_->IsEnough();
}

void THistoryEventCollector::Flush()
{
    Impl_->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NOrm::NServer::NObjects
