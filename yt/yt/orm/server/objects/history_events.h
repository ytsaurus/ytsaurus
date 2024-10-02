#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/transaction_context.h>

#include <library/cpp/yson/node/node.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

ui64 GetRawHistoryTime(THistoryTime time);
ui64 GetRawHistoryTime(THistoryTime time, bool invertTime);
bool HasValue(const THistoryTime& time);

////////////////////////////////////////////////////////////////////////////////

struct THistoryEvent
{
    TString Uuid;
    THistoryTime Time;
    TEventTypeValue EventTypeValue;
    NRpc::TAuthenticationIdentity UserIdentity;
    TAttributeValueList Attributes;
    TAttributeValueList DistinctByAttributes;
    std::vector<TString> HistoryEnabledAttributes;
    TTransactionContext TransactionContext;
    int RowIndex;

    bool operator==(const THistoryEvent& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const THistoryEvent& event, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

// Indexing event type is either equal to updated attribute name or empty string (meaning any other event).
extern const TString OtherHistoryIndexEventType;

////////////////////////////////////////////////////////////////////////////////

ui64 ExtractHistoryTime(const TNode& row);

////////////////////////////////////////////////////////////////////////////////

THistoryEvent ParseHistoryEvent(
    NTableClient::TUnversionedRow row,
    size_t startingIndex,
    const TAttributeSelector& attributeSelector,
    const std::optional<TAttributeSelector>& distinctBySelector,
    bool historyEnabledAttributePathsEnabled,
    bool historyEnabledAttributePathsField,
    EHistoryTimeMode dbTimeMode,
    bool invertTime,
    bool positiveEventTypes);

// NB! Does not parse all the history event data, only necessary for history index building.
THistoryEvent ParseHistoryEvent(
    TNode row,
    const TAttributeSelector& selector,
    bool historyEnabledAttributePathsEnabled,
    bool historyEnabledAttributePathsField,
    EHistoryTimeMode dbTimeMode);

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::TTimestamp ConvertPhysicalToLogicalTime(TInstant time);

////////////////////////////////////////////////////////////////////////////////

// Collector is able to gather events both in ascending and descending time order.
class THistoryEventCollector
    : public TRefCounted
{
public:
    THistoryEventCollector(
        bool distinct,
        std::function<void(THistoryEvent)> onEvent,
        std::optional<int> limit = std::nullopt);
    ~THistoryEventCollector();

    // After enough events have been collected, further calls are no-op.
    //
    // Natural implementation of |distinct| is:
    // - group consecutive equal events;
    // - return the first event in the time order (regardless of a result time order) within each group.
    //
    // When |distinct| is used, applies callback only to the first event within each group.
    void OnEvent(THistoryEvent);

    // Returns true if enough events have been already collected and the iteration must be stopped.
    bool IsEnough() const;

    // Must be called after the processing is done.
    //
    // When |distinct| option is applied, it is uncertain whether last event group has ended and it can be flushed, or
    // new events will still appear, so this method resolves the uncertainty by explicitly flushing last event group.
    void Flush();

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(THistoryEventCollector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
