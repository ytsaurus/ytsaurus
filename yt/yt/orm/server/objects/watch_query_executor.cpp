#include "watch_query_executor.h"

#include "continuation.h"
#include "db_config.h"
#include "db_schema.h"
#include "helpers.h"
#include "object_manager.h"
#include "ordered_tablet_reader.h"
#include "private.h"
#include "tags.h"
#include "transaction.h"
#include "watch_log.h"
#include "watch_log_event_matcher.h"
#include "watch_manager.h"

#include <library/cpp/iterator/enumerate.h>
#include <yt/yt/orm/server/objects/proto/continuation_token.pb.h>
#include <yt/yt/orm/server/objects/proto/watch_record.pb.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/orm/library/attributes/attribute_path.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ypath/helpers.h>

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NConcurrency;
using namespace NYT::NYPath;
using namespace NYT::NQueryClient::NAst;

using namespace NYT::NOrm::NQuery;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void ThrowTabletCountChange(const TString& logName, size_t oldCount, size_t newCount)
{
    THROW_ERROR_EXCEPTION(NClient::EErrorCode::UnstableDatabaseSettings,
        "Watch log %Qv tablet count has changed during execution: was %v, now %v",
        logName,
        oldCount,
        newCount);
}

////////////////////////////////////////////////////////////////////////////////

class TWatchQueryEventComparer
{
public:
    bool operator()(const TWatchQueryEvent& lhsEvent, const TWatchQueryEvent& rhsEvent) const
    {
        auto eventToTuple = [] (const TWatchQueryEvent& event) {
            return std::tuple(
                event.Timestamp,
                event.EventIndex.Tablet,
                event.EventIndex.Row,
                event.EventIndex.Event);
        };

        return eventToTuple(lhsEvent) < eventToTuple(rhsEvent);
    }
};

bool operator<(TWatchQueryEventIndex lhs, TWatchQueryEventIndex rhs)
{
    return std::pair(lhs.Row, lhs.Event) < std::pair(rhs.Row, rhs.Event);
}

bool operator>=(TWatchQueryEventIndex lhs, TWatchQueryEventIndex rhs)
{
    return std::pair(lhs.Row, lhs.Event) >= std::pair(rhs.Row, rhs.Event);
}

////////////////////////////////////////////////////////////////////////////////

class TEnoughEventsChecker
{
public:
    TEnoughEventsChecker(
        const std::vector<TOrderedTabletReaderPtr>& tabletReaders,
        i64 eventCountLimit)
        : TabletReaders_(tabletReaders)
        , EventCountLimit_(eventCountLimit)
    {
        YT_VERIFY(eventCountLimit > 0);
        EnoughPerTabletReader_.reserve(tabletReaders.size());
        for (const auto& reader : tabletReaders) {
            EnoughPerTabletReader_.push_back(!reader->HasMoreRows());
        }
    }

    bool IsEnoughForTabletReader(i64 tabletReaderIndex) const
    {
        return EnoughPerTabletReader_[tabletReaderIndex];
    }

    bool IsEnough() const
    {
        for (auto isEnough : EnoughPerTabletReader_) {
            if (!isEnough) {
                return false;
            }
        }
        return true;
    }

    void UpdateState(const std::vector<TWatchQueryEvent>& events)
    {
        for (size_t i = 0; i < TabletReaders_.size(); ++i) {
            EnoughPerTabletReader_[i] |= !TabletReaders_[i]->HasMoreRows();
        }

        if (auto estimatedEventMaxTimestamp = EstimateAcceptedEventMaxTimestamp(events)) {
            for (const auto& event : events) {
                if (event.Timestamp > *estimatedEventMaxTimestamp) {
                    EnoughPerTabletReader_[event.TabletReaderIndex] = true;
                }
            }
        }
    }

private:
    const std::vector<TOrderedTabletReaderPtr>& TabletReaders_;
    const i64 EventCountLimit_;

    std::vector<bool> EnoughPerTabletReader_;

    mutable std::vector<TWatchQueryEvent> AcceptedEventsBuffer_;

    // Events in the response have timestamps smaller or equal to
    // the timestamp of the accepted event with index #EventCountLimit_ - 1,
    // because the result contains no more than #EventCountLimit_ events.
    //
    // So an event with a greater timestamp is of no interest.
    //
    // So if the tablet reader managed to read further, we can stop it.
    std::optional<TTimestamp> EstimateAcceptedEventMaxTimestamp(const std::vector<TWatchQueryEvent>& events) const
    {
        if (std::ssize(events) <= EventCountLimit_) {
            return std::nullopt;
        }

        AcceptedEventsBuffer_.clear();
        AcceptedEventsBuffer_.reserve(events.size());
        for (const auto& event : events) {
            if (event.Accepted) {
                AcceptedEventsBuffer_.push_back(event);
            }
        }

        if (std::ssize(AcceptedEventsBuffer_) <= EventCountLimit_) {
            return std::nullopt;
        }

        std::nth_element(
            AcceptedEventsBuffer_.begin(),
            AcceptedEventsBuffer_.begin() + EventCountLimit_ - 1,
            AcceptedEventsBuffer_.end(),
            TWatchQueryEventComparer());

        return AcceptedEventsBuffer_[EventCountLimit_ - 1].Timestamp;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBatchSizeReconciler
{
public:
    TBatchSizeReconciler(
        const std::vector<TOrderedTabletReaderPtr>& tabletReaders,
        i64 eventCountLimit,
        i64 perTabletSelectLimit)
        : TabletReaders_(tabletReaders)
        , EventCountLimit_(eventCountLimit)
        , PerTabletSelectLimit_(perTabletSelectLimit)
        , DefaultBatchSize_(std::min(
            static_cast<i64>(EventCountLimit_ * 1.2 / std::ssize(TabletReaders_) + 10),
            PerTabletSelectLimit_))
    {
        PerReaderNextBatchSize_.reserve(tabletReaders.size());
    }

    i64 EstimateNextBatchSize(int readerIndex) const
    {
        return GetOrDefault(PerReaderNextBatchSize_, readerIndex, DefaultBatchSize_);
    }

    void Reconcile(int readerIndex, i64 processedEventCount, i64 acceptedEventCount)
    {
        if (processedEventCount == 0) {
            return;
        }
        TotalAcceptedEventCount_ += acceptedEventCount;

        i64 currentBatchSize = EstimateNextBatchSize(readerIndex);
        i64 nextBatchSize = currentBatchSize * 2;
        if (acceptedEventCount) {
            double acceptedEventsRatio = static_cast<double>(acceptedEventCount) / processedEventCount;
            nextBatchSize = std::max(
                std::min({
                    nextBatchSize,
                    static_cast<i64>((EventCountLimit_ - TotalAcceptedEventCount_) /
                        acceptedEventsRatio /
                        TabletReaders_.size()),
                    static_cast<i64>(nextBatchSize / acceptedEventsRatio),
                }),
                currentBatchSize);
        }
        nextBatchSize = std::min(nextBatchSize, PerTabletSelectLimit_);
        YT_LOG_DEBUG_IF(nextBatchSize != currentBatchSize,
            "Increasing watch log read batch size (ReaderIndex: %v, CurrentBatchSize: %v, NextBatchSize: %v)",
            readerIndex,
            currentBatchSize,
            nextBatchSize);
        PerReaderNextBatchSize_[readerIndex] = nextBatchSize;
    }

private:
    const std::vector<TOrderedTabletReaderPtr>& TabletReaders_;
    const i64 EventCountLimit_;
    const i64 PerTabletSelectLimit_;
    const i64 DefaultBatchSize_;

    i64 TotalAcceptedEventCount_{0};
    THashMap<int, i64> PerReaderNextBatchSize_;
};

////////////////////////////////////////////////////////////////////////////////

class TWatchEventCollector
{
public:
    TWatchEventCollector(
        const std::vector<TOrderedTabletReaderPtr>& tabletReaders,
        i64 eventCountLimit,
        i64 perTabletSelectLimit,
        const IWatchLogEventMatcher* const eventMatcher,
        TWatchLogChangedAttributesConfigPtr watchLogChangedAttributesConfig,
        const std::vector<TWatchQueryEventIndex>& offsets,
        bool fetchChangedAttributes)
        : TabletReaders_(tabletReaders)
        , EventCountLimit_(eventCountLimit)
        , EventMatcher_(eventMatcher)
        , WatchLogChangedAttributesConfig_(std::move(watchLogChangedAttributesConfig))
        , InitialOffsets_(offsets)
        , FetchChangedAttributes_(fetchChangedAttributes)
        , BatchSizeReconciler_(tabletReaders, eventCountLimit, perTabletSelectLimit)
        , EnoughChecker_(tabletReaders, eventCountLimit)
        , Offsets_(offsets)
    {
        YT_VERIFY(EventCountLimit_ > 0);
    }

    bool IsEnoughForTabletReader(int tabletReaderIndex) const
    {
        return EnoughChecker_.IsEnoughForTabletReader(tabletReaderIndex);
    }

    void UpdateState()
    {
        EnoughChecker_.UpdateState(Events_);
    }

    bool IsEnough() const
    {
        return EnoughChecker_.IsEnough();
    }

    i64 GetAcceptedEventCount() const
    {
        return AcceptedEventCount_;
    }

    i64 GetTabletSelectLimit(int readerIndex) const
    {
        return BatchSizeReconciler_.EstimateNextBatchSize(readerIndex);
    }

    void HandleRows(int readerIndex, NQueueClient::IQueueRowsetPtr rowset)
    {
        if (rowset->GetRows().Empty()) {
            if (rowset->GetStartOffset() > Offsets_[readerIndex].Row) {
                Offsets_[readerIndex].Row = rowset->GetStartOffset();
            }
            return;
        }
        auto expectedTabletIndex = TabletReaders_[readerIndex]->GetTabletIndex();
        auto acceptedEventCountBefore = GetAcceptedEventCount();
        i64 processedEventCount = 0;
        i64 dummyCount = 0;
        for (const auto& row : rowset->GetRows()) {
            int tabletIndex = 0;
            i64 rowIndex = 0;
            TTimestamp timestamp = 0;
            NProto::TWatchRecord record;

            FromUnversionedRow(
                row,
                &tabletIndex,
                &rowIndex,
                &timestamp,
                &record);

            YT_VERIFY(tabletIndex == expectedTabletIndex);

            processedEventCount += record.events_size();
            dummyCount += record.dummy();

            Append(
                rowIndex,
                tabletIndex,
                readerIndex,
                timestamp,
                std::move(record));
        }
        auto acceptedEventCount = GetAcceptedEventCount() - acceptedEventCountBefore;
        YT_VERIFY(acceptedEventCount >= 0);
        YT_LOG_DEBUG("Appended watch log events (ReaderIndex: %v, Count: %v, AcceptedCount: %v, DummyCount: %v)",
            readerIndex,
            processedEventCount,
            acceptedEventCount,
            dummyCount);
        BatchSizeReconciler_.Reconcile(
            readerIndex,
            processedEventCount - dummyCount,
            acceptedEventCount);
    }

    std::vector<TWatchQueryEvent> Finalize()
    {
        // Select and sort accepted events.
        // Stable partition is used to ensure order between events of a tablet.
        auto acceptedEventsItEnd = std::stable_partition(
            Events_.begin(),
            Events_.end(),
            [] (const auto& event) {
                return event.Accepted;
            });
        std::sort(Events_.begin(), acceptedEventsItEnd, TWatchQueryEventComparer());

        // Define resulting events.
        size_t limit = std::min(
            std::distance(Events_.begin(), acceptedEventsItEnd),
            EventCountLimit_);

        std::vector<std::optional<TWatchQueryEventIndex>> offsets(Offsets_.size());
        auto updateOffset = [&] (size_t tabletReaderIndex, TWatchQueryEventIndex newOffset) {
            if (offsets[tabletReaderIndex]) {
                // Expected that `commit_ordering=strong` for watch-logs.
                YT_VERIFY(*offsets[tabletReaderIndex] < newOffset);
            }
            offsets[tabletReaderIndex] = newOffset;
        };

        // Define offsets of resulting events.
        for (size_t i = 0; i < limit; ++i) {
            const auto& event = Events_[i];
            updateOffset(event.TabletReaderIndex, event.EventIndex);
        }

        // Ensure progress even if no accepted event was found.
        // Also, skips irrelevant events for future requests.
        //
        // NB! Relies on fact, that accepted events are processed before others.
        std::vector<bool> foundAcceptedEvent(Offsets_.size());
        // Advance every offset by one event to find offsets of next events.
        //
        // If the offset points to the last event within the row,
        // the next event will be the first event of the next row.
        //
        // It is easy to find out whether an event points to the last event within the row,
        // because the last event of any row is always presented in the #Events_ vector.
        std::vector<bool> lastEventInRow(Offsets_.size(), true);
        for (i64 index = limit; index < std::ssize(Events_); ++index) {
            const auto& event = Events_[index];
            if (event.Accepted) {
                foundAcceptedEvent[event.TabletReaderIndex] = true;
            } else if (!foundAcceptedEvent[event.TabletReaderIndex] &&
                (!offsets[event.TabletReaderIndex] ||
                    *offsets[event.TabletReaderIndex] < event.EventIndex))
            {
                updateOffset(event.TabletReaderIndex, event.EventIndex);
            }
            if (offsets[event.TabletReaderIndex] &&
                offsets[event.TabletReaderIndex]->Row == event.EventIndex.Row &&
                offsets[event.TabletReaderIndex]->Event < event.EventIndex.Event)
            {
                lastEventInRow[event.TabletReaderIndex] = false;
            }
        }
        for (size_t tabletReaderIndex = 0; tabletReaderIndex < Offsets_.size(); ++tabletReaderIndex) {
            if (offsets[tabletReaderIndex]) {
                if (lastEventInRow[tabletReaderIndex]) {
                    offsets[tabletReaderIndex] = TWatchQueryEventIndex{
                        .Tablet = offsets[tabletReaderIndex]->Tablet,
                        .Row = offsets[tabletReaderIndex]->Row + 1,
                        .Event = 0,
                        .Timestamp = offsets[tabletReaderIndex]->Timestamp,
                    };
                } else {
                    ++offsets[tabletReaderIndex]->Event;
                }
            }
        }

        for (size_t tabletReaderIndex = 0; tabletReaderIndex < Offsets_.size(); ++tabletReaderIndex) {
            auto pendingOffset = offsets[tabletReaderIndex].value_or(InitialOffsets_[tabletReaderIndex]);
            if (foundAcceptedEvent[tabletReaderIndex] || Offsets_[tabletReaderIndex] < pendingOffset) {
                Offsets_[tabletReaderIndex] = pendingOffset;
            }
        }

        // Move the result.
        Events_.resize(limit);
        return std::move(Events_);
    }

    const std::vector<TWatchQueryEventIndex>& GetOffsets() const
    {
        return Offsets_;
    }

private:
    const std::vector<TOrderedTabletReaderPtr>& TabletReaders_;
    const i64 EventCountLimit_;
    const IWatchLogEventMatcher* const EventMatcher_;
    const TWatchLogChangedAttributesConfigPtr WatchLogChangedAttributesConfig_;
    const std::vector<TWatchQueryEventIndex> InitialOffsets_;
    const bool FetchChangedAttributes_ = false;

    TBatchSizeReconciler BatchSizeReconciler_;
    TEnoughEventsChecker EnoughChecker_;
    std::vector<TWatchQueryEventIndex> Offsets_;
    std::vector<TWatchQueryEvent> Events_;

    i64 AcceptedEventCount_ = 0;

    void Append(
        i64 rowIndex,
        int tabletIndex,
        i64 readerIndex,
        TTimestamp timestamp,
        NProto::TWatchRecord record)
    {
        // Offset.Row may differ from rowIndex because of a concurrent trim.
        YT_VERIFY(rowIndex >= Offsets_[readerIndex].Row);

        if (record.dummy()) {
            YT_VERIFY(record.events_size() == 0);
        }

        for (int eventIndex = 0; eventIndex < record.events_size(); ++eventIndex) {
            const auto& protoEvent = record.events(eventIndex);

            TWatchQueryEvent event;
            event.TabletReaderIndex = readerIndex;
            event.EventIndex = TWatchQueryEventIndex{
                .Tablet = tabletIndex,
                .Row = rowIndex,
                .Event = eventIndex,
                .Timestamp = timestamp,
            };
            event.Timestamp = timestamp;
            event.TypeName = protoEvent.type();
            event.ObjectId = protoEvent.object_id();
            event.Meta = NYson::TYsonString(protoEvent.meta_yson());
            event.Accepted = EventMatcher_->Match(protoEvent, WatchLogChangedAttributesConfig_)
                .ValueOrThrow();

            if (record.has_history_timestamp()) {
                event.HistoryTime.emplace<TTimestamp>(record.history_timestamp());
            }

            if (record.has_history_time()) {
                event.HistoryTime.emplace<TInstant>(NProtoInterop::CastFromProto(record.history_time()));
            }

            FromProto(&event.TransactionContext, record.transaction_context());
            if (FetchChangedAttributes_ && event.TypeName == ObjectUpdatedEventTypeName) {
                if (WatchLogChangedAttributesConfig_) {
                    if (WatchLogChangedAttributesConfig_->MD5 != protoEvent.changed_attributes().paths_md5()) {
                        THROW_ERROR_EXCEPTION(NClient::EErrorCode::WatchesConfigurationMismatch,
                            "Watch log event is not compatible with the current master configuration: "
                            "expected changed attributes summary MD5 hash %v in event, but got %v",
                            WatchLogChangedAttributesConfig_->MD5,
                            protoEvent.changed_attributes().paths_md5())
                            << TErrorAttribute("tablet", event.EventIndex.Tablet)
                            << TErrorAttribute("row", event.EventIndex.Row);
                    }
                } else {
                    if (!protoEvent.changed_attributes().paths_md5().empty()) {
                        THROW_ERROR_EXCEPTION(NClient::EErrorCode::WatchesConfigurationMismatch,
                            "Watch log event is not compatible with the current master configuration: "
                            "expected empty event summary, but got %v",
                            protoEvent.changed_attributes().paths_md5())
                            << TErrorAttribute("tablet", event.EventIndex.Tablet)
                            << TErrorAttribute("row", event.EventIndex.Row);
                    }
                }
                event.ChangedAttributesSummary = protoEvent.changed_attributes().changed_summary();
            }

            TTagSet changedTagsSet;
            for (const auto& changedAttributeTags : protoEvent.changed_attribute_tags()) {
                changedTagsSet |= FromProto(changedAttributeTags);
            }
            event.ChangedTags = TagsSetToVector(changedTagsSet);

            // Offset.Event matters here.
            if (event.EventIndex >= Offsets_[readerIndex]) {
                Events_.push_back(event);
                AcceptedEventCount_ += event.Accepted;
            }
        }

        TWatchQueryEventIndex newOffset{
            .Tablet = tabletIndex,
            .Row = rowIndex + 1,
            .Event = 0,
            .Timestamp = timestamp,
        };
        YT_VERIFY(Offsets_[readerIndex] < newOffset);
        Offsets_[readerIndex] = newOffset;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWatchLogReader
    : public TRefCounted
{
public:
    TWatchLogReader(
        ISession* const session,
        const TWatchQueryOptions& options,
        const TDBTable* table,
        const std::vector<NYT::NApi::TTabletInfo>& tabletInfos,
        std::unique_ptr<IWatchLogEventMatcher> eventMatcher,
        TWatchLogChangedAttributesConfigPtr watchLogChangedAttributesConfig,
        std::optional<TString> consumerPath)
        : Session_(session)
        , Options_(options)
        , EventMatcher_(std::move(eventMatcher))
        , WatchLogChangedAttributesConfig_(std::move(watchLogChangedAttributesConfig))
    {
        YT_VERIFY(options.Tablets.size() == tabletInfos.size());

        for (int index = 0; index < std::ssize(options.Tablets); ++index) {
            TabletReaders_.push_back(New<TOrderedTabletReader>(
                *Options_.Timestamp,
                table,
                options.Tablets[index],
                tabletInfos[index],
                session,
                consumerPath,
                options.SkipTrimmed));
        }
    }

    void ScheduleReadFromEarliestOffsets()
    {
        for (const auto& reader : TabletReaders_) {
            reader->SetEarliestOffset();
        }
    }

    void ScheduleReadFromLastCommittedOffsets()
    {
        for (const auto& reader : TabletReaders_) {
            reader->SetLastCommittedOffset();
        }
    }

    void ScheduleOffsetByTimestampSearch(TTimestamp startTimestamp)
    {
        for (const auto& reader : TabletReaders_) {
            reader->ScheduleSearchOffset(startTimestamp);
        }
    }

    void UpdateOffsets(std::vector<TWatchQueryEventIndex> eventOffsets)
    {
        EventOffsets_ = std::move(eventOffsets);
        YT_LOG_DEBUG("Updating event offsets (EventOffsets: %v)",
            EventOffsets_);

        YT_VERIFY(EventOffsets_.size() == TabletReaders_.size());
        for (size_t i = 0; i < EventOffsets_.size(); ++i) {
            TabletReaders_[i]->SetOffset(EventOffsets_[i].Row, EventOffsets_[i].Timestamp);
        }
    }

    THashMap<int, i64> GetPerTabletSkippedRows() const
    {
        THashMap<int, i64> perTabletSkippedRows;
        for (const auto& reader : TabletReaders_) {
            if (auto skippedRows = reader->GetSkippedRows(); skippedRows > 0) {
                perTabletSkippedRows[reader->GetTabletIndex()] = skippedRows;
            }
        }
        return perTabletSkippedRows;
    }

    TTimestamp GetTimestamp() const
    {
        return *Options_.Timestamp;
    }

    const std::vector<TWatchQueryEventIndex>& GetEventOffsets()
    {
        Session_->FlushLoads();

        if (EventOffsets_.empty()) {
            for (const auto& reader : TabletReaders_) {
                EventOffsets_.push_back(TWatchQueryEventIndex{
                    .Tablet = reader->GetTabletIndex(),
                    .Row = reader->GetOffset(),
                    .Event = 0,
                    .Timestamp = reader->GetTimestamp(),
                });
            }
        }

        return EventOffsets_;
    }

    std::vector<TWatchQueryEvent> Read(i64 perTabletSelectLimit)
    {
        auto eventCountLimit = *Options_.EventCountLimit;
        auto timeLimit = *Options_.ReadTimeLimit;
        int tabletCount = std::ssize(TabletReaders_);

        TWatchEventCollector collector(
            TabletReaders_,
            eventCountLimit,
            perTabletSelectLimit,
            EventMatcher_.get(),
            WatchLogChangedAttributesConfig_,
            GetEventOffsets(),
            Options_.FetchChangedAttributes);

        NProfiling::TWallTimer timer;
        do {
            for (int tabletReaderIndex = 0; tabletReaderIndex < tabletCount; ++tabletReaderIndex) {
                if (collector.IsEnoughForTabletReader(tabletReaderIndex)) {
                    continue;
                }

                TabletReaders_[tabletReaderIndex]->ScheduleRead(
                    std::bind_front(&TWatchEventCollector::HandleRows, &collector, tabletReaderIndex),
                    // NB! Limit must be present here to ensure order between events of a tablet.
                    collector.GetTabletSelectLimit(tabletReaderIndex));
            }

            Session_->FlushLoads();
            UpdateOffsets(collector.GetOffsets());

            collector.UpdateState();
        } while (!collector.IsEnough() && timer.GetElapsedTime() < timeLimit);

        auto events = collector.Finalize();
        UpdateOffsets(collector.GetOffsets());

        return events;
    }

private:
    ISession* const Session_;
    const TWatchQueryOptions& Options_;
    const std::unique_ptr<IWatchLogEventMatcher> EventMatcher_;
    const TWatchLogChangedAttributesConfigPtr WatchLogChangedAttributesConfig_;

    std::vector<TOrderedTabletReaderPtr> TabletReaders_;
    std::vector<TWatchQueryEventIndex> InitialEventOffsets_;
    std::vector<TWatchQueryEventIndex> EventOffsets_;
};

DECLARE_REFCOUNTED_CLASS(TWatchLogReader)
DEFINE_REFCOUNTED_TYPE(TWatchLogReader)

} // namespace

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchQueryOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat(
        "ObjectType: %v",
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrThrow(options.ObjectType));
    wrapper->AppendFormat("WatchLog: %v", options.WatchLog);
    wrapper->AppendFormat("Tablets: %v", options.Tablets);

    wrapper->AppendFormat("InitialOffsets: %v", options.InitialOffsets);
    wrapper->AppendFormat("Consumer: %v", options.Consumer);

    wrapper->AppendFormat("Timestamp: %v", options.Timestamp);
    wrapper->AppendFormat("TimeLimit: %v", options.TimeLimit);

    wrapper->AppendFormat("EventCountLimit: %v", options.EventCountLimit);
    wrapper->AppendFormat("ReadTimeLimit: %v", options.ReadTimeLimit);

    wrapper->AppendFormat("SkipTrimmed: %v", options.SkipTrimmed);
    wrapper->AppendFormat("FetchChangedAttributes: %v", options.FetchChangedAttributes);

    wrapper->AppendFormat("Filter: %v", options.Filter);
    wrapper->AppendFormat("Selector: %v", options.Selector);
    wrapper->AppendFormat("RequiredTags: %v", options.RequiredTags);
    wrapper->AppendFormat("ExcludedTags: %v", options.ExcludedTags);
    builder->AppendChar('}');
}

void FormatValue(
    TStringBuilderBase* builder,
    const TStartFromEarliestOffsetTag& /*tag*/,
    TStringBuf /*format*/)
{
    builder->AppendString("{StartFromEarliestOffset: True}");
}

void FormatValue(
    TStringBuilderBase* builder,
    const TStartFromLastCommittedOffsetTag& /*tag*/,
    TStringBuf /*format*/)
{
    builder->AppendString("{StartFromLastCommittedOffset: True}");
}

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchObjectsContinuationToken& token,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("WatchLog: %v", token.WatchLog);
    wrapper->AppendFormat("DbVersion: %v", token.DbVersion);
    wrapper->AppendFormat("TabletCount: %v", token.TabletCount);
    wrapper->AppendFormat("EventOffsets: %v", token.EventOffsets);
    wrapper->AppendFormat("SerializedToken: %v", token.SerializedToken);
    builder->AppendChar('}');
}

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchQueryEventIndex& index,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");
    builder->AppendChar('{');
    wrapper->AppendFormat("Tablet: %v", index.Tablet);
    wrapper->AppendFormat("Row: %v", index.Row);
    wrapper->AppendFormat("Event: %v", index.Event);
    wrapper->AppendFormat("Timestamp: %v", index.Timestamp);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

class TWatchQueryExecutor
    : public IWatchQueryExecutor
{
public:
    TWatchQueryExecutor(NMaster::IBootstrap* bootstrap, ISession* session)
        : Bootstrap_(bootstrap)
        , WatchManager_(bootstrap->GetWatchManager())
        , Config_(WatchManager_->GetExtendedConfig())
        , Session_(session)
    {
        THROW_ERROR_EXCEPTION_UNLESS(GetConfig()->Enabled,
            "Watch logs are disabled now");
    }

    TWatchQueryResult ExecuteWatchQuery(TWatchQueryOptions options) const override
    {
        YT_LOG_DEBUG("Executing watch query (Options: %v)", options);

        auto currentDBVersion = Bootstrap_->GetYTConnector()->GetExpectedDBVersion();

        ValidateWatchQueryOptions(options);
        auto objectType = options.ObjectType;
        i64 ResultEventCountLimit = GetConfig()->ResultEventCountLimit;
        if (options.EventCountLimit) {
            if (*options.EventCountLimit > ResultEventCountLimit || *options.EventCountLimit <= 0) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Event count limit must be in range [1, %v], but got %v",
                    ResultEventCountLimit,
                    *options.EventCountLimit);
            }
        } else {
            options.EventCountLimit = ResultEventCountLimit;
        }

        Visit(options.InitialOffsets,
            [&] (TWatchObjectsContinuationToken& continuationToken) {
                ValidateContinuationTokenDbVersion(&continuationToken);
                ValidateOptionsAndTokenMatch(&continuationToken, &options);
            },
            [&] (const TStartFromLastCommittedOffsetTag&) {
                THROW_ERROR_EXCEPTION_UNLESS(options.Consumer,
                    "A consumer must be specified for reading from last committed offset");
            },
            [&] (const TStartFromEarliestOffsetTag&) { },
            [&] (const TTimestamp& /*timestamp*/) { });

        auto log = FindAppropriateWatchLog(options);
        const auto* logTable = WatchManager_->GetLogTableOrCrash(objectType, log.Name);

        auto initialTableMountInfo = WaitFor(GetTableMountInfo(Bootstrap_->GetYTConnector(), logTable))
            .ValueOrThrow();
        int initialTabletCount = std::ssize(initialTableMountInfo->Tablets);

        bool tabletsWereExplicitlySet = !options.Tablets.empty();
        if (!tabletsWereExplicitlySet) {
            if (auto* token = std::get_if<TWatchObjectsContinuationToken>(&options.InitialOffsets))
            {
                if (token->ReadAllTablets) {
                    while (initialTabletCount > std::ssize(token->EventOffsets)) {
                        token->EventOffsets.push_back(TWatchQueryEventIndex{
                            .Tablet = static_cast<int>(std::ssize(token->EventOffsets))
                        });
                    }
                } else {
                    tabletsWereExplicitlySet = true;
                }
                options.Tablets.reserve(token->EventOffsets.size());
                std::ranges::transform(
                    token->EventOffsets,
                    std::back_inserter(options.Tablets),
                    std::identity{},
                    &TWatchQueryEventIndex::Tablet);
            } else {
                options.Tablets.resize(initialTableMountInfo->Tablets.size(), 0);
                std::iota(options.Tablets.begin(), options.Tablets.end(), 0);
            }
        }

        auto initialTabletInfos = PrepareInitialTabletInfos(
            objectType,
            log.Name,
            options.Tablets,
            options.Timestamp,
            options.TimeLimit,
            /*fromContinuationToken*/ std::holds_alternative<TWatchObjectsContinuationToken>(options.InitialOffsets));
        YT_VERIFY(options.Tablets.size() == initialTabletInfos.size());

        if (!options.Timestamp) {
            options.Timestamp = GetBarrierTimestamp(initialTabletInfos);
        }
        if (!options.ReadTimeLimit) {
            options.ReadTimeLimit.emplace(TDuration::Max());
        }

        auto changedAttributesConfig = Config_.ChangedAttributes
            ->TryGetLogChangedAttributesConfig(objectType, log.Name);
        YT_VERIFY(changedAttributesConfig);

        auto reader = CreateWatchLogReader(options, logTable, initialTabletInfos, changedAttributesConfig);

        TWatchQueryResult result;

        if (options.FetchChangedAttributes) {
            result.ChangedAttributes.resize(changedAttributesConfig->PathToIndex.size());
            for (const auto& [attribute, index] : changedAttributesConfig->PathToIndex) {
                YT_VERIFY(index < changedAttributesConfig->PathToIndex.size());
                result.ChangedAttributes[index] = attribute;
            }
        }

        Visit(options.InitialOffsets,
            [&] (TWatchObjectsContinuationToken& token) {
                ValidateTokenTabletData(
                    token,
                    initialTabletInfos,
                    initialTabletCount);
                reader->UpdateOffsets(token.EventOffsets);
            },
            [&] (const TStartFromEarliestOffsetTag&) {
                reader->ScheduleReadFromEarliestOffsets();
            },
            [&] (const TStartFromLastCommittedOffsetTag&) {
                reader->ScheduleReadFromLastCommittedOffsets();
            },
            [&] (const TTimestamp& timestamp) {
                reader->ScheduleOffsetByTimestampSearch(timestamp);
            });

        auto initialEventOffsets = reader->GetEventOffsets();

        result.Timestamp = reader->GetTimestamp();
        result.Events = reader->Read(GetConfig()->PerTabletSelectLimit);
        THROW_ERROR_EXCEPTION_IF(std::ssize(result.Events) > ResultEventCountLimit,
            "Result must contain no more than %v events",
            ResultEventCountLimit);

        const auto& offsets = reader->GetEventOffsets();

        auto tabletInfos = WaitFor(WatchManager_->GetTabletInfos(
            objectType,
            log.Name,
            options.Tablets))
            .ValueOrThrow();
        YT_VERIFY(options.Tablets.size() == tabletInfos.size());
        YT_VERIFY(initialEventOffsets.size() == tabletInfos.size());

        auto tableMountInfo = WaitFor(GetTableMountInfo(Bootstrap_->GetYTConnector(), logTable))
            .ValueOrThrow();
        i64 currentTabletCount = tableMountInfo->Tablets.size();
        if (!tabletsWereExplicitlySet && currentTabletCount != initialTabletCount) {
            ThrowTabletCountChange(log.Name, initialTabletCount, currentTabletCount);
        }
        for (const auto& offset : offsets) {
            if (offset.Tablet >= std::ssize(tableMountInfo->Tablets)) {
                THROW_ERROR_EXCEPTION("Tablet index is out of range: expected less than %v, got %v",
                    std::ssize(tableMountInfo->Tablets),
                    offset.Tablet);
            }
        }

        result.ContinuationToken.EventOffsets = offsets;
        result.ContinuationToken.TabletCount = initialTabletCount;
        result.ContinuationToken.WatchLog = log.Name;
        result.ContinuationToken.DbVersion = currentDBVersion;
        result.ContinuationToken.ReadAllTablets = !tabletsWereExplicitlySet;

        if (auto perTabletSkippedRows = reader->GetPerTabletSkippedRows()) {
            for (const auto& [_, skippedRows] : perTabletSkippedRows) {
                result.SkippedRowCount += skippedRows;
            }
            YT_LOG_DEBUG(
                "Profiling watch log skipped row count ("
                "ObjectName: %v, "
                "LogName: %v, "
                "PerTabletSkippedRows: %v)",
                log.ObjectName,
                log.Name,
                perTabletSkippedRows);
            WatchManager_->UpdateQueryProfiling(
                objectType,
                log.Name, TWatchQueryProfilingValues{
                    .PerTabletSkippedRows = std::move(perTabletSkippedRows)
                });
        }

        return result;
    }

private:
    NMaster::IBootstrap* const Bootstrap_;
    const TWatchManagerPtr WatchManager_;
    const TWatchManagerExtendedConfig Config_;
    ISession* const Session_;

    TWatchManagerConfigPtr GetConfig() const
    {
        return Config_.WatchManager;
    }

    bool DoSelectorsMatch(
        const THashSet<TString>& querySelector,
        const TWatchLog& log,
        bool explicitWatchLog) const
    {
        auto changedAttributes = Config_.ChangedAttributes->TryGetLogChangedAttributesConfig(
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeValueByNameOrThrow(log.ObjectName),
            log.Name);
        // Log is disabled.
        if (!changedAttributes) {
            return false;
        }
        if (log.Selector.empty() || explicitWatchLog) {
            return std::all_of(
                querySelector.begin(),
                querySelector.end(),
                [&] (const auto& path) {
                    return changedAttributes->PathToIndex.contains(path);
                });
        }

        if (querySelector.empty()) {
            return false;
        }

        return std::all_of(
            querySelector.begin(),
            querySelector.end(),
            [&] (const auto& path) {
                return log.Selector.contains(path);
            });
    }

    bool DoFiltersMatch(
        const TObjectFilter& queryFilter,
        const TObjectFilter& watchLogFilter,
        bool explicitWatchLog) const
    {
        if (!watchLogFilter.Query) {
            return true;
        }
        if (explicitWatchLog) {
            return true;
        }
        if (!queryFilter.Query) {
            return false;
        }

        auto expressionFromSource = [] (const auto& source) -> const auto& {
            return *std::get<TExpressionPtr>(source->AstHead.Ast);
        };

        auto queryFilterSource = ParseSource(queryFilter.Query, NQueryClient::EParseMode::Expression);
        auto watchLogFilterSource = ParseSource(watchLogFilter.Query, NQueryClient::EParseMode::Expression);

        return expressionFromSource(queryFilterSource) == expressionFromSource(watchLogFilterSource);
    }

    bool DoTagSetsMatch(
        const TWatchLog& log,
        const TTagSet& requiredTags,
        const TTagSet& /*excludedTags*/,
        bool explicitWatchLog = false) const
    {
        if (explicitWatchLog) {
            return true;
        }

        if (!requiredTags && log.RequiredTags) {
            return false;
        }

        if (log.ExcludedTags & requiredTags) {
            return false;
        }

        auto notInRequiredTags = [&] (TTag tag) {
            return !log.RequiredTags.contains(tag);
        };
        if (log.RequiredTags && std::any_of(requiredTags.begin(), requiredTags.end(), notInRequiredTags)) {
            return false;
        }

        return true;
    }

    bool DoesLogMatch(
        const TWatchLog& log,
        const TWatchQueryOptions& options,
        bool explicitWatchLog) const
    {
        return GetConfig()->IsLogQueryEnabled(log.ObjectName, log.Name, explicitWatchLog) &&
            DoFiltersMatch(options.Filter, log.Filter, explicitWatchLog) &&
            DoSelectorsMatch(options.Selector, log, explicitWatchLog) &&
            DoTagSetsMatch(log, options.RequiredTags, options.ExcludedTags, explicitWatchLog);
    }

    TWatchLog FindAppropriateWatchLog(const TWatchQueryOptions& options) const
    {
        std::optional<TWatchLog> bestLog;

        if (options.WatchLog) {
            YT_LOG_DEBUG("Checking hinted watch log (LogName: %v)",
                options.WatchLog);
            const auto& log = WatchManager_->GetLogByNameOrThrow(options.ObjectType, options.WatchLog);
            if (!DoesLogMatch(log, options, /*explicitWatchLog*/ true)) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::NotWatchable,
                    "Requested %v watch log %Qv is not available for watching",
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(options.ObjectType),
                    options.WatchLog);
            }
            bestLog.emplace(log);
        } else {
            int tagCount = NClient::NObjects::GetGlobalTagsRegistry()->GetTagCount();
            auto isMoreSuitableThan = [tagCount] (const TWatchLog& left, const TWatchLog& right) {
                auto getSelectorAttributeCoverage = [] (const THashSet<TString>& selector) {
                    auto selectorPathCount = selector.size();
                    return !selectorPathCount
                        ? std::numeric_limits<size_t>::max()
                        : selectorPathCount;
                };

                int leftExcludedTags = left.ExcludedTags.size();
                int leftRequiredTags = left.RequiredTags.size();
                int leftTagsSelectivity = (leftRequiredTags == 0 ? -tagCount : -leftRequiredTags) + leftExcludedTags;
                int rightExcludedTags = right.ExcludedTags.size();
                int rightRequiredTags = right.RequiredTags.size();
                int rightTagsSelectivity = (rightRequiredTags == 0 ? -tagCount : -rightRequiredTags) + rightExcludedTags;

                return std::tuple(!left.Filter.Query, getSelectorAttributeCoverage(left.Selector), -leftTagsSelectivity) <
                    std::tuple(!right.Filter.Query, getSelectorAttributeCoverage(right.Selector), -rightTagsSelectivity);
            };

            YT_LOG_DEBUG("Looking for matching watch log (ObjectName: %v, Filter: %v, Selector: %v)",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(options.ObjectType),
                options.Filter,
                options.Selector);

            for (const auto& log : WatchManager_->GetLogs(options.ObjectType)) {
                if (DoesLogMatch(log, options, /*explicitWatchLog*/ false)) {
                    YT_LOG_DEBUG(
                        "Found matching watch log (ObjectName: %v, LogName: %v, LogFilter: %v, LogSelector: %v)",
                        log.ObjectName,
                        log.Name,
                        log.Filter.Query,
                        log.Selector);

                    if (!bestLog || isMoreSuitableThan(log, *bestLog)) {
                        bestLog = log;
                    }
                }
            }
        }

        if (!bestLog) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::NotWatchable,
                "Could not find appropriate watch log for object %Qv for the given selector %v and filter %Qv",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(options.ObjectType),
                options.Selector,
                options.Filter.Query);
        }
        YT_LOG_DEBUG("Chosen watch log for given query (ObjectName: %v, LogName: %v)",
            bestLog->ObjectName,
            bestLog->Name);
        return std::move(*bestLog);
    }

    std::vector<TTabletInfo> PrepareInitialTabletInfos(
        TObjectTypeValue objectType,
        const TString& logName,
        const std::vector<int>& tablets,
        std::optional<TTimestamp> endTimestamp,
        std::optional<TDuration> timeLimit,
        bool fromContinuationToken) const
    {
        TErrorOr<std::vector<TTabletInfo>> tabletInfosOrError;
        if (endTimestamp && timeLimit) {
            tabletInfosOrError = WatchManager_->WaitForBarrier(
                objectType,
                logName,
                tablets,
                *endTimestamp,
                *timeLimit);
        } else {
            tabletInfosOrError = WaitFor(WatchManager_->GetTabletInfos(
                objectType,
                logName,
                tablets));
            if (tabletInfosOrError.IsOK() &&
                endTimestamp && GetBarrierTimestamp(tabletInfosOrError.Value()) < *endTimestamp)
            {
                tabletInfosOrError = TError("End timestamp %v is not available yet",
                    *endTimestamp);
            }
        }
        if (tabletInfosOrError.FindMatching(NTabletClient::EErrorCode::NoSuchTablet)) {
            tabletInfosOrError = std::move(tabletInfosOrError).Wrap(fromContinuationToken
                ? NClient::EErrorCode::InvalidContinuationToken
                : NClient::EErrorCode::InvalidRequestArguments,
                "Invalid tablets passed for %v watch log %Qv",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
                logName)
                << TErrorAttribute("tablets", tablets);
        }

        return tabletInfosOrError.ValueOrThrow();
    }

    TWatchLogReaderPtr CreateWatchLogReader(
        const TWatchQueryOptions& options,
        const TDBTable* logTable,
        const std::vector<TTabletInfo>& tabletInfos,
        TWatchLogChangedAttributesConfigPtr watchLogChangedAttributesConfig) const
    {
        auto eventMatcher = CreateWatchLogEventMatcher(
            options.Filter,
            options.Selector,
            options.RequiredTags,
            options.ExcludedTags,
            GetConfig()->ValidateEventTags,
            GetConfig()->AllowFilteringByMeta);

        std::optional<TYPath> consumerPath;
        if (options.Consumer) {
            consumerPath = Bootstrap_->GetYTConnector()->GetConsumerPath(*options.Consumer);
        }
        return New<TWatchLogReader>(
            Session_,
            options,
            logTable,
            tabletInfos,
            std::move(eventMatcher),
            std::move(watchLogChangedAttributesConfig),
            std::move(consumerPath));
    }

    void ValidateWatchQueryOptions(const TWatchQueryOptions& options) const
    {
        auto objectType = options.ObjectType;
        if (options.Filter.Query && !GetConfig()->IsQueryFilterEnabled(objectType)) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Watch log for the object %Qv does not support query filter",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
        }
        if ((!options.Selector.empty() || options.FetchChangedAttributes) &&
            !GetConfig()->IsQuerySelectorEnabled(objectType))
        {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Watch log for the object %Qv does not support query selector",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
        }
        ValidateSelectors(options.Selector,
            Bootstrap_->GetObjectManager()->GetTypeHandlerOrCrash(objectType),
            /*abortOnFail*/ false);
        if (THashSet<int>(options.Tablets.begin(), options.Tablets.end()).size() != options.Tablets.size()) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Requested tablets must not contain duplicates");
        }
        if (options.Timestamp.value_or(NullTimestamp) > Session_->GetOwner()->GetStartTimestamp()) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Requested timestamp %v is larger than current",
                *options.Timestamp);
        }
        for (auto tabletIndex : options.Tablets) {
            if (tabletIndex < 0) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Negative tablet index %v requested", tabletIndex);
            }
        }

        if (GetConfig()->ValidateRequestTags) {
            auto tagsRegistry = NClient::NObjects::GetGlobalTagsRegistry();
            auto unknownRequiredTags = tagsRegistry->FilterUnknownTags(options.RequiredTags);
            auto unknownExcludedTags = tagsRegistry->FilterUnknownTags(options.ExcludedTags);
            THROW_ERROR_EXCEPTION_UNLESS(unknownRequiredTags.empty(),
                NClient::EErrorCode::InvalidRequestArguments,
                "Requested unknown required tags: %v",
                unknownRequiredTags);
            THROW_ERROR_EXCEPTION_UNLESS(unknownExcludedTags.empty(),
                NClient::EErrorCode::InvalidRequestArguments,
                "Requested unknown excluded tags: %v",
                unknownExcludedTags);
        }

        THROW_ERROR_EXCEPTION_IF(
            (!options.RequiredTags.empty() || !options.ExcludedTags.empty()) &&
            !Session_->GetTypeHandlerOrCrash(objectType)->AreTagsEnabled(),
            "Cannot use required or excluded tags for object type %v with disabled tags",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(objectType));
    }

    void ValidateContinuationTokenDbVersion(const TWatchObjectsContinuationToken* continuationToken) const
    {
        if (continuationToken->DbVersion && GetConfig()->ValidateTokenMinorVersion) {
            auto minorVersion = Bootstrap_->GetYTConnector()->GetExpectedDBVersion();
            if (continuationToken->DbVersion != minorVersion) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidContinuationToken,
                    "Incorrect watch continuation token minor version: expected %v, but got %v",
                    minorVersion,
                    continuationToken->DbVersion);
            }
        }
    }

    void ValidateOptionsAndTokenMatch(
        const TWatchObjectsContinuationToken* continuationToken,
        TWatchQueryOptions* options) const
    {
        THROW_ERROR_EXCEPTION_IF(options->WatchLog && continuationToken->WatchLog != options->WatchLog,
            NClient::EErrorCode::InvalidContinuationToken,
            "Requested watch log %Qv for object %Qv, does not match continuation token watch log %Qv",
            options->WatchLog,
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrThrow(options->ObjectType),
            continuationToken->WatchLog);
        options->WatchLog = continuationToken->WatchLog;

        TError tabletMismatchError(NClient::EErrorCode::InvalidContinuationToken,
            "Tablet mismatch between continuation token and watch query options");

        THROW_ERROR_EXCEPTION_IF(
            !options->Tablets.empty() && options->Tablets.size() != continuationToken->EventOffsets.size(),
            tabletMismatchError);
        if (!options->Tablets.empty()) {
            for (int index = 0; index < std::ssize(continuationToken->EventOffsets); ++index) {
                if (options->Tablets[index] != continuationToken->EventOffsets[index].Tablet) {
                    THROW_ERROR tabletMismatchError;
                }
            }
        }
    }

    void ValidateTokenTabletData(
        const TWatchObjectsContinuationToken& continuationToken,
        const std::vector<TTabletInfo>& tabletInfos,
        int tabletCount) const
    {
        const auto& eventOffsets = continuationToken.EventOffsets;
        YT_VERIFY(eventOffsets.size() == tabletInfos.size());

        for (int index = 0; index < std::ssize(eventOffsets); ++index) {
            auto tablet = eventOffsets[index].Tablet;
            if (continuationToken.ReadAllTablets) {
                THROW_ERROR_EXCEPTION_IF(index != tablet,
                    NClient::EErrorCode::InvalidContinuationToken,
                    "Expected tablet index %v to be equal continuation token offset %v",
                    tablet,
                    index);
            }
            THROW_ERROR_EXCEPTION_IF(tablet < 0 || tablet >= tabletCount,
                NClient::EErrorCode::InvalidContinuationToken,
                "Tablet index %v in continuation token offset %v is out of range [0, %v)",
                tablet,
                index,
                tabletCount);
            THROW_ERROR_EXCEPTION_IF(eventOffsets[index].Row < 0,
                NClient::EErrorCode::InvalidContinuationToken,
                "Negative row index %v in continuation token offset %v",
                eventOffsets[index].Row,
                index);
            if (eventOffsets[index].Row > tabletInfos[index].TotalRowCount) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidContinuationToken,
                "Invalid row index %v in continuation token offset %v: expected <= %v",
                eventOffsets[index].Row,
                index,
                tabletInfos[index].TotalRowCount)
                << TErrorAttribute("tablet", tablet);
            }
            THROW_ERROR_EXCEPTION_IF(eventOffsets[index].Event < 0,
                NClient::EErrorCode::InvalidContinuationToken,
                "Negative event index %v in continuation token offset %v",
                eventOffsets[index].Event,
                index);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWatchQueryExecutor> MakeWatchQueryExecutor(TTransactionPtr transaction)
{
    return std::make_unique<TWatchQueryExecutor>(transaction->GetBootstrap(), transaction->GetSession());
}

////////////////////////////////////////////////////////////////////////////////

void ValidateInitialOffsetsConstruction(
    bool hasStartTimestamp,
    bool startFromEarliestOffset,
    bool hasContinuationToken)
{
    if (auto sum = static_cast<int>(hasContinuationToken) +
        static_cast<int>(hasStartTimestamp) +
        static_cast<int>(startFromEarliestOffset); sum > 1)
    {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
            "Cannot specify more than one option of start_timestamp, start_from_earliest_offset and continuation_token");
    } else if (sum < 1) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
            "Either start_timestamp, start_from_earliest_offset or continuation_token should be specified");
    }
}

std::vector<TWatchQueryEventIndex> ParseEventOffsets(
    const NProto::TWatchQueryContinuationToken& continuationToken)
{
    std::vector<TWatchQueryEventIndex> eventOffsets;
    eventOffsets.reserve(continuationToken.event_offsets_size());
    for (auto& eventOffset : continuationToken.event_offsets()) {
        eventOffsets.push_back({
            .Tablet = eventOffset.tablet_index(),
            .Row = eventOffset.row_index(),
            .Event = eventOffset.event_index(),
            .Timestamp = eventOffset.timestamp()
        });
    }

    return eventOffsets;
}

void FillEventOffsets(
    NProto::TWatchQueryContinuationToken& continuationToken,
    const std::vector<TWatchQueryEventIndex> offsets)
{
    for (const auto& offset : offsets) {
        auto* protoOffset = continuationToken.add_event_offsets();

        protoOffset->set_row_index(offset.Row);
        protoOffset->set_event_index(offset.Event);
        protoOffset->set_tablet_index(offset.Tablet);
        if (offset.Timestamp != NullTimestamp) {
            protoOffset->set_timestamp(offset.Timestamp);
        }
    }
}

constexpr i64 ContinuationTokenMajorVersion = 0;
TWatchObjectsContinuationToken DeserializeContinuationToken(const TString& serializedToken)
{
    NProto::TWatchQueryContinuationToken continuationToken;
    DeserializeContinuationToken(serializedToken, &continuationToken);
    THROW_ERROR_EXCEPTION_IF(continuationToken.log_name().empty(),
        "Incorrect watch continuation token: expected non-empty log name");
    THROW_ERROR_EXCEPTION_IF(continuationToken.major_version() != ContinuationTokenMajorVersion,
        NClient::EErrorCode::InvalidContinuationToken,
        "Incorrect watch continuation token major version: expected %v, but got %v",
        ContinuationTokenMajorVersion,
        continuationToken.major_version());

    return TWatchObjectsContinuationToken{
        .EventOffsets = ParseEventOffsets(continuationToken),
        .TabletCount = continuationToken.tablet_count(),
        .WatchLog = continuationToken.log_name(),
        .DbVersion = continuationToken.minor_version(),
        .SerializedToken = serializedToken,
        .ReadAllTablets = continuationToken.read_all_tablets(),
    };
}

TString SerializeContinuationToken(const TWatchObjectsContinuationToken& token)
{

    NProto::TWatchQueryContinuationToken newContinuationToken;
    FillEventOffsets(newContinuationToken, token.EventOffsets);
    newContinuationToken.set_tablet_count(token.TabletCount);
    newContinuationToken.set_log_name(token.WatchLog);
    newContinuationToken.set_major_version(ContinuationTokenMajorVersion);
    if (token.DbVersion.has_value()) {
        newContinuationToken.set_minor_version(token.DbVersion.value());
    }
    newContinuationToken.set_read_all_tablets(token.ReadAllTablets);
    return SerializeContinuationToken(newContinuationToken);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
