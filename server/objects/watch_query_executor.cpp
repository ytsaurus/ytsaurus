#include "watch_query_executor.h"

#include "continuation_token.h"
#include "db_schema.h"
#include "helpers.h"
#include "object_manager.h"
#include "private.h"
#include "tablet_reader.h"
#include "watch_manager.h"

#include <yp/server/objects/proto/continuation_token.pb.h>

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/client/api/client.h>
#include <yt/client/api/rowset.h>
#include <yt/client/table_client/helpers.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYP::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NConcurrency;
using namespace NYT::NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TWatchQueryEventComparer
{
public:
    bool operator()(const TWatchQueryEvent& lhsEvent, const TWatchQueryEvent& rhsEvent) const
    {
        static const auto TypePriority = BuildEventTypePriority();

        auto eventToTuple = [](const TWatchQueryEvent& event) {
            YT_VERIFY(event.Type != EEventType::None);
            return std::make_tuple(
                event.Timestamp,
                TypePriority[event.Type],
                event.ObjectId);
        };

        return eventToTuple(lhsEvent) < eventToTuple(rhsEvent);
    }

private:
    static TEnumIndexedVector<EEventType, int> BuildEventTypePriority()
    {
        TEnumIndexedVector<EEventType, int> result;
        result[EEventType::ObjectRemoved] = 0;
        result[EEventType::ObjectUpdated] = 1;
        result[EEventType::ObjectCreated] = 2;
        return result;
    }
};

class TEnoughEventsChecker
{
public:
    TEnoughEventsChecker(
        const std::vector<TTabletReaderPtr>& tabletReaders,
        std::optional<i64> eventCountLimit)
        : TabletReaders_(tabletReaders)
        , EventCountLimit_(eventCountLimit)
        , EnoughPerTablet_(tabletReaders.size(), false)
    { }

    bool IsEnoughForTablet(i64 tabletIndex) const
    {
        return EnoughPerTablet_[tabletIndex];
    }

    bool IsEnough() const
    {
        for (auto isEnough : EnoughPerTablet_) {
            if (!isEnough) {
                return false;
            }
        }
        return true;
    }

    void UpdateState(std::vector<TWatchQueryEvent>* events)
    {
        if (EventCountLimit_ && *EventCountLimit_ == 0) {
            std::fill(EnoughPerTablet_.begin(), EnoughPerTablet_.end(), true);
            return;
        }

        for (size_t i = 0; i < TabletReaders_.size(); ++i) {
            if (!TabletReaders_[i]->HasMoreRows()) {
                EnoughPerTablet_[i] = true;
            }
        }

        if (EventCountLimit_ && static_cast<i64>(events->size()) > *EventCountLimit_) {
            std::nth_element(
                events->begin(),
                events->begin() + *EventCountLimit_ - 1,
                events->end(),
                TWatchQueryEventComparer());

            for (size_t i = *EventCountLimit_; i < events->size(); ++i) {
                if ((*events)[i].Timestamp > (*events)[*EventCountLimit_ - 1].Timestamp) {
                    EnoughPerTablet_[(*events)[i].TabletIndex] = true;
                }
            }
        }
    }

private:
    const std::vector<TTabletReaderPtr>& TabletReaders_;
    const std::optional<i64> EventCountLimit_;
    std::vector<bool> EnoughPerTablet_;
};

void SetTabletReadersOffset(const std::vector<TWatchQueryEvent>& events, std::vector<TTabletReaderPtr>& readers)
{
    for (const auto& event : events) {
        const auto& reader = readers[event.TabletIndex];
        reader->SetOffset(std::max(reader->GetOffset(), event.RowIndex + 1));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchQueryOptions& options,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Timestamp: %llx, StartTimestamp: %llx, ContinuationToken: %Qv, EventCountLimit: %v, TimeLimit: %v}",
        options.Timestamp,
        options.StartTimestamp,
        options.ContinuationToken,
        options.EventCountLimit,
        options.TimeLimit);
}

////////////////////////////////////////////////////////////////////////////////

TWatchLogReader::TWatchLogReader(
    TTimestamp timestamp,
    const TDBTable* table,
    const std::vector<TTabletInfo>& tabletInfos,
    ISession* const session)
    : Timestamp_(timestamp)
    , Session_(session)
    , TabletCount_(tabletInfos.size())
{
    for (size_t tabletIndex = 0; tabletIndex < tabletInfos.size(); ++tabletIndex) {
        TabletReaders_.emplace_back(New<TTabletReader>(
            timestamp,
            table,
            tabletIndex,
            tabletInfos[tabletIndex],
            session));
    }
}

void TWatchLogReader::ScheduleRead(
    TTimestamp startTimestamp)
{
    for (const auto& reader : TabletReaders_) {
        reader->ScheduleSearchOffset(startTimestamp);
    }
}

void TWatchLogReader::ScheduleRead(
    const std::vector<i64>& tabletOffsets)
{
    YT_VERIFY(tabletOffsets.size() == TabletReaders_.size());
    for (size_t i = 0; i < tabletOffsets.size(); ++i) {
        TabletReaders_[i]->SetOffset(tabletOffsets[i]);
    }
}

TTimestamp TWatchLogReader::GetTimestamp() const
{
    return Timestamp_;
}

std::vector<i64> TWatchLogReader::GetTabletOffsets() const
{
    Session_->FlushLoads();
    std::vector<i64> results;
    results.reserve(TabletReaders_.size());
    for (const auto& reader : TabletReaders_) {
        results.emplace_back(reader->GetOffset());
    }
    return results;
}

std::vector<TWatchQueryEvent> TWatchLogReader::Read(std::optional<i64> eventCountLimit)
{
    const auto initialTabletOffsets = GetTabletOffsets();

    std::vector<TWatchQueryEvent> events;

    std::optional<i64> perTabletLimit;
    if (eventCountLimit) {
        // TODO(avitella): Introduce better magic formula.
        perTabletLimit = *eventCountLimit * 1.2 / TabletCount_ + 10;
    }

    TEnoughEventsChecker enoughChecker(TabletReaders_, eventCountLimit);

    do {
        for (i64 tabletIndex = 0; tabletIndex < TabletCount_; ++tabletIndex) {
            if (enoughChecker.IsEnoughForTablet(tabletIndex)) {
                continue;
            }

            auto handler = [tabletIndex, &events] (const IUnversionedRowsetPtr& rowset) {
                for (const auto& row : rowset->GetRows()) {
                    TWatchQueryEvent event;
                    event.TabletIndex = tabletIndex;
                    FromUnversionedRow(
                        row,
                        &event.RowIndex,
                        &event.Timestamp,
                        &event.ObjectId,
                        &event.Type);
                    events.push_back(event);
                }
            };

            TabletReaders_[tabletIndex]->ScheduleRead(
                {
                    &WatchLogSchema.Key.RowIndex,
                    &WatchLogSchema.Fields.Timestamp,
                    &WatchLogSchema.Fields.ObjectId,
                    &WatchLogSchema.Fields.EventType
                },
                std::move(handler),
                perTabletLimit);
        }

        Session_->FlushLoads();
        enoughChecker.UpdateState(&events);
        SetTabletReadersOffset(events, TabletReaders_);

    } while (!enoughChecker.IsEnough());

    std::sort(events.begin(), events.end(), TWatchQueryEventComparer());
    if (eventCountLimit && static_cast<i64>(events.size()) > *eventCountLimit) {
        events.resize(*eventCountLimit);
    }

    for (i64 tabletIndex = 0; tabletIndex < TabletCount_; ++tabletIndex) {
        TabletReaders_[tabletIndex]->SetOffset(initialTabletOffsets[tabletIndex]);
    }
    SetTabletReadersOffset(events, TabletReaders_);

    return events;
}

////////////////////////////////////////////////////////////////////////////////

class TWatchQueryExecutor::TImpl
    : public TRefCounted
{
public:
    TImpl(NMaster::TBootstrap* bootstrap, ISession* session)
        : Bootstrap_(bootstrap)
        , Session_(session)
    { }

    TWatchQueryResult ExecuteWatchQuery(
        EObjectType objectType,
        const TWatchQueryOptions& options) const
    {
        std::optional<NProto::TWatchQueryContinuationToken> continuationToken;
        if (options.ContinuationToken) {
            DeserializeContinuationToken(*options.ContinuationToken, &continuationToken.emplace());
        }

        const auto& watchManager = Bootstrap_->GetWatchManager();
        if (!watchManager->Enabled()) {
            THROW_ERROR_EXCEPTION("Watch logs are disabled now");
        }

        auto reader = CreateWatchLogReader(objectType, options.Timestamp, options.TimeLimit);

        if (continuationToken && options.StartTimestamp) {
            THROW_ERROR_EXCEPTION("Start timestamp and continuation token cannot be specified both");
        } else if (continuationToken) {
            const auto tabletOffsets = NYT::FromProto<std::vector<i64>>(continuationToken->row_offsets());
            const auto tabletCount = watchManager->GetTabletCount(objectType);

            if (static_cast<int>(tabletOffsets.size()) != tabletCount) {
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::InvalidContinuationToken,
                    "Incorrect number of tablets in continuation token: expected %v, got %v",
                    tabletCount,
                    tabletOffsets.size());
            }

            reader->ScheduleRead(tabletOffsets);
        } else if (options.StartTimestamp) {
            reader->ScheduleRead(*options.StartTimestamp);
        } else {
            THROW_ERROR_EXCEPTION("Either start timestamp or continuation token should be specified");
        }

        const auto initialTabletOffsets = reader->GetTabletOffsets();

        TWatchQueryResult result;
        result.Timestamp = reader->GetTimestamp();
        result.Events = reader->Read(options.EventCountLimit);

        {
            NProto::TWatchQueryContinuationToken newContinuationToken;
            ToProto(newContinuationToken.mutable_row_offsets(), reader->GetTabletOffsets());
            result.ContinuationToken = SerializeContinuationToken(newContinuationToken);
        }

        const auto tabletInfos = Bootstrap_->GetWatchManager()->GetTabletInfos(objectType);
        for (size_t tabletIndex = 0; tabletIndex < tabletInfos.size(); ++tabletIndex) {
            if (tabletInfos[tabletIndex].TrimmedRowCount > initialTabletOffsets[tabletIndex]) {
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::RowsAlreadyTrimmed,
                    "The needed rows of watch log for type %Qlv are already trimmed",
                    objectType)
                    << TErrorAttribute("tablet_index", tabletIndex)
                    << TErrorAttribute("queried_row_index", initialTabletOffsets[tabletIndex])
                    << TErrorAttribute("trimmed_row_count", tabletInfos[tabletIndex].TrimmedRowCount);
            }
        }

        return result;
    }

private:
    NMaster::TBootstrap* const Bootstrap_;
    ISession* const Session_;

    TWatchLogReaderPtr CreateWatchLogReader(
        EObjectType objectType,
        std::optional<TTimestamp> endTimestamp,
        std::optional<TDuration> timeLimit) const
    {
        const auto& watchManager = Bootstrap_->GetWatchManager();

        if (!endTimestamp) {
            const auto tabletInfos = watchManager->GetTabletInfos(objectType);
            return New<TWatchLogReader>(
                GetBarrierTimestamp(tabletInfos),
                watchManager->GetWatchLogTable(objectType),
                tabletInfos,
                Session_);

        } else if (!timeLimit) {
            const auto tabletInfos = watchManager->GetTabletInfos(objectType);
            if (GetBarrierTimestamp(tabletInfos) < *endTimestamp) {
                THROW_ERROR_EXCEPTION("End timestamp %llx is not available yet",
                    *endTimestamp);
            }

            return New<TWatchLogReader>(
                *endTimestamp,
                watchManager->GetWatchLogTable(objectType),
                tabletInfos,
                Session_);

        } else {
            const auto tabletInfos = watchManager->WaitForTabletInfos(objectType, *endTimestamp, *timeLimit);
            return New<TWatchLogReader>(
                *endTimestamp,
                watchManager->GetWatchLogTable(objectType),
                tabletInfos,
                Session_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TWatchQueryExecutor::TWatchQueryExecutor(NMaster::TBootstrap* bootstrap, ISession* session)
    : Impl_(New<TImpl>(bootstrap, session))
{ }

TWatchQueryResult TWatchQueryExecutor::ExecuteWatchQuery(
    EObjectType type,
    const TWatchQueryOptions& options) const
{
    return Impl_->ExecuteWatchQuery(type, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
