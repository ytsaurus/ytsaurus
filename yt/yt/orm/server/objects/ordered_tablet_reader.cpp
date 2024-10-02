#include "ordered_tablet_reader.h"

#include "helpers.h"
#include "private.h"
#include "db_schema.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/api/rowset.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYPath;
using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int OffsetRowIndex = 1;
constexpr int TimestampRowIndex = 2;

void ThrowRowsByTimestampAlreadyTrimmed(
    const TString& table,
    TTimestamp searched,
    int tabletIndex,
    i64 rowOffset,
    std::optional<TTimestamp> rowTimestamp = std::nullopt)
{
    auto error = TError(NClient::EErrorCode::RowsAlreadyTrimmed,
        "Failed to search offset by timestamp %v: the needed rows are already trimmed",
        searched)
        << TErrorAttribute("log", table)
        << TErrorAttribute("tablet_index", tabletIndex)
        << TErrorAttribute("row_offset", rowOffset);

    if (rowTimestamp) {
        error <<= TErrorAttribute("row_timestamp", rowTimestamp);
    }
    THROW_ERROR error;
}

void ThrowRowsAlreadyTrimmed(
    const TString& logName,
    int tabletIndex,
    i64 rowOffset,
    const size_t firstUntrimmedOffset)
{
    THROW_ERROR_EXCEPTION(NClient::EErrorCode::RowsAlreadyTrimmed,
        "Requested rows of watch log %Qv have already been trimmed",
        logName)
        << TErrorAttribute("tablet_index", tabletIndex)
        << TErrorAttribute("queried_row_offset", rowOffset)
        << TErrorAttribute("first_untrimmed_offset", firstUntrimmedOffset);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TOrderedTabletReader::TOrderedTabletReader(
    TTimestamp timestamp,
    const TDBTable* table,
    i64 tabletIndex,
    TTabletInfo tabletInfo,
    ISession* session,
    std::optional<NYPath::TYPath> consumerPath,
    bool skipTrimmed)
    : BarrierTimestamp_(timestamp)
    , Table_(table)
    , TabletIndex_(tabletIndex)
    , TabletInfo_(std::move(tabletInfo))
    , Session_(session)
    , ConsumerPath_(std::move(consumerPath))
    , SkipTrimmed_(skipTrimmed)
    , Logger(NObjects::Logger()
        .WithTag("Table: %v", Table_->GetName())
        .WithTag("TabletIndex: %v", TabletIndex_))
{ }

void TOrderedTabletReader::SetEarliestOffset()
{
    RowOffset_ = 0;
    // When reading with earliest event TWatchLogReader either finds at least one event
    // and this timestamp gets updated, or finds none, so usage of `BarrierTimestamp` seems reasonable.
    Timestamp_ = TabletInfo_.BarrierTimestamp;
    OffsetType_ = EOffsetType::Earliest;
}

void TOrderedTabletReader::SetLastCommittedOffset()
{
    RowOffset_ = 0;
    // When reading with last committed TWatchLogReader either finds at least one event
    // and this timestamp gets updated, or finds none, so usage of `BarrierTimestamp` seems reasonable.
    Timestamp_ = TabletInfo_.BarrierTimestamp;
    OffsetType_ = EOffsetType::LastCommitted;
}

void TOrderedTabletReader::SetOffset(i64 offset, TTimestamp timestamp)
{
    RowOffset_ = offset;
    Timestamp_ = timestamp;
    OffsetType_ = EOffsetType::Exact;
}

i64 TOrderedTabletReader::GetOffset() const
{
    return RowOffset_;
}

int TOrderedTabletReader::GetTabletIndex() const
{
    return TabletIndex_;
}

TTimestamp TOrderedTabletReader::GetTimestamp() const
{
    return Timestamp_;
}

bool TOrderedTabletReader::HasMoreRows() const
{
    return HasMoreRows_;
}

i64 TOrderedTabletReader::GetSkippedRows() const
{
    return SkippedRows_;
}

void TOrderedTabletReader::ScheduleSearchOffset(TTimestamp timestamp)
{
    YT_VERIFY(TabletInfo_.TrimmedRowCount <= TabletInfo_.TotalRowCount);
    YT_LOG_DEBUG("Scheduling offset search by timestamp ("
        "Timestamp: %v, "
        "TrimmedRowCount: %v, "
        "TotalRowCount: %v, "
        "TabletBarrierTimestamp: %v, "
        "ReaderBarrierTimestamp: %v, "
        "SkipTrimmed: %v)",
        timestamp,
        TabletInfo_.TrimmedRowCount,
        TabletInfo_.TotalRowCount,
        TabletInfo_.BarrierTimestamp,
        BarrierTimestamp_,
        SkipTrimmed_);
    ScheduleSearchOffsetViaPullQueue(timestamp);
}

void TOrderedTabletReader::ScheduleRead(
    std::function<void(NQueueClient::IQueueRowsetPtr)> handler,
    std::optional<i64> rowCountLimit)
{
    HasMoreRows_ = true;

    auto tryUpdateSkippedRows = [this] (i64 startOffset) {
        YT_VERIFY(startOffset >= RowOffset_);
        // TODO(dgolear): Check for trimmed rows for EOffsetType::LastCommitted.
        if (startOffset > RowOffset_ && OffsetType_ == EOffsetType::Exact) {
            if (!SkipTrimmed_) {
                ThrowRowsAlreadyTrimmed(
                    Table_->GetName(),
                    TabletIndex_,
                    RowOffset_,
                    /*firstUntrimmedOffset*/ startOffset);
            }
            YT_LOG_DEBUG("Requested rows are already trimmed (RequestedRowOffset: %v, FirstRowOffset: %v)",
                RowOffset_,
                startOffset);
            SkippedRows_ += startOffset - RowOffset_;
        }
    };

    auto handlerWrapper = [
        tryUpdateSkippedRows = std::move(tryUpdateSkippedRows),
        rowCountLimit,
        handler = std::move(handler),
        this
    ] (const NQueueClient::IQueueRowsetPtr& rowset) {
        // We do not know beforehand which offset to use, so set it here on the first read row.
        if (OffsetType_ == EOffsetType::LastCommitted) {
            RowOffset_ = rowset->GetStartOffset();
        }
        tryUpdateSkippedRows(rowset->GetStartOffset());
        const auto& rows = rowset->GetRows();
        auto firstTooNewRow = std::upper_bound(
            rows.Begin(),
            rows.End(),
            BarrierTimestamp_,
            [] (const TTimestamp& barrierTimestamp, const TUnversionedRow& row) {
                YT_VERIFY(row.GetCount() >= 3);
                YT_VERIFY(row[2].Type == EValueType::Uint64);
                TTimestamp rowTimestamp = row[2].Data.Uint64;
                return barrierTimestamp < rowTimestamp;
        });

        auto selectedRows = rows.Slice(rows.Begin(), firstTooNewRow);
        if (!rowCountLimit || std::ssize(selectedRows) == 0 || std::ssize(selectedRows) < std::ssize(rows)) {
            HasMoreRows_ = false;
        }
        handler(NQueueClient::CreateQueueRowset(
            CreateRowset(rowset->GetSchema(), std::move(selectedRows)),
            rowset->GetStartOffset()));
    };
    Session_->ScheduleLoad(
        [handlerWrapper = std::move(handlerWrapper), rowCountLimit, this] (ILoadContext* context) {
            context->SchedulePullQueue(
                Table_,
                OffsetType_ == EOffsetType::LastCommitted ? std::nullopt : std::optional(RowOffset_),
                TabletIndex_,
                rowCountLimit,
                ConsumerPath_,
                std::move(handlerWrapper));
        });
}

////////////////////////////////////////////////////////////////////////////////

void TOrderedTabletReader::ScheduleSearchOffsetViaPullQueue(TTimestamp timestamp)
{
    auto handler = [timestamp, this] (const NQueueClient::IQueueRowsetPtr& rowset) {
        i64 startOffset = rowset->GetStartOffset();

        if (rowset->GetRows().Empty()) {
            if (!SkipTrimmed_ && TabletInfo_.TrimmedRowCount != 0) {
                ThrowRowsByTimestampAlreadyTrimmed(
                    Table_->GetName(),
                    /*searched*/ timestamp,
                    TabletIndex_,
                    /*rowOffset*/ 0);
            } else {
                SetOffset(startOffset, TabletInfo_.BarrierTimestamp);
                HasMoreRows_ = false;
                YT_LOG_DEBUG("Tablet is empty");
            }
        } else {
            TTimestamp rowTimestamp = rowset->GetRows()[0][TimestampRowIndex].Data.Uint64;

            if (startOffset > TabletInfo_.TotalRowCount) {
                THROW_ERROR_EXCEPTION("The watchlog start row offset (%v) is greater than total row count (%v)",
                    startOffset,
                    TabletInfo_.TotalRowCount)
                    << TErrorAttribute("log", Table_->GetName())
                    << TErrorAttribute("tablet_index", TabletIndex_);
            }

            if (rowTimestamp > timestamp) {
                if (!SkipTrimmed_ && startOffset != 0) {
                    ThrowRowsByTimestampAlreadyTrimmed(
                        Table_->GetName(),
                        timestamp,
                        TabletIndex_,
                        startOffset,
                        rowTimestamp);
                } else {
                    SetOffset(startOffset, std::min(rowTimestamp, BarrierTimestamp_));
                    HasMoreRows_ = rowTimestamp <= BarrierTimestamp_;
                    YT_LOG_DEBUG(
                        "Requested timestamp is already trimmed, skipping it (RowOffset: %v, RowTimestamp: %v)",
                        startOffset,
                        rowTimestamp);
                }
            } else {
                ScheduleSearchOffsetImpl(
                    startOffset - 1,
                    TabletInfo_.TotalRowCount,
                    timestamp,
                    /*prevMiddleTimestamp*/ std::nullopt);
            }
        }
    };

    Session_->ScheduleLoad(
        [handler = std::move(handler), this] (ILoadContext* context) {
            context->SchedulePullQueue(
                Table_,
                /*offset*/ 0,
                TabletIndex_,
                /*rowCountLimit*/ 1,
                ConsumerPath_,
                std::move(handler));
        });
}

void TOrderedTabletReader::ScheduleValidateRowExistence(i64 rowOffset, TTimestamp searchedTimestamp)
{
    auto handler = [searchedTimestamp, rowOffset, this] (const NQueueClient::IQueueRowsetPtr& rowset) {
        if (rowset->GetRows().Empty()) {
            if (!SkipTrimmed_) {
                ThrowRowsByTimestampAlreadyTrimmed(
                    Table_->GetName(),
                    searchedTimestamp,
                    TabletIndex_,
                    rowOffset);
            }
            Timestamp_ = TabletInfo_.BarrierTimestamp;
            HasMoreRows_ = false;
        } else {
            auto rowTimestamp = FromUnversionedValue<TTimestamp>(rowset->GetRows()[0][TimestampRowIndex]);
            if (rowTimestamp > searchedTimestamp && !SkipTrimmed_) {
                ThrowRowsByTimestampAlreadyTrimmed(
                    Table_->GetName(),
                    searchedTimestamp,
                    TabletIndex_,
                    rowOffset,
                    rowTimestamp);
            }
            Timestamp_ = rowTimestamp;
        }
    };

    Session_->ScheduleLoad(
        [handler = std::move(handler), rowOffset, this] (ILoadContext* context) {
            context->SchedulePullQueue(
                Table_,
                rowOffset,
                TabletIndex_,
                /*rowCountLimit*/ 1,
                /*consumer*/ ConsumerPath_,
                std::move(handler));
        });
}

//! Searches for offset corresponding the given start timestamp.
/*!
 *  Ordered dynamic table query language cannot perform this step on its own,
 *  because timestamp is not a key column. Moreover, timestamp cannot be
 *  key column at all for the case of weak commit ordering.
 *  Straightforward query "timestamp > start timestamp" performs full scan.
 *
 *  Lower index is always non-feasible and upper index is always feasible.
 *  Lower index can be equal to -1, and upper index can be equal to the total row count.
 */
void TOrderedTabletReader::ScheduleSearchOffsetImpl(
    i64 lowerRowIndex,
    i64 upperRowIndex,
    TTimestamp timestamp,
    std::optional<TTimestamp> prevMiddleTimestamp)
{
    YT_VERIFY(lowerRowIndex < upperRowIndex);

    if (lowerRowIndex + 1 == upperRowIndex) {
        SetOffset(upperRowIndex, timestamp);
        YT_VERIFY(RowOffset_ >= 0);
        YT_LOG_DEBUG("Found offset for given timestamp (RowOffset: %v, RowTimestamp: %v, PreviousMiddleTimestamp: %v)",
            RowOffset_,
            timestamp,
            prevMiddleTimestamp);

        // Non-existent row can possibly have feasible timestamp.
        // Make sure that previous row is existent and so was checked out by the search.
        if (RowOffset_ > 0) {
            ScheduleValidateRowExistence(RowOffset_ - 1, timestamp);
        } else {
            Timestamp_ = prevMiddleTimestamp.value_or(BarrierTimestamp_);
        }

        return;
    }

    const auto middleRowIndex = lowerRowIndex + (upperRowIndex - lowerRowIndex) / 2;
    YT_VERIFY(lowerRowIndex < middleRowIndex && middleRowIndex < upperRowIndex);

    auto handler = [=, this] (const NQueueClient::IQueueRowsetPtr& rowset) {
        const auto& rows = rowset->GetRows();
        YT_VERIFY(std::ssize(rows) <= 1);

        i64 nextLowerRowIndex = lowerRowIndex;
        i64 nextUpperRowIndex = upperRowIndex;

        auto rowTimestamp = prevMiddleTimestamp;
        if (rows.Empty()) {
            if (!rowTimestamp || *rowTimestamp <= timestamp) {
                if (!SkipTrimmed_) {
                    ThrowRowsByTimestampAlreadyTrimmed(
                        Table_->GetName(),
                        timestamp,
                        TabletIndex_,
                        middleRowIndex,
                        rowTimestamp);
                }
                YT_LOG_DEBUG("Requested timestamp is already trimmed, skipping it (RowOffset: %v)",
                    RowOffset_);
                SetOffset(middleRowIndex, TabletInfo_.BarrierTimestamp);
                HasMoreRows_ = false;
                return;
            } else {
                nextUpperRowIndex = middleRowIndex - 1;
            }
        } else {
            rowTimestamp.emplace(FromUnversionedValue<TTimestamp>(rows[0][TimestampRowIndex]));
            if (rowTimestamp <= timestamp) {
                nextLowerRowIndex = std::min(
                    FromUnversionedValue<i64>(rows[0][OffsetRowIndex]),
                    upperRowIndex - 1);
            } else {
                nextUpperRowIndex = middleRowIndex;
            }
        }

        ScheduleSearchOffsetImpl(
            nextLowerRowIndex,
            nextUpperRowIndex,
            timestamp,
            /*prevMiddleTimestamp*/ rowTimestamp);
    };

    Session_->ScheduleLoad(
        [handler = std::move(handler), middleRowIndex, this] (ILoadContext* context) {
            context->SchedulePullQueue(
                Table_,
                middleRowIndex,
                TabletIndex_,
                /*rowCountLimit*/ 1,
                /*consumer*/ std::nullopt,
                std::move(handler));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
