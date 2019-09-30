#include "tablet_reader.h"
#include "helpers.h"

#include <yt/client/api/rowset.h>

#include <yt/client/table_client/helpers.h>

namespace NYP::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

// TODO(babenko): Move to YT.
static const TString TimestampColumnName = "$timestamp";
static const TString TabletIndexColumnName = "$tablet_index";
static const TString RowIndexColumnName = "$row_index";

TString GetTimestampQueryString(const TYPath& tablePath, i64 tabletIndex, i64 rowIndex)
{
    return Format("[%v] from [%v] where [%v] = %v and [%v] = %v",
        TimestampColumnName,
        tablePath,
        TabletIndexColumnName,
        tabletIndex,
        RowIndexColumnName,
        rowIndex);
}

TString GetReadQueryString(
    const TYPath& tablePath,
    i64 tabletIndex,
    i64 rowOffset,
    TRange<const TDBField*> fields,
    TTimestamp timestamp,
    std::optional<i64> rowCountLimit)
{
    TStringBuilder builder;
    for (const auto& field : fields) {
        if (builder.GetLength() > 0) {
            builder.AppendString(", ");
        }

        builder.AppendFormat("[%v]", field->Name);
    }

    builder.AppendFormat(" from [%v]", tablePath);
    builder.AppendFormat(" where [%v] = %v and [%v] >= %v and [%v] <= %v",
        TabletIndexColumnName,
        tabletIndex,
        RowIndexColumnName,
        rowOffset,
        TimestampColumnName,
        timestamp);

    if (rowCountLimit) {
        builder.AppendFormat(" limit %v", *rowCountLimit);
    }

    return builder.Flush();
}

} // namespace

TTabletReader::TTabletReader(
    TTimestamp timestamp,
    const TDBTable* table,
    i64 tabletIndex,
    const TTabletInfo& tabletInfo,
    ISession* session)
    : Timestamp_(timestamp)
    , Table_(table)
    , TabletIndex_(tabletIndex)
    , TabletInfo_(tabletInfo)
    , Session_(session)
{ }

void TTabletReader::SetOffset(i64 offset)
{
    RowOffset_ = offset;
}

i64 TTabletReader::GetOffset() const
{
    return RowOffset_;
}

bool TTabletReader::HasMoreRows() const
{
    return HasMoreRows_;
}

void TTabletReader::ThrowRowsAlreadyTrimmedException() const
{
    THROW_ERROR_EXCEPTION(
        NClient::NApi::EErrorCode::RowsAlreadyTrimmed,
        "The needed rows of watch log %Qv are already trimmed",
        Table_->Name);
}

void TTabletReader::ScheduleSearchOffset(TTimestamp timestamp)
{
    if (TabletInfo_.TotalRowCount == 0) {
        RowOffset_ = 0;
    } else if (TabletInfo_.TrimmedRowCount == TabletInfo_.TotalRowCount) {
        ThrowRowsAlreadyTrimmedException();
    } else {
        ScheduleSearchOffsetImpl(TabletInfo_.TrimmedRowCount - 1, TabletInfo_.TotalRowCount, timestamp);
    }
}

void TTabletReader::ScheduleCheckRowExistence(
    i64 rowIndex)
{
    auto handler = [=] (const IUnversionedRowsetPtr& rowset) {
        if (rowset->GetRows().Empty()) {
            ThrowRowsAlreadyTrimmedException();
        }
    };

    Session_->ScheduleLoad(
        [=] (ILoadContext* context) {
            const auto query = GetTimestampQueryString(
                context->GetTablePath(Table_),
                TabletIndex_,
                rowIndex);
            context->ScheduleSelect(query, std::move(handler));
        });
}

void TTabletReader::ScheduleSearchOffsetImpl(
    i64 lowerRowIndex,
    i64 upperRowIndex,
    TTimestamp timestamp)
{
    if (lowerRowIndex + 1 >= upperRowIndex) {
        RowOffset_ = upperRowIndex;
        if (RowOffset_ > 0) {
            ScheduleCheckRowExistence(RowOffset_ - 1);
        }
        return;
    }

    const auto middleRowIndex = lowerRowIndex + (upperRowIndex - lowerRowIndex) / 2;

    auto handler = [=] (const IUnversionedRowsetPtr& rowset) {
        const auto& rows = rowset->GetRows();
        YT_VERIFY(rows.size() <= 1);

        i64 nextLowerRowIndex = lowerRowIndex;
        i64 nextUpperRowIndex = upperRowIndex;

        if (rows.Empty()) {
            nextLowerRowIndex = middleRowIndex;
        } else {
            TTimestamp rowTimestamp;
            FromUnversionedRow(rows[0], &rowTimestamp);
            if (rowTimestamp < timestamp) {
                nextLowerRowIndex = middleRowIndex;
            } else {
                nextUpperRowIndex = middleRowIndex;
            }
        }
        
        ScheduleSearchOffsetImpl(nextLowerRowIndex, nextUpperRowIndex, timestamp);
    };

    Session_->ScheduleLoad(
        [=] (ILoadContext* context) {
            const auto query = GetTimestampQueryString(
                context->GetTablePath(Table_),
                TabletIndex_,
                middleRowIndex);
            context->ScheduleSelect(query, std::move(handler));
        });
}

void TTabletReader::ScheduleRead(
    TRange<const TDBField*> fields,
    std::function<void(const IUnversionedRowsetPtr&)> handler,
    std::optional<i64> rowCountLimit)
{
    HasMoreRows_ = true;

    auto handlerWrapper = [=] (const IUnversionedRowsetPtr& rowset) {
        const auto& rows = rowset->GetRows();
        if (!rowCountLimit || static_cast<i64>(rows.size()) < *rowCountLimit) {
            HasMoreRows_ = false;
        }
        handler(rowset);
    };

    Session_->ScheduleLoad(
        [=] (ILoadContext* context) {
            const auto query = GetReadQueryString(
                context->GetTablePath(Table_),
                TabletIndex_,
                RowOffset_,
                fields,
                Timestamp_,
                rowCountLimit);
            context->ScheduleSelect(query, std::move(handlerWrapper));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
