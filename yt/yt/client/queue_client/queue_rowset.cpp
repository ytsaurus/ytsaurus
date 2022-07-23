#include "queue_rowset.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void VerifyRowBatchReadOptions(const TQueueRowBatchReadOptions& options)
{
    if (options.MaxRowCount <= 0) {
        THROW_ERROR_EXCEPTION("MaxRowCount is non-positive");
    }
    if (options.MaxDataWeight <= 0) {
        THROW_ERROR_EXCEPTION("MaxDataWeight is non-positive");
    }
    if (options.DataWeightPerRowHint && *options.DataWeightPerRowHint <= 0) {
        THROW_ERROR_EXCEPTION("DataWeightPerRowHint is non-positive");
    }
}

i64 ComputeRowsToRead(const TQueueRowBatchReadOptions& options)
{
    VerifyRowBatchReadOptions(options);

    auto rowsToRead = options.MaxRowCount;
    if (options.DataWeightPerRowHint) {
        auto rowsToReadWithHint = options.MaxDataWeight / *options.DataWeightPerRowHint;
        rowsToRead = std::min(rowsToRead, rowsToReadWithHint);
    }

    rowsToRead = std::max<i64>(rowsToRead, 1);

    return rowsToRead;
}

i64 GetStartOffset(const IUnversionedRowsetPtr& rowset)
{
    const auto& nameTable = rowset->GetNameTable();

    YT_VERIFY(!rowset->GetRows().Empty());

    auto rowIndexColumnId = nameTable->GetIdOrThrow("$row_index");
    auto startOffsetValue = rowset->GetRows()[0][rowIndexColumnId];
    THROW_ERROR_EXCEPTION_IF_NOT(
        startOffsetValue.Type == EValueType::Int64,
        "Incorrect type %Qlv for $row_index column, %Qlv expected; only ordered dynamic tables are supported as queues",
        startOffsetValue.Type,
        EValueType::Int64);

    return startOffsetValue.Data.Int64;
}

////////////////////////////////////////////////////////////////////////////////

TQueueRowset::TQueueRowset(IUnversionedRowsetPtr rowset, i64 startOffset)
    : Rowset_(std::move(rowset))
    , StartOffset_(startOffset)
{ }

const NTableClient::TTableSchemaPtr& TQueueRowset::GetSchema() const
{
    return Rowset_->GetSchema();
}

const NTableClient::TNameTablePtr& TQueueRowset::GetNameTable() const
{
    return Rowset_->GetNameTable();
}

TRange<NTableClient::TUnversionedRow> TQueueRowset::GetRows() const
{
    return Rowset_->GetRows();
}

TSharedRange<NTableClient::TUnversionedRow> TQueueRowset::GetSharedRange() const
{
    return Rowset_->GetSharedRange();
}

i64 TQueueRowset::GetStartOffset() const
{
    return StartOffset_;
}

i64 TQueueRowset::GetFinishOffset() const
{
    return StartOffset_ + std::ssize(GetRows());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
