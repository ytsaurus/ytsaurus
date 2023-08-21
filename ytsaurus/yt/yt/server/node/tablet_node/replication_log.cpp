#include "replication_log.h"

#include "tablet.h"
#include "tablet_reader.h"
#include "private.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

i64 GetLogRowIndex(TUnversionedRow logRow)
{
    YT_ASSERT(logRow[1].Type == EValueType::Int64);
    return logRow[1].Data.Int64;
}

TTimestamp GetLogRowTimestamp(TUnversionedRow logRow)
{
    YT_ASSERT(logRow[2].Type == EValueType::Uint64);
    return logRow[2].Data.Uint64;
}

TLegacyOwningKey MakeRowBound(i64 rowIndex, i64 tabletIndex)
{
    return MakeUnversionedOwningRow(
        tabletIndex,
        rowIndex);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TUnversionedRow BuildOrderedLogRow(
    TUnversionedRow row,
    ERowModificationType changeType,
    TUnversionedRowBuilder* rowBuilder)
{
    YT_VERIFY(changeType == ERowModificationType::Write);

    for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
        auto value = row[index];
        value.Id += 1;
        rowBuilder->AddValue(value);
    }
    return rowBuilder->GetRow();
}

TUnversionedRow BuildSortedLogRow(
    TUnversionedRow row,
    ERowModificationType changeType,
    const TTableSchemaPtr& tableSchema,
    TUnversionedRowBuilder* rowBuilder)
{
    rowBuilder->AddValue(MakeUnversionedInt64Value(static_cast<int>(changeType), 1));

    int keyColumnCount = tableSchema->GetKeyColumnCount();
    int valueColumnCount = tableSchema->GetValueColumnCount();

    YT_VERIFY(static_cast<int>(row.GetCount()) >= keyColumnCount);
    for (int index = 0; index < keyColumnCount; ++index) {
        auto value = row[index];
        value.Id += 2;
        rowBuilder->AddValue(value);
    }

    if (changeType == ERowModificationType::Write) {
        for (int index = 0; index < valueColumnCount; ++index) {
            rowBuilder->AddValue(MakeUnversionedSentinelValue(
                EValueType::Null,
                index * 2 + keyColumnCount + 2));
            rowBuilder->AddValue(MakeUnversionedUint64Value(
                static_cast<ui64>(EReplicationLogDataFlags::Missing),
                index * 2 + keyColumnCount + 3));
        }
        auto logRow = rowBuilder->GetRow();
        for (int index = keyColumnCount; index < static_cast<int>(row.GetCount()); ++index) {
            auto value = row[index];
            value.Id = (value.Id - keyColumnCount) * 2 + keyColumnCount + 2;
            logRow[value.Id] = value;
            auto& flags = logRow[value.Id + 1].Data.Uint64;
            flags &= ~static_cast<ui64>(EReplicationLogDataFlags::Missing);
            if (Any(value.Flags & EValueFlags::Aggregate)) {
                flags |= static_cast<ui64>(EReplicationLogDataFlags::Aggregate);
            }
        }
    }

    return rowBuilder->GetRow();
}

TUnversionedRow BuildSortedLogRow(
    TVersionedRow row,
    const TTableSchemaPtr& tableSchema,
    TUnversionedRowBuilder* rowBuilder)
{
    YT_VERIFY(row.GetDeleteTimestampCount() == 1 || row.GetWriteTimestampCount() == 1);

    if (row.GetDeleteTimestampCount() == 1) {
        rowBuilder->AddValue(MakeUnversionedUint64Value(row.BeginDeleteTimestamps()[0], 0));
        rowBuilder->AddValue(MakeUnversionedInt64Value(static_cast<int>(ERowModificationType::Delete), 1));
        return rowBuilder->GetRow();
    }

    rowBuilder->AddValue(MakeUnversionedUint64Value(row.WriteTimestamps()[0], 0));
    rowBuilder->AddValue(MakeUnversionedInt64Value(static_cast<int>(ERowModificationType::Write), 1));

    int keyColumnCount = tableSchema->GetKeyColumnCount();
    int valueColumnCount = tableSchema->GetValueColumnCount();

    YT_VERIFY(static_cast<int>(row.GetKeyCount()) >= keyColumnCount);
    for (int index = 0; index < keyColumnCount; ++index) {
        auto value = row.Keys()[index];
        value.Id += 2;
        rowBuilder->AddValue(value);
    }

    for (int index = 0; index < valueColumnCount; ++index) {
        rowBuilder->AddValue(MakeUnversionedSentinelValue(
            EValueType::Null,
            index * 2 + keyColumnCount + 2));
        rowBuilder->AddValue(MakeUnversionedUint64Value(
            static_cast<ui64>(EReplicationLogDataFlags::Missing),
            index * 2 + keyColumnCount + 3));
    }

    auto logRow = rowBuilder->GetRow();

    for (int index = 0; index < row.GetValueCount(); ++index) {
        auto value = row.Values()[index];
        value.Id = (value.Id - keyColumnCount) * 2 + keyColumnCount + 2;
        logRow[value.Id] = static_cast<TUnversionedValue>(value);
        auto& flags = logRow[value.Id + 1].Data.Uint64;
        flags &= ~static_cast<ui64>(EReplicationLogDataFlags::Missing);
        if (Any(value.Flags & EValueFlags::Aggregate)) {
            flags |= static_cast<ui64>(EReplicationLogDataFlags::Aggregate);
        }
    }

    return logRow;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TUnversionedRow BuildLogRow(
    TUnversionedRow row,
    ERowModificationType changeType,
    const TTableSchemaPtr& tableSchema,
    TUnversionedRowBuilder* rowBuilder)
{
    rowBuilder->Reset();
    rowBuilder->AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));

    if (tableSchema->IsSorted()) {
        return NDetail::BuildSortedLogRow(row, changeType, tableSchema, rowBuilder);
    } else {
        return NDetail::BuildOrderedLogRow(row, changeType, rowBuilder);
    }
}

TUnversionedRow BuildLogRow(
    TVersionedRow row,
    const TTableSchemaPtr& tableSchema,
    TUnversionedRowBuilder* rowBuilder)
{
    rowBuilder->Reset();

    YT_VERIFY(tableSchema->IsSorted());

    return NDetail::BuildSortedLogRow(row, tableSchema, rowBuilder);
}

////////////////////////////////////////////////////////////////////////////////

class TReplicationLogParser
    : public IReplicationLogParser
{
public:
    TReplicationLogParser(
        TTableSchemaPtr tableSchema,
        TTableMountConfigPtr mountConfig,
        EWorkloadCategory workloadCategory,
        NLogging::TLogger logger)
        : IsSorted_(tableSchema->IsSorted())
        , PreserveTabletIndex_(mountConfig->PreserveTabletIndex)
        , TabletIndexColumnId_(tableSchema->ToReplicationLog()->GetColumnCount() + 1) /* maxColumnId - 1(timestamp) + 3(header size)*/
        , TimestampColumnId_(
            tableSchema->HasTimestampColumn()
                ? std::make_optional(tableSchema->GetColumnIndex(TimestampColumnName))
                : std::nullopt)
        , WorkloadCategory_(workloadCategory)
        , Logger(std::move(logger))
    { }

    std::optional<int> GetTimestampColumnId() override
    {
        return TimestampColumnId_;
    }

    void ParseLogRow(
        const TTabletSnapshotPtr& tabletSnapshot,
        TUnversionedRow logRow,
        const TRowBufferPtr& rowBuffer,
        TTypeErasedRow* replicationRow,
        ERowModificationType* modificationType,
        i64* rowIndex,
        TTimestamp* timestamp,
        bool isVersioned) override
    {
        *rowIndex = GetLogRowIndex(logRow);
        *timestamp = GetLogRowTimestamp(logRow);
        if (IsSorted_) {
            if (isVersioned) {
                ParseSortedLogRowWithTimestamps(
                    tabletSnapshot,
                    logRow,
                    rowBuffer,
                    *timestamp,
                    replicationRow,
                    modificationType);
            } else {
                ParseSortedLogRow(
                    tabletSnapshot,
                    logRow,
                    rowBuffer,
                    replicationRow,
                    modificationType);
            }
        } else {
            ParseOrderedLogRow(
                logRow,
                rowBuffer,
                replicationRow,
                modificationType,
                isVersioned);
        }
    }

    std::optional<i64> ComputeStartRowIndex(
        const TTabletSnapshotPtr& tabletSnapshot,
        NTransactionClient::TTimestamp startReplicationTimestamp,
        const TClientChunkReadOptions& chunkReadOptions,
        std::optional<i64> lowerRowIndex = {},
        TOnMissingRowCallback onMissingRow = [] {}) override
    {
        auto trimmedRowCount = tabletSnapshot->TabletRuntimeData->TrimmedRowCount.load();
        auto totalRowCount = tabletSnapshot->TabletRuntimeData->TotalRowCount.load();

        auto rowIndexLo = lowerRowIndex.value_or(trimmedRowCount);
        auto rowIndexHi = totalRowCount;
        if (rowIndexLo == rowIndexHi) {
            onMissingRow();
            return {};
        }

        YT_LOG_DEBUG("Started computing replication start row index (StartReplicationTimestamp: %v, RowIndexLo: %v, RowIndexHi: %v)",
            startReplicationTimestamp,
            rowIndexLo,
            rowIndexHi);

        while (rowIndexLo < rowIndexHi - 1) {
            auto rowIndexMid = rowIndexLo + (rowIndexHi - rowIndexLo) / 2;
            auto timestampMid = ReadLogRowTimestamp(tabletSnapshot, chunkReadOptions, rowIndexMid, onMissingRow);
            if (!timestampMid || *timestampMid <= startReplicationTimestamp) {
                rowIndexLo = rowIndexMid;
            } else {
                rowIndexHi = rowIndexMid;
            }
        }

        auto startRowIndex = rowIndexLo;
        auto startTimestamp = NullTimestamp;
        while (startRowIndex < totalRowCount) {
            auto rowTimestamp = ReadLogRowTimestamp(tabletSnapshot, chunkReadOptions, startRowIndex, onMissingRow);
            if (rowTimestamp) {
                startTimestamp = *rowTimestamp;
                if (startTimestamp > startReplicationTimestamp) {
                    break;
                }
            }
            ++startRowIndex;
        }

        YT_LOG_DEBUG("Finished computing replication start row index (StartRowIndex: %v, StartTimestamp: %v)",
            startRowIndex,
            startTimestamp);

        if (startTimestamp == NullTimestamp) {
            YT_LOG_DEBUG("No replication log rows available (LowerRowIndex: %v, TrimmedRowCount: %v, TotalRowCount: %v)",
                lowerRowIndex,
                trimmedRowCount,
                totalRowCount);
            onMissingRow();
            return {};
        }

        return startRowIndex;
    }

private:
    const bool IsSorted_;
    const bool PreserveTabletIndex_;
    const int TabletIndexColumnId_;
    const std::optional<int> TimestampColumnId_;
    const EWorkloadCategory WorkloadCategory_;
    const NLogging::TLogger Logger;

    void ParseOrderedLogRow(
        TUnversionedRow logRow,
        const TRowBufferPtr& rowBuffer,
        TTypeErasedRow* replicationRow,
        ERowModificationType* modificationType,
        bool isVersioned)
    {
        constexpr int headerRows = 3;
        YT_VERIFY(static_cast<int>(logRow.GetCount()) >= headerRows);

        auto mutableReplicationRow = rowBuffer->AllocateUnversioned(logRow.GetCount() - headerRows);
        int columnCount = 0;
        for (int index = headerRows; index < static_cast<int>(logRow.GetCount()); ++index) {
            int id = index - headerRows;

            if (logRow[index].Id == TabletIndexColumnId_ && !PreserveTabletIndex_) {
                continue;
            }

            if (id == TimestampColumnId_) {
                continue;
            }

            mutableReplicationRow.Begin()[columnCount] = rowBuffer->CaptureValue(logRow[index]);
            mutableReplicationRow.Begin()[columnCount].Id = id;
            columnCount++;
        }

        if (isVersioned) {
            auto timestamp = GetLogRowTimestamp(logRow);
            YT_VERIFY(TimestampColumnId_);
            mutableReplicationRow.Begin()[columnCount++] = MakeUnversionedUint64Value(timestamp, *TimestampColumnId_);
        }

        mutableReplicationRow.SetCount(columnCount);

        *modificationType = isVersioned ? ERowModificationType::VersionedWrite : ERowModificationType::Write;
        *replicationRow = mutableReplicationRow.ToTypeErasedRow();
    }

    void ParseSortedLogRowWithTimestamps(
        const TTabletSnapshotPtr& tabletSnapshot,
        TUnversionedRow logRow,
        const TRowBufferPtr& rowBuffer,
        TTimestamp timestamp,
        TTypeErasedRow* result,
        ERowModificationType* modificationType)
    {
        TVersionedRow replicationRow;

        YT_ASSERT(logRow[3].Type == EValueType::Int64);
        auto changeType = ERowModificationType(logRow[3].Data.Int64);

        int keyColumnCount = tabletSnapshot->TableSchema->GetKeyColumnCount();
        int valueColumnCount = tabletSnapshot->TableSchema->GetValueColumnCount();
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;

        YT_ASSERT(static_cast<int>(logRow.GetCount()) == keyColumnCount + valueColumnCount * 2 + 4);

        switch (changeType) {
            case ERowModificationType::Write: {
                YT_ASSERT(static_cast<int>(logRow.GetCount()) >= keyColumnCount + 4);

                auto getFlags = [&] (int logValueIndex) {
                    const auto& value = logRow[logValueIndex * 2 + keyColumnCount + 5];
                    YT_ASSERT(value.Type == EValueType::Null || value.Type == EValueType::Uint64);

                    return value.Type == EValueType::Null
                        ? EReplicationLogDataFlags::Missing
                        : static_cast<EReplicationLogDataFlags>(value.Data.Uint64);
                };

                int replicationValueCount = 0;
                for (int logValueIndex = 0; logValueIndex < valueColumnCount; ++logValueIndex) {
                    auto flags = getFlags(logValueIndex);
                    if (None(flags & EReplicationLogDataFlags::Missing)) {
                        ++replicationValueCount;
                    }
                }
                auto mutableReplicationRow = rowBuffer->AllocateVersioned(
                    keyColumnCount,
                    replicationValueCount,
                    1,  // writeTimestampCount
                    0); // deleteTimestampCount
                for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                    mutableReplicationRow.Keys()[keyIndex] = rowBuffer->CaptureValue(logRow[keyIndex + 4]);
                }
                int replicationValueIndex = 0;
                for (int logValueIndex = 0; logValueIndex < valueColumnCount; ++logValueIndex) {
                    auto flags = getFlags(logValueIndex);
                    if (None(flags & EReplicationLogDataFlags::Missing)) {
                        TVersionedValue value{};
                        static_cast<TUnversionedValue&>(value) = rowBuffer->CaptureValue(logRow[logValueIndex * 2 + keyColumnCount + 4]);
                        value.Id = logValueIndex + keyColumnCount;
                        if (Any(flags & EReplicationLogDataFlags::Aggregate)) {
                            value.Flags |= EValueFlags::Aggregate;
                        }
                        value.Timestamp = timestamp;
                        mutableReplicationRow.Values()[replicationValueIndex++] = value;
                    }
                }
                YT_VERIFY(replicationValueIndex == replicationValueCount);
                mutableReplicationRow.WriteTimestamps()[0] = timestamp;
                replicationRow = mutableReplicationRow;
                YT_LOG_DEBUG_IF(mountConfig->EnableReplicationLogging, "Replicating write (Row: %v)", replicationRow);
                break;
            }

            case ERowModificationType::Delete: {
                auto mutableReplicationRow = rowBuffer->AllocateVersioned(
                    keyColumnCount,
                    0,  // valueCount
                    0,  // writeTimestampCount
                    1); // deleteTimestampCount
                for (int index = 0; index < keyColumnCount; ++index) {
                    mutableReplicationRow.Keys()[index] = rowBuffer->CaptureValue(logRow[index + 4]);
                }
                mutableReplicationRow.DeleteTimestamps()[0] = timestamp;
                replicationRow = mutableReplicationRow;
                YT_LOG_DEBUG_IF(mountConfig->EnableReplicationLogging, "Replicating delete (Row: %v)", replicationRow);
                break;
            }

            default:
                YT_ABORT();
        }

        *modificationType = ERowModificationType::VersionedWrite;
        *result = replicationRow.ToTypeErasedRow();
    }

    void ParseSortedLogRow(
        const TTabletSnapshotPtr& tabletSnapshot,
        TUnversionedRow logRow,
        const TRowBufferPtr& rowBuffer,
        TTypeErasedRow* result,
        ERowModificationType* modificationType)
    {
        TUnversionedRow replicationRow;

        YT_ASSERT(logRow[3].Type == EValueType::Int64);
        auto changeType = ERowModificationType(logRow[3].Data.Int64);

        int keyColumnCount = tabletSnapshot->TableSchema->GetKeyColumnCount();
        int valueColumnCount = tabletSnapshot->TableSchema->GetValueColumnCount();
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;

        YT_ASSERT(static_cast<int>(logRow.GetCount()) == keyColumnCount + valueColumnCount * 2 + 4);

        *modificationType = ERowModificationType::Write;

        switch (changeType) {
            case ERowModificationType::Write: {
                YT_ASSERT(static_cast<int>(logRow.GetCount()) >= keyColumnCount + 4);
                int replicationValueCount = 0;
                for (int logValueIndex = 0; logValueIndex < valueColumnCount; ++logValueIndex) {
                    const auto& value = logRow[logValueIndex * 2 + keyColumnCount + 5];
                    auto flags = FromUnversionedValue<EReplicationLogDataFlags>(value);
                    if (None(flags & EReplicationLogDataFlags::Missing)) {
                        ++replicationValueCount;
                    }
                }
                auto mutableReplicationRow = rowBuffer->AllocateUnversioned(
                    keyColumnCount + replicationValueCount);
                for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                    mutableReplicationRow.Begin()[keyIndex] = rowBuffer->CaptureValue(logRow[keyIndex + 4]);
                    mutableReplicationRow.Begin()[keyIndex].Id = keyIndex;
                }
                int replicationValueIndex = 0;
                for (int logValueIndex = 0; logValueIndex < valueColumnCount; ++logValueIndex) {
                    const auto& flagsValue = logRow[logValueIndex * 2 + keyColumnCount + 5];
                    YT_ASSERT(flagsValue.Type == EValueType::Uint64);
                    auto flags = static_cast<EReplicationLogDataFlags>(flagsValue.Data.Uint64);
                    if (None(flags & EReplicationLogDataFlags::Missing)) {
                        TUnversionedValue value{};
                        static_cast<TUnversionedValue&>(value) = rowBuffer->CaptureValue(logRow[logValueIndex * 2 + keyColumnCount + 4]);
                        value.Id = logValueIndex + keyColumnCount;
                        if (Any(flags & EReplicationLogDataFlags::Aggregate)) {
                            value.Flags |= EValueFlags::Aggregate;
                        }
                        mutableReplicationRow.Begin()[keyColumnCount + replicationValueIndex++] = value;
                    }
                }
                YT_VERIFY(replicationValueIndex == replicationValueCount);
                replicationRow = mutableReplicationRow;
                YT_LOG_DEBUG_IF(mountConfig->EnableReplicationLogging, "Replicating write (Row: %v)", replicationRow);
                break;
            }

            case ERowModificationType::Delete: {
                auto mutableReplicationRow = rowBuffer->AllocateUnversioned(
                    keyColumnCount);
                for (int index = 0; index < keyColumnCount; ++index) {
                    mutableReplicationRow.Begin()[index] = rowBuffer->CaptureValue(logRow[index + 4]);
                    mutableReplicationRow.Begin()[index].Id = index;
                }
                replicationRow = mutableReplicationRow;
                *modificationType = ERowModificationType::Delete;
                YT_LOG_DEBUG_IF(mountConfig->EnableReplicationLogging, "Replicating delete (Row: %v)", replicationRow);
                break;
            }

            default:
                YT_ABORT();
        }

        *result = replicationRow.ToTypeErasedRow();
    }

    std::optional<TTimestamp> ReadLogRowTimestamp(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TClientChunkReadOptions& chunkReadOptions,
        i64 rowIndex,
        TOnMissingRowCallback onMissingRow)
    {
        auto reader = CreateSchemafulRangeTabletReader(
            tabletSnapshot,
            TColumnFilter(),
            MakeRowBound(rowIndex),
            MakeRowBound(rowIndex + 1),
            /*timestampRange*/ {},
            chunkReadOptions,
            /*tabletThrottlerKind*/ std::nullopt,
            WorkloadCategory_);

        TRowBatchReadOptions readOptions{
            .MaxRowsPerRead = 1
        };

        IUnversionedRowBatchPtr batch;
        while (true) {
            batch = reader->Read(readOptions);
            if (!batch) {
                YT_LOG_DEBUG("Missing row in replication log (TabletId: %v, RowIndex: %v)",
                    tabletSnapshot->TabletId,
                    rowIndex);
                onMissingRow();
                return {};
            }

            if (batch->IsEmpty()) {
                YT_LOG_DEBUG("Waiting for log row from tablet reader (RowIndex: %v)",
                    rowIndex);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            // One row is enough.
            break;
        }

        auto readerRows = batch->MaterializeRows();
        YT_VERIFY(readerRows.size() == 1);

        i64 actualRowIndex = GetLogRowIndex(readerRows[0]);
        auto timestamp = GetLogRowTimestamp(readerRows[0]);
        YT_VERIFY(actualRowIndex == rowIndex);

        YT_LOG_DEBUG("Replication log row timestamp is read (RowIndex: %v, Timestamp: %v)",
            rowIndex,
            timestamp);

        return timestamp;
    }
};

////////////////////////////////////////////////////////////////////////////////

IReplicationLogParserPtr CreateReplicationLogParser(
    TTableSchemaPtr tableSchema,
    TTableMountConfigPtr mountConfig,
    EWorkloadCategory workloadCategory,
    NLogging::TLogger logger)
{
    return New<TReplicationLogParser>(
        std::move(tableSchema),
        std::move(mountConfig),
        workloadCategory,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
