#include "stdafx.h"
#include "memory_table.h"
#include "tablet.h"
#include "transaction.h"
#include "config.h"

#include <core/concurrency/fiber.h>

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/name_table.h>

#include <ytlib/chunk_client/memory_writer.h>

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TMemoryTable::TComparer
{
public:
    explicit TComparer(int keyColumnCount)
        : KeyColumnCount_(keyColumnCount)
    { }

    int operator () (TRowGroup lhs, TRowGroup rhs) const
    {
        return Compare(lhs, rhs);
    }

    int operator () (TRow lhs, TRowGroup rhs) const
    {
        YASSERT(lhs.GetValueCount() >= KeyColumnCount_);
        return Compare(lhs, rhs);
    }

private:
    int KeyColumnCount_;


    template <class TLhs, class TRhs>
    int Compare(TLhs lhs, TRhs rhs) const
    {
        for (int index = 0; index < KeyColumnCount_; ++index) {
            int result = CompareRowValues(lhs[index], rhs[index]);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

};

////////////////////////////////////////////////////////////////////////////////

TMemoryTable::TMemoryTable(
    TTabletManagerConfigPtr config,
    TTablet* tablet)
    : Config_(config)
    , Tablet_(tablet)
    , TreePool_(Config_->TreePoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , RowPool_(Config_->RowPoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , StringPool_(Config_->StringPoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , NameTable_(New<TNameTable>())
    , Comparer_(new TComparer(
        Tablet_->KeyColumns().size()))
    , Tree_(new TRcuTree<TRowGroup, TComparer>(
        &TreePool_,
        Comparer_.get()))
{
    for (const auto& column : Tablet_->Schema().Columns()) {
        NameTable_->RegisterName(column.Name);
    }
}

void TMemoryTable::WriteRows(
    TTransaction* transaction,
    IReaderPtr reader,
    bool prewrite,
    std::vector<TRowGroup>* lockedGroups)
{
    auto nameTable = New<TNameTable>();

    {
        // The reader is typically synchronous.
        auto error = WaitFor(reader->Open(
            nameTable,
            Tablet_->Schema(),
            true));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    const int RowsBufferSize = 1000;
    std::vector<NVersionedTableClient::TRow> rows;
    rows.reserve(RowsBufferSize);

    while (true) {
        bool hasData = reader->Read(&rows);
        for (auto row : rows) {
            auto group = WriteRow(nameTable, transaction, row, prewrite);
            if (lockedGroups && group) {
                lockedGroups->push_back(group);
            }
        }
        if (!hasData) {
            break;
        }
        if (rows.size() < rows.capacity()) {
            // The reader is typically synchronous.
            auto result = WaitFor(reader->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
        rows.clear();
    }
}

TRowGroup TMemoryTable::WriteRow(
    TNameTablePtr nameTable,
    TTransaction* transaction,
    TRow row,
    bool prewrite)
{
    TRowGroup result;

    int keyColumnCount = static_cast<int>(Tablet_->KeyColumns().size());
    int schemaColumnCount = static_cast<int>(Tablet_->Schema().Columns().size());
    int valueCount = row.GetValueCount();

    auto internValues = [&] (TRowGroupItem item) {
        auto internedRow = item.GetRow();
        for (int index = keyColumnCount; index < valueCount; ++index) {
            const auto& rowValue = row[index];
            auto& internedRowValue = internedRow[index - keyColumnCount];
            InternValue(&internedRowValue, rowValue);
            if (rowValue.Id >= schemaColumnCount) {
                internedRowValue.Id = NameTable_->GetOrRegisterName(nameTable->GetName(rowValue.Id));
            } else {
                internedRowValue.Id = index;
            }
        }
    };

    auto createGroupItem = [&] () -> TRowGroupItem {
        TRowGroupItem item(&RowPool_, valueCount - keyColumnCount, NullTimestamp, false);
        internValues(item);
        return item;
    };

    auto lockGroup = [&] (TRowGroup group) {
        group.SetTransaction(transaction);
        group.SetPrewritten(prewrite);
        if (!prewrite) {
            transaction->LockedRowGroups().push_back(group);
        }
    };

    auto newKeyProvider = [&] () -> TRowGroup {
        TRowGroup group(&RowPool_, keyColumnCount);

        // Acquire the lock.
        lockGroup(group);

        // Copy keys.
        for (int index = 0; index < keyColumnCount; ++index) {
            auto& internedValue = group[index];
            InternValue(&internedValue, row[index]);
            internedValue.Id = index;
        }

        // Intern the values and insert a new item.
        auto item = createGroupItem();
        item.SetNextItem(TRowGroupItem());
        group.SetFirstItem(item);

        result = group;
        return group;
    };

    auto existingKeyAcceptor = [&] (TRowGroup group) {
        // Check for a lock conflict.
        auto* existingTransaction = group.GetTransaction();
        if (existingTransaction) {
            if (existingTransaction != transaction) {
                THROW_ERROR_EXCEPTION("Row lock conflict with transaction %s",
                    ~ToString(existingTransaction->GetId()));
            }
            
            // No need to reacquire the lock.
            YASSERT(group.GetPrewritten() == prewrite);

            // Intern the values and overwrite the item.
            auto item = group.GetFirstItem();
            YASSERT(item);
            internValues(item);
        } else {
            // Acquire the lock.
            lockGroup(group);

            // Intern the values and insert a new item.
            auto item = createGroupItem();
            item.SetNextItem(group.GetFirstItem());
            group.SetFirstItem(item);

            result = group;
        }
    };

    Tree_->Insert(
        row,
        newKeyProvider,
        existingKeyAcceptor);

    return result;
}

void TMemoryTable::InternValue(TRowValue* dst, const TRowValue& src)
{
    switch (src.Type) {
        case EColumnType::Integer:
        case EColumnType::Double:
        case EColumnType::Null:
            *dst = src;
            break;

        case EColumnType::String:
        case EColumnType::Any:
            dst->Type = src.Type;
            dst->Length = src.Length;
            dst->Data.String = StringPool_.AllocateUnaligned(src.Length);
            memcpy(const_cast<char*>(dst->Data.String), src.Data.String, src.Length);
            break;

        default:
            YUNREACHABLE();
    }
}

void TMemoryTable::ConfirmPrewrittenGroup(TRowGroup group)
{
    auto* transaction = group.GetTransaction();
    YASSERT(transaction);

    transaction->LockedRowGroups().push_back(group);
    group.SetPrewritten(false);
}

void TMemoryTable::CommitGroup(TRowGroup group)
{
    auto* transaction = group.GetTransaction();
    YASSERT(transaction);

    auto firstItem = group.GetFirstItem();
    firstItem.GetRow().SetTimestamp(transaction->GetCommitTimestamp());

    group.SetTransaction(nullptr);

    // Validate timestamp ordering.
    YASSERT(
        !firstItem.GetNextItem() ||
        firstItem.GetRow().GetTimestamp() > firstItem.GetNextItem().GetRow().GetTimestamp());
}

void TMemoryTable::AbortGroup(TRowGroup group)
{
    if (group.GetPrewritten()) {
        auto firstItem = group.GetFirstItem();
        firstItem.SetOrphaned(true);
        group.SetFirstItem(firstItem.GetNextItem());
    }

    group.SetTransaction(nullptr);
}

void TMemoryTable::LookupRows(
    TRow key,
    TTimestamp timestamp,
    TChunkMeta* chunkMeta,
    std::vector<TSharedRef>* blocks)
{
    auto memoryWriter = New<TMemoryWriter>();

    auto chunkWriter = New<TChunkWriter>(
        New<TChunkWriterConfig>(), // TODO(babenko): make static
        New<TEncodingWriterOptions>(), // TODO(babenko): make static
        memoryWriter);

    chunkWriter->Open(
        NameTable_,
        Tablet_->Schema(),
        Tablet_->KeyColumns());

    auto* scanner = Tree_->AllocateScanner();

    TRowGroup group;
    if (scanner->Find(key, &group)) {
        auto groupItem = FetchGroupItem(group, timestamp);
        if (groupItem) {
            // Key
            for (int index = 0; index < static_cast<int>(Tablet_->KeyColumns().size()); ++index) {
                const auto& value = group[index];
                chunkWriter->WriteValue(value);
            }
            // Value
            auto row = groupItem.GetRow();
            for (int index = 0; index < row.GetValueCount(); ++index) {
                const auto& value = row[index];
                chunkWriter->WriteValue(value);
            }
            chunkWriter->EndRow();
        }
    }

    Tree_->FreeScanner(scanner);

    {
        // The writer is typically synchronous.
        auto error = WaitFor(chunkWriter->AsyncClose());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    *chunkMeta = std::move(memoryWriter->GetChunkMeta());
    *blocks = std::move(memoryWriter->GetBlocks());
}

TRowGroupItem TMemoryTable::FetchGroupItem(
    TRowGroup group,
    TTimestamp timestamp)
{
    auto item = group.GetFirstItem();
    while (item) {
        if (!item.GetOrphaned()) {
            auto row = item.GetRow();
            // TODO(babenko): fixme
            if (timestamp == LastCommittedTimestamp ||
                row.GetTimestamp() <= timestamp)
            {
                return item;
            }
        }
        item = item.GetNextItem();
    }
    return TRowGroupItem();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
