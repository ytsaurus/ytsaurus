#include "stdafx.h"
#include "memory_table.h"
#include "tablet.h"
#include "transaction.h"
#include "config.h"

#include <core/concurrency/fiber.h>

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/name_table.h>

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;
using namespace NConcurrency;

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
            int result = CompareSameTypeValues(lhs[index], rhs[index]);
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
    , Comparer_(new TComparer(
        Tablet_->Schema().Columns().size()))
    , Tree_(new TRcuTree<TRowGroup, TComparer>(
        &TreePool_,
        Comparer_.get()))
    , TreeReader_(Tree_->CreateReader())
{ }

void TMemoryTable::WriteRows(
    TTransaction* transaction,
    IReaderPtr reader,
    bool transient,
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
            auto group = WriteRow(transaction, row, transient);
            if (lockedGroups) {
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
    TTransaction* transaction,
    TRow row,
    bool transient)
{
    TRowGroup result;

    int keyColumnCount = static_cast<int>(Tablet_->KeyColumns().size());
    int valueCount = row.GetValueCount() - static_cast<int>(Tablet_->KeyColumns().size());

    auto createGroupItem = [&] () -> TRowGroupItem {
        TRowGroupItem item(&RowPool_, valueCount, NullTimestamp, false);
        auto row = item.GetRow();
        for (int index = 0; index < keyColumnCount; ++index) {
            InternValue(row[index], &row[index]);
        }
        return item;
    };

    auto lockGroup = [&] (TRowGroup group) {
        group.SetTransaction(transaction);
        group.SetTransientLock(transient);
        transaction->LockedRowGroups().push_back(group);
    };

    auto newKeyProvider = [&] () -> TRowGroup {
        TRowGroup group(&RowPool_, keyColumnCount);

        // Acquire lock.
        lockGroup(group);

        // Copy key values.
        for (int index = 0; index < keyColumnCount; ++index) {
            InternValue(row[index], &group[index]);
        }

        // Copy regular values.
        auto item = createGroupItem();
        item.SetNextItem(TRowGroupItem());
        group.SetFirstItem(item);

        result = group;
        return group;
    };

    auto existingKeyAcceptor = [&] (TRowGroup group) {
        // Check for lock conflicts.
        auto* transaction = group.GetTransaction();
        if (transaction) {
            THROW_ERROR_EXCEPTION("Row lock conflict with transaction %s",
                ~ToString(transaction->GetId()));
        }

        // Acquire lock.
        lockGroup(group);

        // Copy regular values.
        auto item = createGroupItem();
        item.SetNextItem(group.GetFirstItem());
        group.SetFirstItem(item);

        result = group;
    };

    Tree_->Insert(
        row,
        newKeyProvider,
        existingKeyAcceptor);

    return result;
}

void TMemoryTable::InternValue(const TRowValue& src, TRowValue* dst )
{
    switch (src.Type) {
        case EColumnType::Integer:
        case EColumnType::Double:
        case EColumnType::Null:
            *dst = src;

        case EColumnType::String:
        case EColumnType::Any:
            dst->Type = src.Type;
            dst->Length = src.Length;
            dst->Data.String = StringPool_.AllocateUnaligned(src.Length);
            memcpy(const_cast<char*>(src.Data.String), const_cast<char*>(dst->Data.String), src.Length);
            break;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
