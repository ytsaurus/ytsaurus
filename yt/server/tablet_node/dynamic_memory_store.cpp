#include "stdafx.h"
#include "dynamic_memory_store.h"
#include "tablet.h"
#include "transaction.h"
#include "config.h"
#include "private.h"

#include <core/misc/small_vector.h>

#include <core/concurrency/fiber.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/api/transaction.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

static const int InitialEditListCapacity = 2;
static const int EditListCapacityMultiplier = 2;
static const int MaxEditListCapacity = 256;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T, class TTimestampExtractor>
std::tuple<T*, TEditList<T>> FindVersionedValue(
    TEditList<T> list,
    TTimestamp maxTimestamp,
    TTimestampExtractor timestampExtractor)
{
    if (!list) {
        return std::make_tuple(nullptr, list);
    }

    if (maxTimestamp == LastCommittedTimestamp) {
        auto& value = list.Back();
        if (timestampExtractor(value) != UncommittedTimestamp) {
            return std::make_tuple(&value, list);
        }
        if (list.GetSize() > 1) {
            return std::make_tuple(&value - 1, list);
        }
        auto nextList = list.GetNext();
        if (!nextList) {
            return std::make_tuple(nullptr, nextList);
        }
        return std::make_tuple(&nextList.Back(), nextList);
    } else {
        while (true) {
            if (!list) {
                return std::make_tuple(nullptr, list);
            }
            if (timestampExtractor(list.Front()) <= maxTimestamp) {
                break;
            }
            list = list.GetNext();
        }

        auto* left = list.Begin();
        auto* right = list.End();
        while (right - left > 1) {
            auto* mid = left + (right - left) / 2;
            if (timestampExtractor(*mid) <= maxTimestamp) {
                left = mid;
            }
            else {
                right = mid;
            }
        }

        return
            left && timestampExtractor(*left) <= maxTimestamp
            ? std::make_tuple(left, list)
            : std::make_tuple(nullptr, list);
    }
}

template <class T>
bool AllocateListIfNeeded(TEditList<T>* list, TChunkedMemoryPool* pool)
{
    if (list->GetSize() < list->GetCapacity()) {
        return false;
    }

    int newCapacity = std::min(list->GetCapacity() * EditListCapacityMultiplier, MaxEditListCapacity);
    auto newList = TEditList<T>::Allocate(pool, newCapacity);
    newList.SetNext(*list);
    *list = newList;
    return true;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore::TReader
    : public IVersionedReader
{
public:
    TReader(
        TDynamicMemoryStorePtr store,
        NVersionedTableClient::TKey lowerKey,
        NVersionedTableClient::TKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter)
        : Store_(store)
        , LowerKey_(lowerKey)
        , UpperKey_(upperKey)
        , Timestamp_(timestamp)
        , ColumnFilter_(columnFilter)
        , KeyCount_(Store_->Tablet_->KeyColumns().size())
        , SchemaColumnCount_(Store_->Tablet_->Schema().Columns().size())
        , TreeScanner_(Store_->Tree_.get())
        , Pool_(1024)
        , Finished_(false)
    {
        YCHECK(Timestamp_ != AllCommittedTimestamp || ColumnFilter_.All);
    }

    virtual TAsyncError Open() override
    {
        TreeScanner_->BeginScan(LowerKey_);
        static auto result = MakeFuture(TError());
        return result;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        if (Finished_) {
            return false;
        }

        rows->clear();
        Pool_.Clear();

        TKeyPrefixComparer keyComparer(KeyCount_);

        while (TreeScanner_->IsValid() && rows->size() < MaxRowsPerRead) {
            const auto* rowKeys = TreeScanner_->GetCurrent().GetKeys();
            if (CompareRows(rowKeys, rowKeys + KeyCount_, UpperKey_.Begin(), UpperKey_.End()) >= 0)
                break;

            auto row = ProduceRow();
            if (row) {
                rows->push_back(row);
            }

            TreeScanner_->Advance();
        }

        if (rows->empty()) {
            Finished_ = true;
            return false;
        }

        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        YUNREACHABLE();
    }

private:
    TDynamicMemoryStorePtr Store_;
    NVersionedTableClient::TKey LowerKey_;
    NVersionedTableClient::TKey UpperKey_;
    TTimestamp Timestamp_;
    TColumnFilter ColumnFilter_;

    int KeyCount_;
    int SchemaColumnCount_;

    TRcuTreeScannerPtr<TDynamicRow, TKeyPrefixComparer> TreeScanner_;

    TChunkedMemoryPool Pool_;
    
    bool Finished_;

    static const size_t MaxRowsPerRead = 1024;


    TVersionedRow ProduceRow()
    {
        auto dynamicRow = TreeScanner_->GetCurrent();
        return
            Timestamp_ == AllCommittedTimestamp
            ? ProduceAllRowVersions(dynamicRow)
            : ProduceSingleRowVersion(dynamicRow);
    }

    TVersionedRow ProduceSingleRowVersion(TDynamicRow dynamicRow)
    {
        if (Timestamp_ != LastCommittedTimestamp && dynamicRow.GetPrepareTimestamp() < Timestamp_) {
            WaitFor(dynamicRow.GetTransaction()->GetFinished());
        }

        auto timestampList = dynamicRow.GetTimestampList(KeyCount_);
        const auto* minTimestampPtr = std::get<0>(FindVersionedValue(
            timestampList,
            Timestamp_,
            [] (TTimestamp value) {
                return value & TimestampValueMask;
            }));

        if (!minTimestampPtr) {
            return TVersionedRow();
        }

        auto versionedRow = TVersionedRow::Allocate(&Pool_, KeyCount_, SchemaColumnCount_ - KeyCount_, 1);
        memcpy(versionedRow.BeginKeys(), dynamicRow.GetKeys(), KeyCount_ * sizeof (TUnversionedValue));

        auto minTimestamp = *minTimestampPtr;
        if (minTimestamp & TombstoneTimestampMask) {
            versionedRow.GetHeader()->ValueCount = 0;
            versionedRow.BeginTimestamps()[0] = minTimestamp;
            return versionedRow;
        }

        auto* currentRowValue = versionedRow.BeginValues();
        auto fillValue = [&] (int index) {
            auto list = dynamicRow.GetFixedValueList(index, KeyCount_);
            auto* value = std::get<0>(FindVersionedValue(
                list,
                Timestamp_,
                [] (const TVersionedValue& value) {
                    return value.Timestamp;
                }));
            if (value && value->Timestamp >= minTimestamp) {
                *currentRowValue++ = *value;
            }
        };

        if (ColumnFilter_.All) {
            for (int index = 0; index < SchemaColumnCount_ - KeyCount_; ++index) {
                fillValue(index);
            }
        } else {
            for (int index : ColumnFilter_.Indexes) {
                fillValue(index - KeyCount_);
            }
        }

        versionedRow.GetHeader()->ValueCount = currentRowValue - versionedRow.BeginValues();

        versionedRow.BeginTimestamps()[0] =
            minTimestamp |
            (minTimestampPtr == timestampList.Begin() ? IncrementalTimestampMask : 0);

        return versionedRow;
    }

    TVersionedRow ProduceAllRowVersions(TDynamicRow dynamicRow)
    {
        auto timestampList = dynamicRow.GetTimestampList(KeyCount_);
        if (!timestampList) {
            return TVersionedRow();
        }

        int timestampCount = timestampList.GetSize() + timestampList.GetSuccessorsSize();
        if (timestampList.Back() & UncommittedTimestamp) {
            --timestampCount;
        }

        if (timestampCount == 0) {
            return TVersionedRow();
        }

        int valueCount = 0;
        for (int index = 0; index < SchemaColumnCount_ - KeyCount_; ++index) {
            auto list = dynamicRow.GetFixedValueList(index, KeyCount_);
            if (list) {
                valueCount += list.GetSize() + list.GetSuccessorsSize();
                if (list.Back().Timestamp & UncommittedTimestamp) {
                    --valueCount;
                }
            }
        }

        auto versionedRow = TVersionedRow::Allocate(&Pool_, KeyCount_, valueCount, timestampCount);

        // Keys.
        memcpy(versionedRow.BeginKeys(), dynamicRow.GetKeys(), KeyCount_ * sizeof (TUnversionedValue));

        // Fixed values.
        auto* currentValue = versionedRow.BeginValues();
        for (int index = 0; index < SchemaColumnCount_ - KeyCount_; ++index) {
            auto currentList = dynamicRow.GetFixedValueList(index, KeyCount_);
            while (currentList) {
                for (const auto* it = &currentList.Back(); it >= &currentList.Front(); --it) {
                    const auto& value = *it;
                    if (!(value.Timestamp & UncommittedTimestamp)) {
                        *currentValue++ = value;
                    }
                }
                currentList = currentList.GetNext();
            }
        }
        YASSERT(currentValue == versionedRow.EndValues());

        // Timestamps.
        auto currentTimestampList = timestampList;
        auto* currentTimestamp = versionedRow.BeginTimestamps();
        while (currentTimestampList) {
            for (const auto* it = &currentTimestampList.Back(); it >= &currentTimestampList.Front(); --it) {
                auto timestamp = *it;
                if (!(timestamp & UncommittedTimestamp)) {
                    *currentTimestamp++ = timestamp;
                }
            }
            currentTimestampList = currentTimestampList.GetNext();
        }
        YASSERT(currentTimestamp == versionedRow.EndTimestamps());

        return versionedRow;
    }

};

////////////////////////////////////////////////////////////////////////////////

TDynamicMemoryStore::TDynamicMemoryStore(
    TTabletManagerConfigPtr config,
    TTablet* tablet)
    : Config_(config)
    , Tablet_(tablet)
    , LockCount_(0)
    , Active_(true)
    , KeyCount_(static_cast<int>(Tablet_->KeyColumns().size()))
    , SchemaColumnCount_(static_cast<int>(Tablet_->Schema().Columns().size()))
    , AllocatedStringSpace_(0)
    , AllocatedValueCount_(0)
    , AlignedPool_(Config_->AlignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , UnalignedPool_(Config_->UnalignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , Comparer_(new TKeyPrefixComparer(KeyCount_))
    , Tree_(new TRcuTree<TDynamicRow, TKeyPrefixComparer>(&AlignedPool_, Comparer_.get()))
{
    LOG_DEBUG("Dynamic memory store created (TabletId: %s, This: %p)",
        ~ToString(Tablet_->GetId()),
        this);
}

TDynamicMemoryStore::~TDynamicMemoryStore()
{
    LOG_DEBUG("Dynamic memory store destroyed (TabletId: %s, This: %p)",
        ~ToString(Tablet_->GetId()),
        this);
}

TTablet* TDynamicMemoryStore::GetTablet() const
{
    return Tablet_;
}

int TDynamicMemoryStore::GetLockCount() const
{
    return LockCount_;
}

int TDynamicMemoryStore::Lock()
{
    return ++LockCount_;
}

int TDynamicMemoryStore::Unlock()
{
    return --LockCount_;
}

bool TDynamicMemoryStore::IsActive() const
{
    return Active_;
}

void TDynamicMemoryStore::MakePassive()
{
    Active_ = false;
}

TDynamicRow TDynamicMemoryStore::WriteRow(
    const TNameTablePtr& nameTable,
    TTransaction* transaction,
    TUnversionedRow row,
    bool prewrite)
{
    YASSERT(Active_);

    TDynamicRow result;

    auto writeFixedValue = [&] (TDynamicRow dynamicRow, const TUnversionedValue& srcValue) {
        int listIndex = srcValue.Id - KeyCount_;
        auto list = dynamicRow.GetFixedValueList(listIndex, KeyCount_);

        if (list) {
            auto& lastValue = list.Back();
            if ((lastValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                CopyValue(&lastValue, srcValue);
                return;
            }

            if (AllocateListIfNeeded(&list, &AlignedPool_)) {
                dynamicRow.SetFixedValueList(listIndex, list, KeyCount_);
            }
        } else {
            list = TValueList::Allocate(&AlignedPool_, InitialEditListCapacity);
            dynamicRow.SetFixedValueList(listIndex, list, KeyCount_);
        }

        list.Push([&] (TVersionedValue* dstValue) {
            CopyValue(dstValue, srcValue);
            dstValue->Timestamp = UncommittedTimestamp;
        });

        ++AllocatedValueCount_;
    };

    auto writeValues = [&] (TDynamicRow dynamicRow) {
        for (int index = KeyCount_; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            writeFixedValue(dynamicRow, value);
        }
    };

    auto writeTimestamp = [&] (TDynamicRow dynamicRow) {
        bool pushTimestamp = false;
        auto timestampList = dynamicRow.GetTimestampList(KeyCount_);
        if (timestampList) {
            auto lastTimestamp = timestampList.Back();
            if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
                YASSERT(!(lastTimestamp & TombstoneTimestampMask));
            } else {
                if (lastTimestamp & TombstoneTimestampMask) {
                    pushTimestamp = true;
                }
            }
        } else {
            timestampList = TTimestampList::Allocate(&AlignedPool_, InitialEditListCapacity);
            dynamicRow.SetTimestampList(timestampList, KeyCount_);
            pushTimestamp = true;
        }

        if (pushTimestamp) {
            if (AllocateListIfNeeded(&timestampList, &AlignedPool_)) {
                dynamicRow.SetTimestampList(timestampList, KeyCount_);
            }
            timestampList.Push(UncommittedTimestamp);
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        // Acquire the lock.
        auto dynamicRow = result = AllocateRow();
        YCHECK(LockRow(dynamicRow, transaction, ERowLockMode::Write, prewrite));

        // Add timestamp.
        writeTimestamp(dynamicRow);

        // Copy keys.
        for (int id = 0; id < KeyCount_; ++id) {
            auto& dstValue = dynamicRow.GetKeys()[id];
            CopyValue(&dstValue, row[id]);
            dstValue.Id = id;
        }

        // Copy values.
        writeValues(dynamicRow);

        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Check for lock conflicts and acquire the lock.
        if (LockRow(dynamicRow, transaction, ERowLockMode::Write, prewrite)) {
            result = dynamicRow;
        }

        // Add timestamp, if needed.
        writeTimestamp(dynamicRow);

        // Copy values.
        writeValues(dynamicRow);
    };

    Tree_->Insert(
        row,
        newKeyProvider,
        existingKeyConsumer);

    return result;
}

TDynamicRow TDynamicMemoryStore::DeleteRow(
    TTransaction* transaction,
    NVersionedTableClient::TKey key,
    bool prewrite)
{
    YASSERT(Active_);

    TDynamicRow result;

    auto writeTombstone = [&] (TDynamicRow dynamicRow) {
        bool pushTimestamp = false;
        auto timestampList = dynamicRow.GetTimestampList(KeyCount_);
        if (timestampList) {
            auto lastTimestamp = timestampList.Back();
            if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
                YASSERT(lastTimestamp & TombstoneTimestampMask);
            } else {
                pushTimestamp = true;
            }
        } else {
            timestampList = TTimestampList::Allocate(&AlignedPool_, InitialEditListCapacity);
            dynamicRow.SetTimestampList(timestampList, KeyCount_);
            pushTimestamp = true;
        }

        if (pushTimestamp) {
            if (AllocateListIfNeeded(&timestampList, &AlignedPool_)) {
                dynamicRow.SetTimestampList(timestampList, KeyCount_);
            }
            timestampList.Push(UncommittedTimestamp | TombstoneTimestampMask);
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        // Acquire the lock.
        auto dynamicRow = result = AllocateRow();
        YCHECK(LockRow(dynamicRow, transaction, ERowLockMode::Delete, prewrite));

        // Add tombstone.
        writeTombstone(dynamicRow);

        // Copy keys.
        for (int id = 0; id < KeyCount_; ++id) {
            auto& internedValue = dynamicRow.GetKeys()[id];
            CopyValue(&internedValue, key[id]);
            internedValue.Id = id;
        }

        result = dynamicRow;
        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Check for lock conflicts and acquire the lock.
        if (LockRow(dynamicRow, transaction, ERowLockMode::Delete, prewrite)) {
            result = dynamicRow;
        }

        // Add tombstone.
        writeTombstone(dynamicRow);
    };

    Tree_->Insert(
        key,
        newKeyProvider,
        existingKeyConsumer);

    return result;
}

IVersionedReaderPtr TDynamicMemoryStore::CreateReader(
    NVersionedTableClient::TKey lowerKey,
    NVersionedTableClient::TKey upperKey,
    TTimestamp timestamp,
    const NApi::TColumnFilter& columnFilter)
{
    return New<TReader>(
        this,
        lowerKey,
        upperKey,
        timestamp,
        columnFilter);
}

TDynamicRow TDynamicMemoryStore::MigrateRow(
    TDynamicRow row,
    const TDynamicMemoryStorePtr& migrateTo)
{
    TDynamicRow migratedRow;
    auto newKeyProvider = [&] () -> TDynamicRow {
        // Create migrated row.
        migratedRow = migrateTo->AllocateRow();

        // Migrate lock.
        migrateTo->Lock();
        auto* transaction = row.GetTransaction();
        int lockIndex = row.GetLockIndex();
        migratedRow.Lock(transaction, lockIndex, row.GetLockMode());
        migratedRow.SetPrepareTimestamp(row.GetPrepareTimestamp());
        if (lockIndex != -1) {
            transaction->LockedRows()[lockIndex] = TDynamicRowRef(migrateTo.Get(), migratedRow);
        }

        // Migrate keys.
        const auto* srcKeys = row.GetKeys();
        auto* dstKeys = migratedRow.GetKeys();
        for (int index = 0; index < KeyCount_; ++index) {
            migrateTo->CopyValue(&dstKeys[index], srcKeys[index]);
        }

        // Migrate fixed values.
        for (int index = 0; index < SchemaColumnCount_ - KeyCount_; ++index) {
            auto list = row.GetFixedValueList(index, KeyCount_);
            if (list) {
                const auto& srcValue = list.Back();
                if ((srcValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    auto migratedList = TValueList::Allocate(&migrateTo->AlignedPool_, InitialEditListCapacity);
                    migratedRow.SetFixedValueList(index, migratedList, KeyCount_);
                    migratedList.Push([&] (TVersionedValue* dstValue) {
                        migrateTo->CopyValue(dstValue, srcValue);
                    });
                }
            }
        }

        // Migrate timestamps.
        auto timestampList = row.GetTimestampList(KeyCount_);
        if (timestampList) {
            auto timestamp = timestampList.Back();
            if ((timestamp & TimestampValueMask) == UncommittedTimestamp) {
                auto migratedTimestampList = TTimestampList::Allocate(&migrateTo->AlignedPool_, InitialEditListCapacity);
                migratedRow.SetTimestampList(migratedTimestampList, KeyCount_);
                migratedTimestampList.Push(timestamp);
            }
        }

        Unlock();
        row.Unlock();

        return migratedRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow /*dynamicRow*/) {
        YUNREACHABLE();
    };

    migrateTo->Tree_->Insert(
        row,
        newKeyProvider,
        existingKeyConsumer);

    return migratedRow;
}

TDynamicRow TDynamicMemoryStore::CheckLockAndMaybeMigrateRow(
    NVersionedTableClient::TKey key,
    TTransaction* transaction,
    ERowLockMode mode,
    const TDynamicMemoryStorePtr& migrateTo)
{
    TDynamicRow row;
    TRcuTreeScannerPtr<TDynamicRow, TKeyPrefixComparer> scanner(Tree_.get());
    if (!scanner->Find(key, &row)) {
        return TDynamicRow();
    }

    CheckRowLock(row, transaction, mode);

    if (row.GetLockMode() == ERowLockMode::None) {
        return TDynamicRow();
    }

    return MigrateRow(row, migrateTo);
}

void TDynamicMemoryStore::ConfirmRow(TDynamicRow row)
{
    YASSERT(Active_);

    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    
    int lockIndex = static_cast<int>(transaction->LockedRows().size());
    transaction->LockedRows().push_back(TDynamicRowRef(this, row));
    row.SetLockIndex(lockIndex);
}

void TDynamicMemoryStore::PrepareRow(TDynamicRow row)
{
    YASSERT(Active_);

    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    row.SetPrepareTimestamp(transaction->GetPrepareTimestamp());
}

void TDynamicMemoryStore::CommitRow(TDynamicRow row)
{
    YASSERT(Active_);

    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    auto commitTimestamp = transaction->GetCommitTimestamp();

    // Edit timestamps.
    auto timestampList = row.GetTimestampList(KeyCount_);
    if (timestampList) {
        auto& lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            lastTimestamp = (lastTimestamp & ~TimestampValueMask) | commitTimestamp;
        }
    }

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount_ - KeyCount_; ++index) {
        auto list = row.GetFixedValueList(index, KeyCount_);
        if (list) {
            auto& lastValue = list.Back();
            if (lastValue.Timestamp == UncommittedTimestamp) {
                lastValue.Timestamp = commitTimestamp;
            }
        }
    }

    Unlock();
    row.Unlock();
    row.SetLastCommitTimestamp(transaction->GetCommitTimestamp());
}

void TDynamicMemoryStore::AbortRow(TDynamicRow row)
{
    if (Active_) {
        // Timestamps.
        auto timestampList = row.GetTimestampList(KeyCount_);
        if (timestampList) {
            auto lastTimestamp = timestampList.Back();
            if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
                if (timestampList.Pop() == 0) {
                    row.SetTimestampList(timestampList.GetNext(), KeyCount_);
                }
            }
        }

        // Fixed values.
        for (int index = 0; index < SchemaColumnCount_ - KeyCount_; ++index) {
            auto list = row.GetFixedValueList(index, KeyCount_);
            if (list) {
                const auto& lastValue = list.Back();
                if ((lastValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    if (list.Pop() == 0) {
                        row.SetFixedValueList(index, list.GetNext(), KeyCount_);
                    }
                }
            }
        }
    }

    Unlock();
    row.Unlock();
}

TDynamicRow TDynamicMemoryStore::AllocateRow()
{
    return TDynamicRow::Allocate(
        &AlignedPool_,
        KeyCount_,
        SchemaColumnCount_);
}

void TDynamicMemoryStore::CheckRowLock(
    TDynamicRow row,
    TTransaction* transaction,
    ERowLockMode mode)
{
    auto* existingTransaction = row.GetTransaction();
    if (existingTransaction) {
        if (existingTransaction == transaction) {
            if (row.GetLockMode() != mode) {
                THROW_ERROR_EXCEPTION("Cannot change row lock mode from %s to %s",
                    ~FormatEnum(row.GetLockMode()).Quote(),
                    ~FormatEnum(mode).Quote());
            }
        } else {
            THROW_ERROR_EXCEPTION("Row lock conflict with concurrent transaction %s",
                ~ToString(existingTransaction->GetId()));
        }
    }

    if (row.GetLastCommitTimestamp() >= transaction->GetStartTimestamp()) {
        THROW_ERROR_EXCEPTION("Row lock conflict with a transaction committed at %" PRIu64,
            row.GetLastCommitTimestamp());
    }
}

bool TDynamicMemoryStore::LockRow(
    TDynamicRow row,
    TTransaction* transaction,
    ERowLockMode mode,
    bool prewrite)
{
    CheckRowLock(row, transaction, mode);

    if (row.GetLockMode() != ERowLockMode::None) {
        YASSERT(row.GetTransaction() == transaction);
        return false;
    }

    int lockIndex = -1;
    if (!prewrite) {
        lockIndex = static_cast<int>(transaction->LockedRows().size());
        transaction->LockedRows().push_back(TDynamicRowRef(this, row));
    }
    
    Lock();
    row.Lock(transaction, lockIndex, mode);

    return true;
}

void TDynamicMemoryStore::CopyValue(TUnversionedValue* dst, const TUnversionedValue& src)
{
    *dst = src;
    if (src.Type == EValueType::String || src.Type == EValueType::Any) {
        dst->Data.String = UnalignedPool_.AllocateUnaligned(src.Length);
        memcpy(const_cast<char*>(dst->Data.String), src.Data.String, src.Length);
        AllocatedStringSpace_ += src.Length;
    }
}

i64 TDynamicMemoryStore::GetAllocatedStringSpace() const
{
    return AllocatedStringSpace_;
}

int TDynamicMemoryStore::GetAllocatedValueCount() const
{
    return AllocatedValueCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
