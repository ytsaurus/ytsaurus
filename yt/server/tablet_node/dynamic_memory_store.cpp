#include "stdafx.h"
#include "dynamic_memory_store.h"
#include "tablet.h"
#include "transaction.h"
#include "config.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "transaction_manager.h"
#include "private.h"

#include <core/misc/small_vector.h>
#include <core/misc/skip_list.h>

#include <core/concurrency/fiber.h>

#include <core/ytree/fluent.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/api/transaction.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
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
static const int TypicalEditListCount = 16;
static const int ReaderPoolSize = 1024;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T, class TTimestampExtractor>
T* SearchList(
    TEditList<T> list,
    TTimestamp maxTimestamp,
    const TTimestampExtractor& timestampExtractor)
{
    if (!list) {
        return nullptr;
    }

    if (maxTimestamp == LastCommittedTimestamp) {
        auto& value = list.Back();
        if (timestampExtractor(value) != UncommittedTimestamp) {
            return &value;
        }
        if (list.GetSize() > 1) {
            return &value - 1;
        }
        auto nextList = list.GetNext();
        if (!nextList) {
            return nullptr;
        }
        return &nextList.Back();
    } else {
        while (true) {
            if (!list) {
                return nullptr;
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

        return left && timestampExtractor(*left) <= maxTimestamp
            ? left
            : nullptr;
    }
}

template <class T>
bool AllocateListForPushIfNeeded(
    TEditList<T>* list,
    TChunkedMemoryPool* pool)
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

template <class T>
void EnumerateListsAndReverse(
    TEditList<T> list,
    SmallVector<TEditList<T>, TypicalEditListCount>* result)
{
    result->clear();
    while (list) {
        result->push_back(list);
        list = list.GetNext();
    }
    std::reverse(result->begin(), result->end());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore::TReader
    : public IVersionedReader
{
public:
    TReader(
        TDynamicMemoryStorePtr store,
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter)
        : Store_(store)
        , LowerKey_(std::move(lowerKey))
        , UpperKey_(std::move(upperKey))
        , Timestamp_(timestamp)
        , ColumnFilter_(columnFilter)
        , KeyColumnCount_(Store_->Tablet_->GetKeyColumnCount())
        , SchemaColumnCount_(Store_->Tablet_->GetSchemaColumnCount())
        , Pool_(ReaderPoolSize)
        , Finished_(false)
    {
        YCHECK(Timestamp_ != AllCommittedTimestamp || ColumnFilter_.All);
    }

    virtual TAsyncError Open() override
    {
        Iterator_ = Store_->Rows_->FindGreaterThanOrEqualTo(LowerKey_);
        static auto result = MakeFuture(TError());
        return result;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        if (Finished_) {
            return false;
        }

        YASSERT(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        TKeyComparer keyComparer(KeyColumnCount_);

        while (Iterator_.IsValid() && rows->size() < rows->capacity()) {
            const auto* rowKeys = Iterator_.GetCurrent().GetKeys();
            if (CompareRows(rowKeys, rowKeys + KeyColumnCount_, UpperKey_.Begin(), UpperKey_.End()) >= 0)
                break;

            auto row = ProduceRow();
            if (row) {
                rows->push_back(row);
            }

            Iterator_.MoveNext();
        }

        if (rows->empty()) {
            Finished_ = true;
            return false;
        }

        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        static auto presetResult = MakeFuture(TError());
        return presetResult;
    }

private:
    TDynamicMemoryStorePtr Store_;
    TOwningKey LowerKey_;
    TOwningKey UpperKey_;
    TTimestamp Timestamp_;
    TColumnFilter ColumnFilter_;

    int KeyColumnCount_;
    int SchemaColumnCount_;

    TSkipList<TDynamicRow, TKeyComparer>::TIterator Iterator_;

    TChunkedMemoryPool Pool_;
    
    bool Finished_;

    TVersionedRow ProduceRow()
    {
        auto dynamicRow = Iterator_.GetCurrent();
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

        auto timestampList = dynamicRow.GetTimestampList(KeyColumnCount_);
        const auto* minTimestampPtr = SearchList(
            timestampList,
            Timestamp_,
            [] (TTimestamp value) {
                return value & TimestampValueMask;
            });

        if (!minTimestampPtr) {
            return TVersionedRow();
        }

        auto versionedRow = TVersionedRow::Allocate(&Pool_, KeyColumnCount_, SchemaColumnCount_ - KeyColumnCount_, 1);
        memcpy(versionedRow.BeginKeys(), dynamicRow.GetKeys(), KeyColumnCount_ * sizeof (TUnversionedValue));

        auto minTimestamp = *minTimestampPtr;
        if (minTimestamp & TombstoneTimestampMask) {
            versionedRow.GetHeader()->ValueCount = 0;
            versionedRow.BeginTimestamps()[0] = minTimestamp;
            return versionedRow;
        }

        auto* currentRowValue = versionedRow.BeginValues();
        auto fillValue = [&] (int index) {
            auto list = dynamicRow.GetFixedValueList(index, KeyColumnCount_);
            const auto* value = SearchList(
                list,
                Timestamp_,
                [] (const TVersionedValue& value) {
                    return value.Timestamp;
                });
            if (value && value->Timestamp >= minTimestamp) {
                *currentRowValue++ = *value;
            }
        };

        if (ColumnFilter_.All) {
            for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
                fillValue(index);
            }
        } else {
            for (int index : ColumnFilter_.Indexes) {
                if (index >= KeyColumnCount_) {
                    fillValue(index - KeyColumnCount_);
                }
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
        auto timestampList = dynamicRow.GetTimestampList(KeyColumnCount_);
        if (!timestampList) {
            return TVersionedRow();
        }

        int timestampCount = timestampList.GetSize() + timestampList.GetSuccessorsSize();
        if ((timestampList.Back() & TimestampValueMask) == UncommittedTimestamp) {
            --timestampCount;
        }

        if (timestampCount == 0) {
            return TVersionedRow();
        }

        int valueCount = 0;
        for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
            auto list = dynamicRow.GetFixedValueList(index, KeyColumnCount_);
            if (list) {
                valueCount += list.GetSize() + list.GetSuccessorsSize();
                if ((list.Back().Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    --valueCount;
                }
            }
        }

        auto versionedRow = TVersionedRow::Allocate(&Pool_, KeyColumnCount_, valueCount, timestampCount);

        // Keys.
        memcpy(versionedRow.BeginKeys(), dynamicRow.GetKeys(), KeyColumnCount_ * sizeof (TUnversionedValue));

        // Fixed values.
        auto* currentValue = versionedRow.BeginValues();
        for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
            auto currentList = dynamicRow.GetFixedValueList(index, KeyColumnCount_);
            while (currentList) {
                for (const auto* it = &currentList.Back(); it >= &currentList.Front(); --it) {
                    const auto& value = *it;
                    if ((value.Timestamp & TimestampValueMask) != UncommittedTimestamp) {
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
                if ((timestamp & TimestampValueMask) != UncommittedTimestamp) {
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
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(
        id,
        tablet)
    , Config_(config)
    , LockCount_(0)
    , KeyColumnCount_(Tablet_->GetKeyColumnCount())
    , SchemaColumnCount_(Tablet_->GetSchemaColumnCount())
    , StringSpace_(0)
    , ValueCount_(0)
    , AlignedPool_(Config_->AlignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , UnalignedPool_(Config_->UnalignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , Rows_(new TSkipList<TDynamicRow, TKeyComparer>(
        &AlignedPool_,
        TKeyComparer(KeyColumnCount_)))
{
    State_ = EStoreState::ActiveDynamic;

    LOG_DEBUG("Dynamic memory store created (TabletId: %s, StoreId: %s)",
        ~ToString(Tablet_->GetId()),
        ~ToString(Id_));
}

TDynamicMemoryStore::~TDynamicMemoryStore()
{
    LOG_DEBUG("Dynamic memory store destroyed (StoreId: %s)",
        ~ToString(Id_));
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

TDynamicRow TDynamicMemoryStore::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prewrite)
{
    YASSERT(State_ == EStoreState::ActiveDynamic);

    TDynamicRow result;

    auto addValues = [&] (TDynamicRow dynamicRow) {
        for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            AddUncommittedFixedValue(dynamicRow, value.Id - KeyColumnCount_, value);
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        // Acquire the lock.
        auto dynamicRow = result = AllocateRow();
        YCHECK(LockRow(dynamicRow, transaction, ERowLockMode::Write, prewrite));

        // Add timestamp.
        AddUncommittedTimestamp(dynamicRow, UncommittedTimestamp);

        // Copy keys.
        for (int id = 0; id < KeyColumnCount_; ++id) {
            auto& dstValue = dynamicRow.GetKeys()[id];
            CaptureValue(&dstValue, row[id]);
            dstValue.Id = id;
        }

        // Copy values.
        addValues(dynamicRow);

        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Check for lock conflicts and acquire the lock.
        if (LockRow(dynamicRow, transaction, ERowLockMode::Write, prewrite)) {
            result = dynamicRow;
        }

        // Add timestamp, if needed.
        AddUncommittedTimestamp(dynamicRow, UncommittedTimestamp);

        // Copy values.
        addValues(dynamicRow);
    };

    Rows_->Insert(
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
    YASSERT(State_ == EStoreState::ActiveDynamic);

    TDynamicRow result;

    auto newKeyProvider = [&] () -> TDynamicRow {
        // Acquire the lock.
        auto dynamicRow = result = AllocateRow();
        YCHECK(LockRow(dynamicRow, transaction, ERowLockMode::Delete, prewrite));

        // Add tombstone.
        AddUncommittedTimestamp(dynamicRow, UncommittedTimestamp | TombstoneTimestampMask);

        // Copy keys.
        for (int id = 0; id < KeyColumnCount_; ++id) {
            auto& internedValue = dynamicRow.GetKeys()[id];
            CaptureValue(&internedValue, key[id]);
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
        AddUncommittedTimestamp(dynamicRow, UncommittedTimestamp | TombstoneTimestampMask);
    };

    Rows_->Insert(
        key,
        newKeyProvider,
        existingKeyConsumer);

    return result;
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
        if (lockIndex != TDynamicRow::InvalidLockIndex) {
            transaction->LockedRows()[lockIndex] = TDynamicRowRef(migrateTo.Get(), migratedRow);
        }

        // Migrate keys.
        const auto* srcKeys = row.GetKeys();
        auto* dstKeys = migratedRow.GetKeys();
        for (int index = 0; index < KeyColumnCount_; ++index) {
            migrateTo->CaptureValue(&dstKeys[index], srcKeys[index]);
        }

        // Migrate fixed values.
        for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
            auto list = row.GetFixedValueList(index, KeyColumnCount_);
            if (list) {
                const auto& srcValue = list.Back();
                if ((srcValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    auto migratedList = TValueList::Allocate(&migrateTo->AlignedPool_, InitialEditListCapacity);
                    migratedRow.SetFixedValueList(index, migratedList, KeyColumnCount_);
                    migratedList.Push([&] (TVersionedValue* dstValue) {
                        migrateTo->CaptureValue(dstValue, srcValue);
                    });
                    ++ValueCount_;
                }
            }
        }

        // Migrate timestamps.
        auto timestampList = row.GetTimestampList(KeyColumnCount_);
        if (timestampList) {
            auto timestamp = timestampList.Back();
            if ((timestamp & TimestampValueMask) == UncommittedTimestamp) {
                auto migratedTimestampList = TTimestampList::Allocate(&migrateTo->AlignedPool_, InitialEditListCapacity);
                migratedRow.SetTimestampList(migratedTimestampList, KeyColumnCount_);
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

    migrateTo->Rows_->Insert(
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
    auto it = Rows_->FindEqualTo(key);
    if (!it.IsValid()) {
        return TDynamicRow();
    }

    auto row = it.GetCurrent();

    CheckRowLock(row, transaction, mode);

    if (row.GetLockMode() == ERowLockMode::None) {
        return TDynamicRow();
    }

    return MigrateRow(row, migrateTo);
}

void TDynamicMemoryStore::ConfirmRow(TDynamicRow row)
{
    YASSERT(State_ == EStoreState::ActiveDynamic);

    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    
    int lockIndex = static_cast<int>(transaction->LockedRows().size());
    transaction->LockedRows().push_back(TDynamicRowRef(this, row));
    row.SetLockIndex(lockIndex);
}

void TDynamicMemoryStore::PrepareRow(TDynamicRow row)
{
    YASSERT(State_ == EStoreState::ActiveDynamic);

    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    row.SetPrepareTimestamp(transaction->GetPrepareTimestamp());
}

void TDynamicMemoryStore::CommitRow(TDynamicRow row)
{
    YASSERT(State_ == EStoreState::ActiveDynamic);

    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    auto commitTimestamp = transaction->GetCommitTimestamp();

    // Edit timestamps.
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (timestampList) {
        auto& lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            lastTimestamp = (lastTimestamp & ~TimestampValueMask) | commitTimestamp;
        }
    }

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
        auto list = row.GetFixedValueList(index, KeyColumnCount_);
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
    DropUncommittedValues(row);
    Unlock();
    row.Unlock();
}

TDynamicRow TDynamicMemoryStore::AllocateRow()
{
    return TDynamicRow::Allocate(
        &AlignedPool_,
        KeyColumnCount_,
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

    int lockIndex = TDynamicRow::InvalidLockIndex;
    if (!prewrite) {
        lockIndex = static_cast<int>(transaction->LockedRows().size());
        transaction->LockedRows().push_back(TDynamicRowRef(this, row));
    }
    
    Lock();
    row.Lock(transaction, lockIndex, mode);

    return true;
}

void TDynamicMemoryStore::DropUncommittedValues(TDynamicRow row)
{
    // Timestamps.
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (timestampList) {
        auto lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            if (timestampList.Pop() == 0) {
                row.SetTimestampList(timestampList.GetNext(), KeyColumnCount_);
            }
        }
    }

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount_ - KeyColumnCount_; ++index) {
        auto list = row.GetFixedValueList(index, KeyColumnCount_);
        if (list) {
            const auto& lastValue = list.Back();
            if ((lastValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                if (list.Pop() == 0) {
                    row.SetFixedValueList(index, list.GetNext(), KeyColumnCount_);
                }
            }
        }
    }
}

void TDynamicMemoryStore::AddFixedValue(
    TDynamicRow row,
    int listIndex,
    const TVersionedValue& value)
{
    auto list = row.GetFixedValueList(listIndex, KeyColumnCount_);

    if (list) {
        auto& lastValue = list.Back();
        if ((lastValue.Timestamp & TimestampValueMask) == UncommittedTimestamp) {
            CaptureValue(&lastValue, value);
            return;
        }

        if (AllocateListForPushIfNeeded(&list, &AlignedPool_)) {
            row.SetFixedValueList(listIndex, list, KeyColumnCount_);
        }
    } else {
        list = TValueList::Allocate(&AlignedPool_, InitialEditListCapacity);
        row.SetFixedValueList(listIndex, list, KeyColumnCount_);
    }

    list.Push([&] (TVersionedValue* dstValue) {
        CaptureValue(dstValue, value);
    });

    ++ValueCount_;
}

void TDynamicMemoryStore::AddUncommittedFixedValue(
    TDynamicRow row,
    int listIndex,
    const TUnversionedValue& value)
{
    AddFixedValue(row, listIndex, MakeVersionedValue(value, UncommittedTimestamp));
}

void TDynamicMemoryStore::AddTimestamp(TDynamicRow row, TTimestamp timestamp)
{
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (timestampList) {
        if (AllocateListForPushIfNeeded(&timestampList, &AlignedPool_)) {
            row.SetTimestampList(timestampList, KeyColumnCount_);
        }
    } else {
        timestampList = TTimestampList::Allocate(&AlignedPool_, InitialEditListCapacity);
        row.SetTimestampList(timestampList, KeyColumnCount_);
    }
    timestampList.Push(timestamp);
}

void TDynamicMemoryStore::AddUncommittedTimestamp(TDynamicRow row, TTimestamp timestamp)
{
    YASSERT((timestamp & TimestampValueMask) == UncommittedTimestamp);

    bool pushTimestamp = false;
    auto timestampList = row.GetTimestampList(KeyColumnCount_);
    if (timestampList) {
        auto lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            YASSERT((timestamp & TombstoneTimestampMask) == (lastTimestamp & TombstoneTimestampMask));
        } else {
            if ((lastTimestamp & TombstoneTimestampMask) != (timestamp & TombstoneTimestampMask)) {
                pushTimestamp = true;
            }
        }
    } else {
        timestampList = TTimestampList::Allocate(&AlignedPool_, InitialEditListCapacity);
        row.SetTimestampList(timestampList, KeyColumnCount_);
        pushTimestamp = true;
    }

    if (pushTimestamp) {
        if (AllocateListForPushIfNeeded(&timestampList, &AlignedPool_)) {
            row.SetTimestampList(timestampList, KeyColumnCount_);
        }
        timestampList.Push(timestamp);
    }
}

void TDynamicMemoryStore::CaptureValue(TUnversionedValue* dst, const TUnversionedValue& src)
{
    *dst = src;
    CaptureValueData(dst, src);
}

void TDynamicMemoryStore::CaptureValue(TVersionedValue* dst, const TVersionedValue& src)
{
    *dst = src;
    CaptureValueData(dst, src);
}

void TDynamicMemoryStore::CaptureValueData(TUnversionedValue* dst, const TUnversionedValue& src)
{
    if (src.Type == EValueType::String || src.Type == EValueType::Any) {
        dst->Data.String = UnalignedPool_.AllocateUnaligned(src.Length);
        memcpy(const_cast<char*>(dst->Data.String), src.Data.String, src.Length);
        StringSpace_ += src.Length;
    }
}

i64 TDynamicMemoryStore::GetStringSpace() const
{
    return StringSpace_;
}

int TDynamicMemoryStore::GetValueCount() const
{
    return ValueCount_;
}

int TDynamicMemoryStore::GetKeyCount() const
{
    return Rows_->GetSize();
}

TOwningKey TDynamicMemoryStore::GetMinKey() const
{
    return MinKey();
}

TOwningKey TDynamicMemoryStore::GetMaxKey() const
{
    return MaxKey();
}

IVersionedReaderPtr TDynamicMemoryStore::CreateReader(
    TOwningKey lowerKey,
    TOwningKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    return New<TReader>(
        this,
        std::move(lowerKey),
        std::move(upperKey),
        timestamp,
        columnFilter);
}

void TDynamicMemoryStore::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, State_);

    SmallVector<TValueList, TypicalEditListCount> valueLists;
    SmallVector<TTimestampList, TypicalEditListCount> timestampLists;

    // Rows
    Save(context, Rows_->GetSize());
    for (auto rowIt = Rows_->FindGreaterThanOrEqualTo(MinKey()); rowIt.IsValid(); rowIt.MoveNext()) {
        auto row = rowIt.GetCurrent();

        const auto* keyBegin = row.GetKeys();
        const auto* keyEnd = keyBegin + KeyColumnCount_;
        for (const auto* keyIt = keyBegin; keyIt != keyEnd; ++keyIt) {
            NVersionedTableClient::Save(context, *keyIt);
        }

        for (int listIndex = 0; listIndex < SchemaColumnCount_ - KeyColumnCount_; ++listIndex) {
            auto topList = row.GetFixedValueList(listIndex, KeyColumnCount_);
            if (topList) {
                int valueCount = topList.GetSize() + topList.GetSuccessorsSize();
                if (topList && (topList.Back().Timestamp & TimestampValueMask) == UncommittedTimestamp) {
                    --valueCount;
                }
                Save(context, valueCount);

                EnumerateListsAndReverse(topList, &valueLists);
                for (auto list : valueLists) {
                    for (const auto* valueIt = list.Begin(); valueIt != list.End(); ++valueIt) {
                        const auto& value = *valueIt;
                        if ((value.Timestamp & TimestampValueMask) != UncommittedTimestamp) {
                            NVersionedTableClient::Save(context, *valueIt);
                        }
                    }
                }
            } else {
                Save(context, static_cast<int>(0));
            }
        }

        {
            auto topList = row.GetTimestampList(KeyColumnCount_);
            if (topList) {
                int timestampCount = topList.GetSize() + topList.GetSuccessorsSize();
                if (topList && (topList.Back() & TimestampValueMask) == UncommittedTimestamp) {
                    --timestampCount;
                }
                Save(context, timestampCount);

                EnumerateListsAndReverse(topList, &timestampLists);
                for (auto list : timestampLists) {
                    for (const auto* timestampIt = list.Begin(); timestampIt != list.End(); ++timestampIt) {
                        auto timestamp = *timestampIt;
                        if ((timestamp & TimestampValueMask) != UncommittedTimestamp) {
                            Save(context, timestamp);
                        }
                    }
                }
            } else {
                Save(context, static_cast<int>(0));
            }
        }
    }
}

void TDynamicMemoryStore::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, State_);

    auto* slot = context.GetSlot();
    auto transactionManager = slot->GetTransactionManager();

    // Rows
    int rowCount = Load<int>(context);
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        auto row = AllocateRow();

        auto* keyBegin = row.GetKeys();
        auto* keyEnd = keyBegin + KeyColumnCount_;
        for (auto* keyIt = keyBegin; keyIt != keyEnd; ++keyIt) {
            NVersionedTableClient::Load(context, *keyIt, &UnalignedPool_);
        }

        for (int listIndex = 0; listIndex < SchemaColumnCount_ - KeyColumnCount_; ++listIndex) {
            int valueCount = Load<int>(context);
            for (int valueIndex = 0; valueIndex < valueCount; ++valueIndex) {
                TVersionedValue value;
                NVersionedTableClient::Load(context, value, &UnalignedPool_);
                AddFixedValue(row, listIndex, value);
            }
        }

        {
            int timestampCount = Load<int>(context);
            for (int timestampIndex = 0; timestampIndex < timestampCount; ++timestampIndex) {
                auto timestamp = Load<TTimestamp>(context);
                AddTimestamp(row, timestamp);
            }
        }

        Rows_->Insert(row);
    }
}

void TDynamicMemoryStore::BuildOrchidYson(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("key_count").Value(GetKeyCount())
        .Item("lock_count").Value(GetLockCount())
        .Item("value_count").Value(GetValueCount())
        .Item("string_space").Value(GetStringSpace());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
