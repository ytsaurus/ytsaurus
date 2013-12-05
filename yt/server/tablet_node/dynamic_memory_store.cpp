#include "stdafx.h"
#include "dynamic_memory_store.h"
#include "tablet.h"
#include "transaction.h"
#include "config.h"

#include <core/misc/small_vector.h>

#include <core/concurrency/fiber.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/writer.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

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

class TDynamicMemoryStore::TScanner
    : public IStoreScanner
{
public:
    explicit TScanner(TDynamicMemoryStorePtr store)
        : Store_(std::move(store))
        , KeyCount_(Store_->Tablet_->KeyColumns().size())
        , SchemaValueCount_(Store_->Tablet_->Schema().Columns().size())
        , TreeScanner_(Store_->Tree_.get())
    {
        ResetCurrentRow();
    }


    virtual TTimestamp Find(NVersionedTableClient::TKey key, TTimestamp timestamp) override
    {
        TDynamicRow row;
        if (!TreeScanner_->Find(key, &row)) {
            return ResetCurrentRow();
        }
        return SetCurrentRow(row, timestamp);
    }

    virtual TTimestamp BeginScan(NVersionedTableClient::TKey key, TTimestamp timestamp) override
    {
        TreeScanner_->BeginScan(key);
        if (!TreeScanner_->IsValid()) {
            return ResetCurrentRow();
        }
        return SetCurrentRow(TreeScanner_->GetCurrent(), timestamp);
    }

    virtual TTimestamp Advance() override
    {
        YASSERT(CurrentRow_);

        TreeScanner_->Advance();
        if (!TreeScanner_->IsValid()) {
            return ResetCurrentRow();
        }
        return SetCurrentRow(TreeScanner_->GetCurrent(), CurrentMaxTimestamp_);
    }

    virtual void EndScan() override
    {
        ResetCurrentRow();
    }

    virtual const TUnversionedValue* GetKeys() const override
    {
        YASSERT(CurrentRow_);

        return CurrentRow_.GetKeys();
    }

    virtual const TVersionedValue* GetFixedValue(int index) const override
    {
        YASSERT(CurrentRow_);
        YASSERT(index >= 0 && index < SchemaValueCount_ - KeyCount_);

        auto list = CurrentRow_.GetFixedValueList(index, KeyCount_);
        auto* value = std::get<0>(FindVersionedValue(
            list,
            CurrentMaxTimestamp_,
            [] (const TVersionedValue& value) {
                return value.Timestamp;
            }));
        return value && value->Timestamp >= CurrentMinTimestamp_ ? value : nullptr;
    }

    virtual void GetFixedValues(
        int index,
        int maxVersions,
        std::vector<TVersionedValue>* values) const override
    {
        YASSERT(CurrentRow_);
        YASSERT(index >= 0 && index < SchemaValueCount_ - KeyCount_);

        TVersionedValue* value;
        TValueList list;
        std::tie(value, list) = FindVersionedValue(
            CurrentRow_.GetFixedValueList(index, KeyCount_),
            CurrentMaxTimestamp_,
            [] (const TVersionedValue& value) {
                return value.Timestamp;
            });

        if (!value || value->Timestamp < CurrentMinTimestamp_)
            return;

        while (values->size() < maxVersions) {
            values->push_back(*value++);
            if (value == list.End()) {
                list = list.GetNext();
                if (!list)
                    break;
                value = list.Begin();
            }
        }
    }

    virtual void GetTimestamps(std::vector<TTimestamp>* timestamps) const override
    {
        YASSERT(CurrentRow_);

        TTimestamp* timestamp;
        TTimestampList list;
        std::tie(timestamp, list) = FindVersionedValue(
            CurrentRow_.GetTimestampList(KeyCount_),
            CurrentMaxTimestamp_,
            [] (TTimestamp value) {
                return value & TimestampValueMask;
            });

        if (!timestamp || *timestamp < CurrentMinTimestamp_)
            return;

        while (true) {
            timestamps->push_back(*timestamp++);
            if (timestamp == list.End()) {
                list = list.GetNext();
                if (!list)
                    break;
                timestamp = list.Begin();
            }
        }
    }

private:
    TDynamicMemoryStorePtr Store_;

    int KeyCount_;
    int SchemaValueCount_;
    
    TRcuTreeScannerPtr<TDynamicRow, TKeyPrefixComparer> TreeScanner_;

    TDynamicRow CurrentRow_;
    TTimestamp CurrentMaxTimestamp_;
    TTimestamp CurrentMinTimestamp_;


    TTimestamp SetCurrentRow(TDynamicRow row, TTimestamp timestamp)
    {
        if (timestamp != LastCommittedTimestamp && row.GetPrepareTimestamp() < timestamp) {
            WaitFor(row.GetTransaction()->GetFinished());
        }

        auto timestampList = row.GetTimestampList(KeyCount_);
        const auto* timestampMin = std::get<0>(FindVersionedValue(
            timestampList,
            timestamp,
            [] (TTimestamp value) {
                return value & TimestampValueMask;
            }));

        if (!timestampMin) {
            return NullTimestamp;
        }

        if (*timestampMin & TombstoneTimestampMask) {
            return *timestampMin;
        }

        CurrentRow_ = row;
        CurrentMinTimestamp_ = *timestampMin;
        CurrentMaxTimestamp_ = timestamp;

        auto result = *timestampMin;
        if (timestampMin == timestampList.Begin()) {
            result |= IncrementalTimestampMask;
        }
        return result;
    }

    TTimestamp ResetCurrentRow()
    {
        CurrentRow_ = TDynamicRow();
        CurrentMaxTimestamp_ = NullTimestamp;
        CurrentMinTimestamp_ = NullTimestamp;
        return NullTimestamp;
    }

};

////////////////////////////////////////////////////////////////////////////////

TDynamicMemoryStore::TDynamicMemoryStore(
    TTabletManagerConfigPtr config,
    TTablet* tablet)
    : Config_(config)
    , Tablet_(tablet)
    , KeyCount_(static_cast<int>(Tablet_->KeyColumns().size()))
    , SchemaColumnCount_(static_cast<int>(Tablet_->Schema().Columns().size()))
    , AllocatedStringSpace_(0)
    , AllocatedValueCount_(0)
    , AlignedPool_(Config_->AlignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , UnalignedPool_(Config_->UnalignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , Comparer_(new TKeyPrefixComparer(KeyCount_))
    , Tree_(new TRcuTree<TDynamicRow, TKeyPrefixComparer>(&AlignedPool_, Comparer_.get()))
{ }

TDynamicMemoryStore::~TDynamicMemoryStore()
{ }

TTablet* TDynamicMemoryStore::GetTablet() const
{
    return Tablet_;
}

TDynamicRow TDynamicMemoryStore::WriteRow(
    const TNameTablePtr& nameTable,
    TTransaction* transaction,
    TVersionedRow row,
    bool prewrite)
{
    TDynamicRow result;

    auto writeFixedValue = [&] (TDynamicRow dynamicRow, int id) {
        const auto& srcValue = row[id];

        int listIndex = id - KeyCount_;
        auto list = dynamicRow.GetFixedValueList(listIndex, KeyCount_);

        if (!list) {
            list = TValueList::Allocate(&AlignedPool_, InitialEditListCapacity);
            dynamicRow.SetFixedValueList(listIndex, KeyCount_, list);
        } else {
            auto& lastValue = list.Back();
            if (lastValue.Timestamp == UncommittedTimestamp) {
                CopyValue(&lastValue, srcValue);
                return;
            }

            if (AllocateListIfNeeded(&list, &AlignedPool_)) {
                dynamicRow.SetFixedValueList(listIndex, KeyCount_, list);
            }
        }

        list.Push([&] (TVersionedValue* dstValue) {
            CopyValue(dstValue, srcValue);
            dstValue->Timestamp = UncommittedTimestamp;
            dstValue->Id = id;
        });

        ++AllocatedValueCount_;
    };

    auto writeValues = [&] (TDynamicRow dynamicRow) {
        // Fixed values.
        for (int id = KeyCount_; id < SchemaColumnCount_; ++id) {
            writeFixedValue(dynamicRow, id);
        }

        // Variable values.
        // TODO(babenko)
    };

    auto writeTimestamp = [&] (TDynamicRow dynamicRow) {
        bool pushTimestamp = false;
        auto timestampList = dynamicRow.GetTimestampList(KeyCount_);
        if (timestampList) {
            auto lastTimestamp = timestampList.Back();
            if (lastTimestamp == (TombstoneTimestampMask | UncommittedTimestamp)) {
                YCHECK(prewrite);
                THROW_ERROR_EXCEPTION("Cannot change a deleted row");
            }
            if (!(lastTimestamp & UncommittedTimestamp)) {
                pushTimestamp = true;
            }
        } else {
            timestampList = TTimestampList::Allocate(&AlignedPool_, InitialEditListCapacity);
            dynamicRow.SetTimestampList(KeyCount_, timestampList);
            pushTimestamp = true;
        }

        if (pushTimestamp) {
            if (AllocateListIfNeeded(&timestampList, &AlignedPool_)) {
                dynamicRow.SetTimestampList(KeyCount_, timestampList);
            }
            timestampList.Push(UncommittedTimestamp);
        }
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        // Acquire the lock.
        auto dynamicRow = result = AllocateRow();
        LockRow(dynamicRow, transaction, prewrite);

        // Add timestamp.
        writeTimestamp(dynamicRow);

        // Copy keys.
        for (int id = 0; id < KeyCount_; ++id) {
            auto& internedValue = dynamicRow.GetKeys()[id];
            CopyValue(&internedValue, row[id]);
            internedValue.Id = id;
        }

        // Copy values.
        writeValues(dynamicRow);

        return dynamicRow;
    };

    auto existingKeyConsumer = [&] (TDynamicRow dynamicRow) {
        // Check for lock conflicts and acquire the lock.
        result = dynamicRow;
        LockRow(dynamicRow, transaction, prewrite);

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
    bool predelete)
{
    TDynamicRow result;

    auto writeTombstone = [&] (TDynamicRow dynamicRow) {
        auto timestampList = dynamicRow.GetTimestampList(KeyCount_);
        if (!timestampList) {
            auto timestampList = TTimestampList::Allocate(&AlignedPool_, InitialEditListCapacity);
            dynamicRow.SetTimestampList(KeyCount_, timestampList);
            timestampList.Push(UncommittedTimestamp | TombstoneTimestampMask);
            return;
        }

        auto lastTimestamp = timestampList.Back();
        if (lastTimestamp == UncommittedTimestamp) {
            YCHECK(predelete);
            THROW_ERROR_EXCEPTION("Cannot delete a changed row");
        }

        if (lastTimestamp & TombstoneTimestampMask)
            return;

        if (AllocateListIfNeeded(&timestampList, &AlignedPool_)) {
            dynamicRow.SetTimestampList(KeyCount_, timestampList);
        }

        timestampList.Push(UncommittedTimestamp | TombstoneTimestampMask);
    };

    auto newKeyProvider = [&] () -> TDynamicRow {
        // Acquire the lock.
        auto dynamicRow = result = AllocateRow();
        LockRow(dynamicRow, transaction, predelete);

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
        result = dynamicRow;
        LockRow(dynamicRow, transaction, predelete);

        // Add tombstone.
        writeTombstone(dynamicRow);
    };

    Tree_->Insert(
        key,
        newKeyProvider,
        existingKeyConsumer);

    return result;
}

std::unique_ptr<IStoreScanner> TDynamicMemoryStore::CreateScanner()
{
    return std::unique_ptr<IStoreScanner>(new TScanner(this));
}

void TDynamicMemoryStore::ConfirmRow(TDynamicRow row)
{
    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    transaction->LockedRows().push_back(TDynamicRowRef(this, row));
}

void TDynamicMemoryStore::PrepareRow(TDynamicRow row)
{
    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    row.SetPrepareTimestamp(transaction->GetPrepareTimestamp());
}

void TDynamicMemoryStore::CommitRow(TDynamicRow row)
{
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

    // Variable values.
    // TODO(babenko)

    row.SetTransaction(nullptr);
    row.SetPrepareTimestamp(MaxTimestamp);
    row.SetLastCommitTimestamp(transaction->GetCommitTimestamp());
}

void TDynamicMemoryStore::AbortRow(TDynamicRow row)
{
    // Edit timestamps.
    auto timestampList = row.GetTimestampList(KeyCount_);
    if (timestampList) {
        auto lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            if (timestampList.Pop() == 0) {
                row.SetTimestampList(KeyCount_, timestampList.GetNext());
            }
        }
    }

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount_ - KeyCount_; ++index) {
        auto list = row.GetFixedValueList(index, KeyCount_);
        if (list) {
            const auto& lastValue = list.Back();
            if (lastValue.Timestamp == UncommittedTimestamp) {
                if (list.Pop() == 0) {
                    row.SetFixedValueList(index, KeyCount_, list.GetNext());
                }
            }
        }
    }

    // Variable values.
    // TODO(babenko)

    row.SetTransaction(nullptr);
}

TDynamicRow TDynamicMemoryStore::AllocateRow()
{
    return TDynamicRow::Allocate(
        &AlignedPool_,
        KeyCount_,
        SchemaColumnCount_);
}

void TDynamicMemoryStore::LockRow(
    TDynamicRow row,
    TTransaction* transaction,
    bool preliminary)
{
    auto* existingTransaction = row.GetTransaction();
    if (existingTransaction && existingTransaction != transaction) {
        YCHECK(preliminary);
        THROW_ERROR_EXCEPTION("Row lock conflict with concurrent transaction %s",
            ~ToString(existingTransaction->GetId()));
    }

    if (row.GetLastCommitTimestamp() >= transaction->GetStartTimestamp()) {
        YCHECK(preliminary);
        THROW_ERROR_EXCEPTION("Row lock conflict with a transaction committed at %" PRIu64,
            row.GetLastCommitTimestamp());
    }

    if (!preliminary && !existingTransaction) {
        transaction->LockedRows().push_back(TDynamicRowRef(this, row));
    }

    row.SetTransaction(transaction);
}

void TDynamicMemoryStore::CopyValue(TUnversionedValue* dst, const TUnversionedValue& src)
{
    switch (src.Type) {
        case EValueType::Integer:
        case EValueType::Double:
        case EValueType::Null:
            *dst = src;
            break;

        case EValueType::String:
        case EValueType::Any:
            dst->Type = src.Type;
            dst->Length = src.Length;
            dst->Data.String = UnalignedPool_.AllocateUnaligned(src.Length);
            memcpy(const_cast<char*>(dst->Data.String), src.Data.String, src.Length);
            AllocatedStringSpace_ += src.Length;
            break;

        default:
            YUNREACHABLE();
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
