#include "stdafx.h"
#include "dynamic_memory_store.h"
#include "tablet.h"
#include "transaction.h"
#include "config.h"
#include "tablet_manager.h"

#include <core/misc/small_vector.h>

#include <core/concurrency/fiber.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/writer.h>

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
T* Fetch(
    TEditList<T> list,
    TTimestamp maxTimestamp,
    TTimestampExtractor timestampExtractor)
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

        return left && timestampExtractor(*left) <= maxTimestamp ? left : nullptr;
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
    { }


    virtual TTimestamp FindRow(NVersionedTableClient::TKey key, TTimestamp timestamp) override
    {
        if (!TreeScanner_->Find(key, &Row_)) {
            return NullTimestamp;
        }

        if (timestamp != LastCommittedTimestamp && Row_.GetPrepareTimestamp() < timestamp) {
            WaitFor(Row_.GetTransaction()->GetFinished());
        }

        MinTimestamp_ = FetchMinTimestamp(Row_.GetTimestampList(KeyCount_), timestamp);
        MaxTimestamp_ = timestamp;

        return MinTimestamp_;
    }

    virtual const TUnversionedValue& GetKey(int index) override
    {
        YASSERT(index >= 0 && index < KeyCount_);

        return Row_.GetKeys()[index];
    }

    virtual const NVersionedTableClient::TVersionedValue* GetFixedValue(int index) override
    {
        YASSERT(index >= 0 && index < SchemaValueCount_ - KeyCount_);

        auto list = Row_.GetFixedValueList(index, KeyCount_);
        return FetchVersionedValue(list, MinTimestamp_, MaxTimestamp_);
    }

private:
    TDynamicMemoryStorePtr Store_;

    int KeyCount_;
    int SchemaValueCount_;
    TRcuTreeScannerPtr<TDynamicRow, TKeyPrefixComparer> TreeScanner_;
    TDynamicRow Row_;
    TTimestamp MaxTimestamp_;
    TTimestamp MinTimestamp_;


    static TTimestamp FetchMinTimestamp(
        TTimestampList list,
        TTimestamp maxTimestamp)
    {
        auto* result = Fetch(
            list,
            maxTimestamp,
            [] (TTimestamp timestamp) {
                return timestamp & TimestampValueMask;
            });
        return result ? *result : NullTimestamp;
    }

    static const TVersionedValue* FetchVersionedValue(
        TValueList list,
        TTimestamp minTimestamp,
        TTimestamp maxTimestamp)
    {
        auto* result = Fetch(
            list,
            maxTimestamp,
            [] (const TVersionedValue& value) {
                return value.Timestamp;
            });
        return result && result->Timestamp >= minTimestamp ? result : nullptr;
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
    , WastedStringSpace_(0)
    , AllocatedValueCount_(0)
    , WastedValueCount_(0)
    , AlignedPool_(Config_->AlignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , UnalignedPool_(Config_->UnalignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , NameTable_(New<TNameTable>())
    , Comparer_(new TKeyPrefixComparer(KeyCount_))
    , Tree_(new TRcuTree<TDynamicRow, TKeyPrefixComparer>(&AlignedPool_, Comparer_.get()))
{
    for (const auto& column : Tablet_->Schema().Columns()) {
        NameTable_->RegisterName(column.Name);
    }
}

TDynamicMemoryStore::~TDynamicMemoryStore()
{ }

TDynamicRow TDynamicMemoryStore::WriteRow(
    const TNameTablePtr& nameTable,
    TTransaction* transaction,
    TVersionedRow row,
    bool prewrite)
{
    TDynamicRow result;

    auto maybeExpireFixedValue = [&] (TValueList list, int id) {
        int maxVersions = Tablet_->GetConfig()->MaxVersions;
        if (list.GetSize() + list.GetSuccessorsSize() <= maxVersions)
            return;

        ++WastedValueCount_;

        const auto& columnSchema = Tablet_->Schema().Columns()[id];
        if (columnSchema.Type != EValueType::String && columnSchema.Type != EValueType::Any)
            return;

        int expiredIndex = list.GetSize() - maxVersions - 1;
        auto currentList = list;
        while (expiredIndex < 0) {
            currentList = currentList.GetNext();
            YASSERT(currentList);
            YASSERT(currentList.GetSize() == currentList.GetCapacity());
            expiredIndex += currentList.GetSize();
        }

        const auto& expiredValue = currentList[expiredIndex];
        if (expiredValue.Type == EValueType::Null)
            return;

        YASSERT(expiredValue.Type == columnSchema.Type);
        WastedStringSpace_ += expiredValue.Length;
    };

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

        maybeExpireFixedValue(list, id);
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

void TDynamicMemoryStore::LookupRow(
    const IWriterPtr& writer,
    NVersionedTableClient::TKey key,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    TSmallVector<int, TypicalColumnCount> fixedColumnIds(SchemaColumnCount_);
    yhash_map<int, int> variableColumnIds;

    TNameTablePtr localNameTable;
    if (columnFilter.All) {
        localNameTable = NameTable_;

        for (int globalId = 0; globalId < SchemaColumnCount_; ++globalId) {
            fixedColumnIds[globalId] = globalId;
        }
    } else {
        localNameTable = New<TNameTable>();

        for (int globalId = 0; globalId < SchemaColumnCount_; ++globalId) {
            fixedColumnIds[globalId] = -1;
        }

        for (const auto& name : columnFilter.Columns) {
            auto globalId = NameTable_->FindId(name);
            if (globalId) {
                int localId = localNameTable->GetIdOrRegisterName(name);
                if (*globalId < SchemaColumnCount_) {
                    fixedColumnIds[*globalId] = localId;
                } else {
                    variableColumnIds.insert(std::make_pair(*globalId, localId));
                }
            }
        }
    }

    writer->Open(
        std::move(localNameTable),
        Tablet_->Schema(),
        Tablet_->KeyColumns());

    TRcuTreeScannerPtr<TDynamicRow, TKeyPrefixComparer> scanner(Tree_.get());

    TDynamicRow dynamicRow;
    if (scanner->Find(key, &dynamicRow)) {
        if (timestamp != LastCommittedTimestamp &&
            dynamicRow.GetPrepareTimestamp() < timestamp)
        {
            WaitFor(dynamicRow.GetTransaction()->GetFinished());
        }

        auto minTimestamp = FetchTimestamp(dynamicRow.GetTimestampList(KeyCount_), timestamp);
        if (!(minTimestamp & TombstoneTimestampMask)) {
            // Key
            for (int globalId = 0; globalId < KeyCount_; ++globalId) {
                int localId = fixedColumnIds[globalId];
                if (localId < 0)
                    continue;

                auto value = dynamicRow.GetKeys()[globalId];
                value.Id = localId;
                writer->WriteValue(value);
            }

            // Fixed values
            for (int globalId = KeyCount_; globalId < SchemaColumnCount_; ++globalId) {
                int localId = fixedColumnIds[globalId];
                if (localId < 0)
                    continue;

                auto list = dynamicRow.GetFixedValueList(globalId - KeyCount_, KeyCount_);
                const auto* value = FetchVersionedValue(list, minTimestamp, timestamp);
                if (value) {
                    auto valueCopy = *value;
                    valueCopy.Id = localId;
                    writer->WriteValue(valueCopy);
                } else {
                    writer->WriteValue(MakeSentinelValue<TVersionedValue>(EValueType::Null, localId));
                }
            }

            // Variable values
            // TODO(babenko)

            writer->EndRow();
        }
    }

    // NB: The writer must be synchronous.
    YCHECK(writer->AsyncClose().Get().IsOK());
}

std::unique_ptr<IStoreScanner> TDynamicMemoryStore::CreateScanner()
{
    return std::unique_ptr<IStoreScanner>(new TScanner(this));
}

void TDynamicMemoryStore::ConfirmRow(TDynamicRow row)
{
    auto* transaction = row.GetTransaction();
    YASSERT(transaction);
    transaction->LockedRows().push_back(TDynamicRowRef(Tablet_, row));
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
        transaction->LockedRows().push_back(TDynamicRowRef(Tablet_, row));
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

TTimestamp TDynamicMemoryStore::FetchTimestamp(
    TTimestampList list,
    TTimestamp timestamp)
{
    auto* result = Fetch(
        list,
        timestamp,
        [] (TTimestamp timestamp) {
            return timestamp & TimestampValueMask;
        });
    return result ? *result : (NullTimestamp | TombstoneTimestampMask);
}

const TVersionedValue* TDynamicMemoryStore::FetchVersionedValue(
    TValueList list,
    TTimestamp minTimestamp,
    TTimestamp maxTimestamp)
{
    auto* result = Fetch(
        list,
        maxTimestamp,
        [] (const TVersionedValue& value) {
            return value.Timestamp;
        });
    return result && result->Timestamp >= minTimestamp ? result : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
