#include "stdafx.h"
#include "memory_table.h"
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

using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NConcurrency;
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

    // TODO(babenko): locking
    if (maxTimestamp == LastCommittedTimestamp) {
        auto& value = list.Back();
        if (!(timestampExtractor(value) & UncommittedTimestamp)) {
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
        // TODO(babenko): fix this
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

class TMemoryTable::TComparer
{
public:
    explicit TComparer(int keyColumnCount)
        : KeyColumnCount_(keyColumnCount)
    { }

    int operator () (NVersionedTableClient::TKey lhs, TBucket rhs) const
    {
        YASSERT(lhs.GetValueCount() >= KeyColumnCount_);
        for (int index = 0; index < KeyColumnCount_; ++index) {
            int result = CompareRowValues(lhs[index], rhs.GetKey(index));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    // TODO(babenko): eliminate this
    int operator () (NVersionedTableClient::TVersionedRow lhs, TBucket rhs) const
    {
        YASSERT(lhs.GetValueCount() >= KeyColumnCount_);
        for (int index = 0; index < KeyColumnCount_; ++index) {
            int result = CompareRowValues(lhs[index], rhs.GetKey(index));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

private:
    int KeyColumnCount_;

};

////////////////////////////////////////////////////////////////////////////////

TMemoryTable::TMemoryTable(
    TTabletManagerConfigPtr config,
    TTablet* tablet)
    : Config_(config)
    , Tablet_(tablet)
    , KeyCount(static_cast<int>(Tablet_->KeyColumns().size()))
    , SchemaColumnCount(static_cast<int>(Tablet_->Schema().Columns().size()))
    , AlignedPool_(Config_->AlignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , UnalignedPool_(Config_->UnalignedPoolChunkSize, Config_->MaxPoolSmallBlockRatio)
    , NameTable_(New<TNameTable>())
    , Comparer_(new TComparer(KeyCount))
    , Tree_(new TRcuTree<TBucket, TComparer>(&AlignedPool_, Comparer_.get()))
{
    for (const auto& column : Tablet_->Schema().Columns()) {
        NameTable_->RegisterName(column.Name);
    }
}

TMemoryTable::~TMemoryTable()
{ }

TBucket TMemoryTable::WriteRow(
    const TNameTablePtr& nameTable,
    TTransaction* transaction,
    TVersionedRow row,
    bool prewrite)
{
    TBucket result;

    auto writeFixedValue = [&] (TBucket bucket, int id) {
        const auto& srcValue = row[id];

        int listIndex = id - KeyCount;
        auto list = bucket.GetFixedValueList(listIndex, KeyCount);

        if (!list) {
            list = TValueList::Allocate(&AlignedPool_, InitialEditListCapacity);
            bucket.SetFixedValueList(listIndex, KeyCount, list);
        } else {
            auto& lastValue = list.Back();
            if (lastValue.Timestamp == UncommittedTimestamp) {
                InternValue(&lastValue, srcValue);
                return;
            }

            if (AllocateListIfNeeded(&list, &AlignedPool_)) {
                bucket.SetFixedValueList(listIndex, KeyCount, list);
            }
        }

        list.Push([&] (TVersionedValue* dstValue) {
            InternValue(dstValue, srcValue);
            dstValue->Timestamp = UncommittedTimestamp;
            dstValue->Id = id;
        });
    };

    auto writeValues = [&] (TBucket bucket) {
        // Fixed values.
        for (int id = KeyCount; id < SchemaColumnCount; ++id) {
            writeFixedValue(bucket, id);
        }

        // Variable values.
        // TODO(babenko)
    };

    auto newKeyProvider = [&] () -> TBucket {
        // Acquire the lock.
        auto bucket = result = AllocateBucket();
        LockBucket(bucket, transaction, prewrite);

        // Copy keys.
        for (int id = 0; id < KeyCount; ++id) {
            auto& internedValue = bucket.GetKey(id);
            InternValue(&internedValue, row[id]);
            internedValue.Id = id;
        }

        // Copy values.
        writeValues(bucket);

        return bucket;
    };

    auto existingKeyConsumer = [&] (TBucket bucket) {
        // Check for lock conflicts and acquire the lock.
        result = bucket;
        LockBucket(bucket, transaction, prewrite);

        // Add write timestamp, if needed.
        auto timestampList = bucket.GetTimestampList(KeyCount);
        if (timestampList) {
            auto& lastTimestamp = timestampList.Back();
            if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
                // Ensure that there's no tombstone here.
                lastTimestamp = UncommittedTimestamp;
            } else {
                // Check if write timestamp is needed.
                if (lastTimestamp & TombstoneTimestampMask) {
                    if (AllocateListIfNeeded(&timestampList, &AlignedPool_)) {
                        bucket.SetTimestampList(KeyCount, timestampList);
                    }
                    timestampList.Push(UncommittedTimestamp);
                }
            }
        }

        // Copy values.
        writeValues(bucket);
    };

    Tree_->Insert(
        row,
        newKeyProvider,
        existingKeyConsumer);

    return result;
}

TBucket TMemoryTable::DeleteRow(
    TTransaction* transaction,
    NVersionedTableClient::TKey key,
    bool predelete)
{
    TBucket result;

    auto writeTombstone = [&] (TBucket bucket) {
        auto timestampList = bucket.GetTimestampList(KeyCount);
        if (!timestampList) {
            auto timestampList = TTimestampList::Allocate(&AlignedPool_, InitialEditListCapacity);
            bucket.SetTimestampList(KeyCount, timestampList);
            timestampList.Push(UncommittedTimestamp | TombstoneTimestampMask);
            return;
        }

        auto lastTimestamp = timestampList.Back();
        if (lastTimestamp & TombstoneTimestampMask)
            return;

        if (AllocateListIfNeeded(&timestampList, &AlignedPool_)) {
            bucket.SetTimestampList(KeyCount, timestampList);
        }

        timestampList.Push(UncommittedTimestamp | TombstoneTimestampMask);
    };

    auto newKeyProvider = [&] () -> TBucket {
        // Acquire the lock.
        auto bucket = result = AllocateBucket();
        LockBucket(bucket, transaction, predelete);

        // Add tombstone.
        writeTombstone(bucket);

        result = bucket;
        return bucket;
    };

    auto existingKeyConsumer = [&] (TBucket bucket) {
        // Check for lock conflicts and acquire the lock.
        result = bucket;
        LockBucket(bucket, transaction, predelete);

        // Add tombstone.
        writeTombstone(bucket);
    };

    Tree_->Insert(
        key,
        newKeyProvider,
        existingKeyConsumer);

    return result;
}

void TMemoryTable::LookupRow(
    const IWriterPtr& writer,
    NVersionedTableClient::TKey key,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    TSmallVector<int, TypicalColumnCount> fixedColumnIds(SchemaColumnCount);
    yhash_map<int, int> variableColumnIds;

    TNameTablePtr localNameTable;
    if (columnFilter.All) {
        localNameTable = NameTable_;

        for (int globalId = 0; globalId < SchemaColumnCount; ++globalId) {
            fixedColumnIds[globalId] = globalId;
        }
    } else {
        localNameTable = New<TNameTable>();

        for (int globalId = 0; globalId < SchemaColumnCount; ++globalId) {
            fixedColumnIds[globalId] = -1;
        }

        for (const auto& name : columnFilter.Columns) {
            auto globalId = NameTable_->FindId(name);
            if (globalId) {
                int localId = localNameTable->GetOrRegisterName(name);
                if (*globalId < SchemaColumnCount) {
                    fixedColumnIds[*globalId] = localId;
                } else {
                    variableColumnIds.insert(std::make_pair(*globalId, localId));
                }
            }
        }
    }

    writer->Open(
        std::move(localNameTable),
        TTableSchema(),
        TKeyColumns());

    TRcuTreeScannerGuard<TBucket, TComparer> scanner(Tree_.get());

    TBucket bucket;
    if (scanner->Find(key, &bucket)) {
        auto minTimestamp = FetchTimestamp(bucket.GetTimestampList(KeyCount), timestamp);
        if (!(minTimestamp & TombstoneTimestampMask)) {
            // Key
            for (int globalId = 0; globalId < KeyCount; ++globalId) {
                int localId = fixedColumnIds[globalId];
                if (localId < 0)
                    continue;

                auto value = bucket.GetKey(globalId);
                value.Id = localId;
                writer->WriteValue(value);
            }

            // Fixed values
            for (int globalId = KeyCount; globalId < SchemaColumnCount; ++globalId) {
                int localId = fixedColumnIds[globalId];
                if (localId < 0)
                    continue;

                auto list = bucket.GetFixedValueList(globalId - KeyCount, KeyCount);
                const auto* value = FetchVersionedValue(list, minTimestamp, timestamp);
                if (value) {
                    auto valueCopy = *value;
                    valueCopy.Id = localId;
                    writer->WriteValue(valueCopy);
                } else {
                    writer->WriteValue(TUnversionedValue::MakeSentinel(EValueType::Null, localId));
                }
            }

            // Variable values
            // TODO(babenko)

            writer->EndRow();
        }
    }

    {
        // The writer is typically synchronous.
        auto error = WaitFor(writer->AsyncClose());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }
}

void TMemoryTable::ConfirmBucket(TBucket bucket)
{
    auto* transaction = bucket.GetTransaction();
    YASSERT(transaction);
    transaction->LockedBuckets().push_back(TBucketRef(Tablet_, bucket));
}

void TMemoryTable::PrepareBucket(TBucket bucket)
{
    auto* transaction = bucket.GetTransaction();
    YASSERT(transaction);
    bucket.SetPrepareTimestamp(transaction->GetPrepareTimestamp());
}

void TMemoryTable::CommitBucket(TBucket bucket)
{
    auto* transaction = bucket.GetTransaction();
    YASSERT(transaction);
    auto commitTimestamp = transaction->GetCommitTimestamp();

    // Edit timestamps.
    auto timestampList = bucket.GetTimestampList(KeyCount);
    if (timestampList) {
        auto& lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            lastTimestamp = (lastTimestamp & ~TimestampValueMask) | commitTimestamp;
        }
    }

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount - KeyCount; ++index) {
        auto list = bucket.GetFixedValueList(index, KeyCount);
        if (list) {
            auto& lastValue = list.Back();
            if (lastValue.Timestamp == UncommittedTimestamp) {
                lastValue.Timestamp = commitTimestamp;
            }
        }
    }

    // Variable values.
    // TODO(babenko)

    bucket.SetTransaction(nullptr);
    bucket.SetPrepareTimestamp(MaxTimestamp);
}

void TMemoryTable::AbortBucket(TBucket bucket)
{
    // Edit timestamps.
    auto timestampList = bucket.GetTimestampList(KeyCount);
    if (timestampList) {
        auto lastTimestamp = timestampList.Back();
        if ((lastTimestamp & TimestampValueMask) == UncommittedTimestamp) {
            if (timestampList.Pop() == 0) {
                bucket.SetTimestampList(KeyCount, timestampList.GetNext());
            }
        }
    }

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount - KeyCount; ++index) {
        auto list = bucket.GetFixedValueList(index, KeyCount);
        if (list) {
            const auto& lastValue = list.Back();
            if (lastValue.Timestamp == UncommittedTimestamp) {
                if (list.Pop() == 0) {
                    bucket.SetFixedValueList(index, KeyCount, list.GetNext());
                }
            }
        }
    }

    // Variable values.
    // TODO(babenko)

    bucket.SetTransaction(nullptr);
}

TBucket TMemoryTable::AllocateBucket()
{
    return TBucket::Allocate(
        &AlignedPool_,
        KeyCount,
        // one slot per each non-key schema column +
        // variable values slot +
        // timestamp slot
        SchemaColumnCount - KeyCount + 2);
}

void TMemoryTable::LockBucket(
    TBucket bucket,
    TTransaction* transaction,
    bool preliminary)
{
    auto* existingTransaction = bucket.GetTransaction();
    if (existingTransaction && existingTransaction != transaction) {
        YCHECK(preliminary);
        THROW_ERROR_EXCEPTION("Row lock conflict with transaction %s",
            ~ToString(existingTransaction->GetId()));
    }

    if (!preliminary && !existingTransaction) {
        transaction->LockedBuckets().push_back(TBucketRef(Tablet_, bucket));
    }

    bucket.SetTransaction(transaction);
}

void TMemoryTable::InternValue(TUnversionedValue* dst, const TUnversionedValue& src)
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
        break;

    default:
        YUNREACHABLE();
    }
}

TTimestamp TMemoryTable::FetchTimestamp(
    TTimestampList list,
    TTimestamp timestamp)
{
    auto* result = Fetch(
        list,
        timestamp,
        [] (TTimestamp timestamp) {
            return timestamp & TimestampValueMask;
        });
    return result ? *result : NullTimestamp;
}

const TVersionedValue* TMemoryTable::FetchVersionedValue(
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
