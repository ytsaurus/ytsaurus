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

static const int InitialValueListCapacity = 2;
static const int ValueListCapacityMultiplier = 2;
static const int MaxValueListCapacity = 256;

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
    , TreePool_(Config_->TreePoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , RowPool_(Config_->RowPoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , StringPool_(Config_->StringPoolChunkSize, Config_->PoolMaxSmallBlockRatio)
    , NameTable_(New<TNameTable>())
    , Comparer_(new TComparer(KeyCount))
    , Tree_(new TRcuTree<TBucket, TComparer>(&TreePool_, Comparer_.get()))
{
    for (const auto& column : Tablet_->Schema().Columns()) {
        NameTable_->RegisterName(column.Name);
    }
}

TMemoryTable::~TMemoryTable()
{ }

void TMemoryTable::WriteRows(
    TTransaction* transaction,
    IReaderPtr reader,
    bool prewrite,
    std::vector<TBucket>* lockedBuckets)
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
    // TODO(babenko): use unversioned rows
    std::vector<TVersionedRow> rows;
    rows.reserve(RowsBufferSize);

    while (true) {
        bool hasData = reader->Read(&rows);
        for (auto row : rows) {
            auto bucket = WriteRow(nameTable, transaction, row, prewrite);
            if (lockedBuckets && bucket) {
                lockedBuckets->push_back(bucket);
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

TBucket TMemoryTable::WriteRow(
    TNameTablePtr nameTable,
    TTransaction* transaction,
    TVersionedRow row,
    bool prewrite)
{
    TBucket result;

    int valueCount = row.GetValueCount();

    auto writeFixedValue = [&] (TBucket bucket, int id) {
        const auto& srcValue = row[id];

        int listIndex = id - KeyCount;
        auto list = bucket.GetValueList(listIndex, KeyCount);

        if (!list) {
            list = TValueList::Allocate(&RowPool_, InitialValueListCapacity);
            bucket.SetValueList(listIndex, KeyCount, list);
        } else {
            auto& lastValue = list[list.GetSize() - 1];
            if (lastValue.Timestamp == MaxTimestamp) {
                InternValue(&lastValue, srcValue);
                return;
            }

            if (list.GetSize() == list.GetCapacity()) {
                int newCapacity = std::min(list.GetCapacity() * ValueListCapacityMultiplier, MaxValueListCapacity);
                auto newList = TValueList::Allocate(&RowPool_, newCapacity);
                newList.SetNext(list);
                bucket.SetValueList(listIndex, KeyCount, newList);
                list = newList;
            }
        }

        auto* dstValue = list.End();
        InternValue(dstValue, srcValue);
        dstValue->Timestamp = MaxTimestamp;
        dstValue->Id = id;

        list.SetSize(list.GetSize() + 1);
    };

    auto writeValues = [&] (TBucket bucket) {
        // Fixed values.
        for (int id = KeyCount; id < SchemaColumnCount; ++id) {
            writeFixedValue(bucket, id);
        }

        // Variable values.
        // TODO(babenko)
    };

    auto lockBucket = [&] (TBucket bucket) {
        bucket.SetTransaction(transaction);
        if (!prewrite) {
            transaction->LockedBuckets().push_back(TBucketRef(Tablet_, bucket));
        }
    };

    auto newKeyProvider = [&] () -> TBucket {
        auto bucket = TBucket::Allocate(
            &RowPool_,
            KeyCount,
            SchemaColumnCount - KeyCount + 1);

        // Acquire the lock.
        lockBucket(bucket);

        // Copy keys.
        for (int id = 0; id < KeyCount; ++id) {
            auto& internedValue = bucket.GetKey(id);
            InternValue(&internedValue, row[id]);
            internedValue.Id = id;
        }

        // Copy values.
        writeValues(bucket);

        result = bucket;
        return bucket;
    };

    auto existingKeyConsumer = [&] (TBucket bucket) {
        // Check for a lock conflict.
        auto* existingTransaction = bucket.GetTransaction();
        if (existingTransaction) {
            if (existingTransaction != transaction) {
                THROW_ERROR_EXCEPTION("Row lock conflict with transaction %s",
                    ~ToString(existingTransaction->GetId()));
            }           
        } else {
            // Acquire the lock.
            lockBucket(bucket);
        }

        writeValues(bucket);
        result = bucket;
    };

    Tree_->Insert(
        row,
        newKeyProvider,
        existingKeyConsumer);

    return result;
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
            dst->Data.String = StringPool_.AllocateUnaligned(src.Length);
            memcpy(const_cast<char*>(dst->Data.String), src.Data.String, src.Length);
            break;

        default:
            YUNREACHABLE();
    }
}

void TMemoryTable::ConfirmPrewrittenBucket(TBucket bucket)
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

    // Fixed values.
    for (int index = 0; index < SchemaColumnCount - KeyCount; ++index) {
        auto list = bucket.GetValueList(index, KeyCount);
        if (list) {
            auto& lastValue = list[list.GetSize() - 1];
            if (lastValue.Timestamp == MaxTimestamp) {
                lastValue.Timestamp = transaction->GetCommitTimestamp();
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
    // Fixed values.
    for (int index = 0; index < SchemaColumnCount - KeyCount; ++index) {
        auto list = bucket.GetValueList(index, KeyCount);
        if (list) {
            const auto& lastValue = list[list.GetSize() - 1];
            if (lastValue.Timestamp == MaxTimestamp) {
                list.SetSize(list.GetSize() - 1);
                if (list.GetSize() == 0) {
                    bucket.SetValueList(index, KeyCount, list.GetNext());
                }
            }
        }
    }
    
    // Variable values.
    // TODO(babenko)

    bucket.SetTransaction(nullptr);
}

void TMemoryTable::LookupRows(
    NVersionedTableClient::TKey key,
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

    TBucket bucket;
    if (scanner->Find(key, &bucket)) {
        bool hasValues = false;

        // Fixed values
        for (int id = KeyCount; id < SchemaColumnCount; ++id) {
            auto list = bucket.GetValueList(id - KeyCount, KeyCount);
            const auto* value = FetchVersionedValue(list, timestamp);
            if (value) {
                chunkWriter->WriteValue(*value);
                hasValues = true;
            } else {
                chunkWriter->WriteValue(TUnversionedValue::MakeSentinel(EValueType::Null, id));
            }
        }

        // Variable values
        // TODO(babenko)
        
        // Key
        if (hasValues) {
            for (int id = 0; id < KeyCount; ++id) {
                const auto& value = bucket.GetKey(id);
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

const TVersionedValue* TMemoryTable::FetchVersionedValue(
    TValueList list,
    TTimestamp timestamp)
{
    // TODO(babenko): locking
    if (timestamp == LastCommittedTimestamp) {
        return list.End() - 1;
    } else {
        while (true) {
            if (!list) {
                return nullptr;
            }
            if (list.Begin()->Timestamp <= timestamp) {
                break;
            }
            list = list.GetNext();
        }

        const auto* left = list.Begin();
        const auto* right = list.End();
        while (left != right) {
            const auto* mid = left + (right - left) / 2;
            if (mid->Timestamp <= timestamp) {
                left = mid;
            }
            else {
                right = mid;
            }
        }

        return left && left->Timestamp <= timestamp ? left : nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
