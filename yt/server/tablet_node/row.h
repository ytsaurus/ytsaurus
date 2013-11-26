#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>

#include <ytlib/new_table_client/row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TBucketHeader
{
    TTransaction* Transaction;
    NVersionedTableClient::TTimestamp PrepareTimestamp;
    // A sequence of TUnversionedValue-s (representing key values) follow.
    // A sequence of TValueBucketHeader*-s (representing versioned values, one per each schema column + 1) follow.
};

struct TValueListHeader
{
    TValueListHeader* Next;
    // TODO(babenko): smaller footprint
    ui32 Size;
    ui32 Capacity;
    // A sequence of TVersionedValue-s follow.
};

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TValueListHeader*.
class TValueList
{
public:
    FORCED_INLINE TValueList()
        : Header_(nullptr)
    { }

    FORCED_INLINE explicit TValueList(TValueListHeader* header)
        : Header_(header)
    { }

    FORCED_INLINE static TValueList Allocate(
        TChunkedMemoryPool* pool,
        int capacity)
    {
        auto* header = reinterpret_cast<TValueListHeader*>(pool->Allocate(
            sizeof (TValueListHeader)+
            capacity * sizeof(NVersionedTableClient::TVersionedValue)));
        header->Capacity = capacity;
        header->Size = 0;
        header->Next = nullptr;
        return TValueList(header);
    }


    FORCED_INLINE explicit operator bool()
    {
        return Header_ != nullptr;
    }


    FORCED_INLINE TValueList GetNext() const
    {
        return TValueList(Header_->Next);
    }

    FORCED_INLINE void SetNext(TValueList next)
    {
        Header_->Next = next.Header_;
    }


    FORCED_INLINE int GetSize() const
    {
        return Header_->Size;
    }

    FORCED_INLINE void SetSize(int size)
    {
        Header_->Size = size;
    }


    FORCED_INLINE int GetCapacity() const
    {
        return Header_->Capacity;
    }


    FORCED_INLINE const NVersionedTableClient::TVersionedValue* Begin() const
    {
        auto* values = reinterpret_cast<NVersionedTableClient::TVersionedValue*>(Header_ + 1);
        return values;
    }

    FORCED_INLINE NVersionedTableClient::TVersionedValue* Begin()
    {
        auto* values = reinterpret_cast<NVersionedTableClient::TVersionedValue*>(Header_ + 1);
        return values;
    }


    FORCED_INLINE const NVersionedTableClient::TVersionedValue* End() const
    {
        auto* values = reinterpret_cast<NVersionedTableClient::TVersionedValue*>(Header_ + 1);
        return values + Header_->Size;
    }

    FORCED_INLINE NVersionedTableClient::TVersionedValue* End()
    {
        auto* values = reinterpret_cast<NVersionedTableClient::TVersionedValue*>(Header_ + 1);
        return values + Header_->Size;
    }


    FORCED_INLINE const NVersionedTableClient::TVersionedValue& operator[] (int index) const
    {
        auto* values = reinterpret_cast<NVersionedTableClient::TVersionedValue*>(Header_ + 1);
        return values[index];
    }

    FORCED_INLINE NVersionedTableClient::TVersionedValue& operator[] (int index)
    {
        auto* values = reinterpret_cast<NVersionedTableClient::TVersionedValue*>(Header_ + 1);
        return values[index];
    }

private:
    friend class  TBucket;

    TValueListHeader* Header_;

};

static_assert(sizeof (TValueList) == sizeof (intptr_t), "TValueList size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TBucketHeader*.
class TBucket
{
public:
    FORCED_INLINE TBucket()
        : Header_(nullptr)
    { }

    FORCED_INLINE explicit TBucket(TBucketHeader* header)
        : Header_(header)
    { }

    FORCED_INLINE static TBucket Allocate(
        TChunkedMemoryPool* pool,
        int keyCount,
        int valueCount)
    {
        auto* header = reinterpret_cast<TBucketHeader*>(pool->Allocate(
            sizeof (TBucketHeader) +
            keyCount * sizeof (NVersionedTableClient::TUnversionedValue) +
            valueCount * sizeof(TValueListHeader*)));
        header->Transaction = nullptr;
        header->PrepareTimestamp = NVersionedTableClient::MaxTimestamp;
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(header + 1);
        auto** lists = reinterpret_cast<TValueListHeader**>(keys + keyCount);
        ::memset(lists, 0, sizeof (TValueListHeader*) * valueCount);
        return TBucket(header);
    }


    FORCED_INLINE explicit operator bool()
    {
        return Header_ != nullptr;
    }

    
    FORCED_INLINE TTransaction* GetTransaction() const
    {
        return Header_->Transaction;
    }

    FORCED_INLINE void SetTransaction(TTransaction* transaction)
    {
        Header_->Transaction = transaction;
    }


    FORCED_INLINE NVersionedTableClient::TTimestamp GetPrepareTimestamp() const
    {
        return Header_->PrepareTimestamp;
    }

    FORCED_INLINE void SetPrepareTimestamp(NVersionedTableClient::TTimestamp timestamp) const
    {
        Header_->PrepareTimestamp = timestamp;
    }


    FORCED_INLINE const NVersionedTableClient::TUnversionedValue& GetKey(int id) const
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        return keys[id];
    }

    FORCED_INLINE NVersionedTableClient::TUnversionedValue& GetKey(int id)
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        return keys[id];
    }


    FORCED_INLINE TValueList GetValueList(int index, int keyCount) const
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        auto** lists = reinterpret_cast<TValueListHeader**>(keys + keyCount);
        return TValueList(lists[index]);
    }

    FORCED_INLINE void SetValueList(int index, int keyCount, TValueList list)
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        auto** lists = reinterpret_cast<TValueListHeader**>(keys + keyCount);
        lists[index] = list.Header_;
    }

private:
    TBucketHeader* Header_;

};

static_assert(sizeof (TBucket) == sizeof (intptr_t), "TBucket size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

struct TBucketRef
{
    FORCED_INLINE TBucketRef()
        : Tablet(nullptr)
        , Bucket()
    { }

    FORCED_INLINE TBucketRef(TTablet* tablet, TBucket bucket)
        : Tablet(tablet)
        , Bucket(bucket)
    { }

    TTablet* Tablet;
    TBucket Bucket;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
