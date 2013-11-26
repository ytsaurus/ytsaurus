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
    
    // Variable-size part:
    // * TUnversionedValue per each key column
    // * TEditListHeader* for tombstones
    // * TEditListHeader* for variable columns
    // * TEditListHeader* per each fixed column
};

struct TEditListHeader
{
    TEditListHeader* Next;
    // TODO(babenko): smaller footprint
    ui32 Size;
    ui32 Capacity;

    // Variable-size part:
    // * |Capacity| values
};

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TEditListHeader*.
template <class T>
class TEditList
{
public:
    FORCED_INLINE TEditList()
        : Header_(nullptr)
    { }

    FORCED_INLINE explicit TEditList(TEditListHeader* header)
        : Header_(header)
    { }

    FORCED_INLINE static TEditList Allocate(
        TChunkedMemoryPool* pool,
        int capacity)
    {
        auto* header = reinterpret_cast<TEditListHeader*>(pool->Allocate(
            sizeof (TEditListHeader) +
            capacity * sizeof(T)));
        header->Capacity = capacity;
        header->Size = 0;
        header->Next = nullptr;
        return TEditList(header);
    }


    FORCED_INLINE explicit operator bool()
    {
        return Header_ != nullptr;
    }


    FORCED_INLINE TEditList GetNext() const
    {
        return TEditList(Header_->Next);
    }

    FORCED_INLINE void SetNext(TEditList next)
    {
        Header_->Next = next.Header_;
    }


    FORCED_INLINE int GetSize() const
    {
        return Header_->Size;
    }

    FORCED_INLINE int GetCapacity() const
    {
        return Header_->Capacity;
    }


    FORCED_INLINE const T* Begin() const
    {
        return reinterpret_cast<T*>(Header_ + 1);
    }

    FORCED_INLINE T* Begin()
    {
        return reinterpret_cast<T*>(Header_ + 1);
    }


    FORCED_INLINE const T* End() const
    {
        return reinterpret_cast<T*>(Header_ + 1) + Header_->Size;
    }

    FORCED_INLINE T* End()
    {
        return reinterpret_cast<T*>(Header_ + 1) + Header_->Size;
    }


    FORCED_INLINE const T& operator[] (int index) const
    {
        return Begin()[index];
    }

    FORCED_INLINE T& operator[] (int index)
    {
        return Begin()[index];
    }


    FORCED_INLINE const T& Front() const
    {
        YASSERT(GetSize() > 0);
        return *Begin();
    }

    FORCED_INLINE T& Front()
    {
        YASSERT(GetSize() > 0);
        return *Begin();
    }
    

    FORCED_INLINE const T& Back() const
    {
        YASSERT(GetSize() > 0);
        return *(End() - 1);
    }

    FORCED_INLINE T& Back()
    {
        YASSERT(GetSize() > 0);
        return *(End() - 1);
    }


    FORCED_INLINE void Push(T value)
    {
        YASSERT(Header_->Size < Header_->Capacity);
        *End() = value;
        ++Header_->Size;
    }

    template <class TCtor>
    FORCED_INLINE void Push(TCtor valueCtor)
    {
        YASSERT(Header_->Size < Header_->Capacity);
        valueCtor(End());
        ++Header_->Size;
    }

    FORCED_INLINE int Pop()
    {
        YASSERT(GetSize() > 0);
        return --Header_->Size;
    }

private:
    friend class  TBucket;

    TEditListHeader* Header_;

};

static_assert(sizeof (TValueList) == sizeof (intptr_t), "TValueList size must match that of a pointer.");
static_assert(sizeof (TTimestampList) == sizeof (intptr_t), "TTimestampList size must match that of a pointer.");

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
            valueCount * sizeof(TEditListHeader*)));
        header->Transaction = nullptr;
        header->PrepareTimestamp = NVersionedTableClient::MaxTimestamp;
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(header + 1);
        auto** lists = reinterpret_cast<TEditListHeader**>(keys + keyCount);
        ::memset(lists, 0, sizeof (TEditListHeader*) * valueCount);
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


    FORCED_INLINE TValueList GetFixedValueList(int index, int keyCount) const
    {
        return TValueList(GetLists(keyCount)[index + 2]);
    }

    FORCED_INLINE void SetFixedValueList(int index, int keyCount, TValueList list)
    {
        GetLists(keyCount)[index + 2] = list.Header_;
    }


    FORCED_INLINE TValueList GetVariableValueList(int keyCount) const
    {
        return TValueList(GetLists(keyCount)[0]);
    }

    FORCED_INLINE void SetVariableValueList(int keyCount, TValueList list)
    {
        GetLists(keyCount)[0] = list.Header_;
    }


    FORCED_INLINE TTimestampList GetTimestampList(int keyCount) const
    {
        return TTimestampList(GetLists(keyCount)[1]);
    }

    FORCED_INLINE void SetTimestampList(int keyCount, TTimestampList list)
    {
        GetLists(keyCount)[1] = list.Header_;
    }


    FORCED_INLINE bool operator == (TBucket other) const
    {
        return Header_ == other.Header_;
    }

    FORCED_INLINE bool operator != (TBucket other) const
    {
        return Header_ != other.Header_;
    }

private:
    TBucketHeader* Header_;

    FORCED_INLINE TEditListHeader** GetLists(int keyCount) const
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        return reinterpret_cast<TEditListHeader**>(keys + keyCount);
    }

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
