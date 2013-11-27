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
    NVersionedTableClient::TTimestamp LastCommitTimestamp;
    
    // Variable-size part:
    // * TUnversionedValue per each key column
    // * TEditListHeader* for variable columns
    // * TEditListHeader* for timestamps
    // * TEditListHeader* per each fixed column
};

struct TEditListHeader
{
    TEditListHeader* Next;
    ui32 Size;
    ui32 Capacity;

    // Variable-size part:
    // * |Capacity| TVersionedValue-s
};

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TEditListHeader*.
template <class T>
class TEditList
{
public:
    TEditList()
        : Header_(nullptr)
    { }

    explicit TEditList(TEditListHeader* header)
        : Header_(header)
    { }

    static TEditList Allocate(
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


    explicit operator bool()
    {
        return Header_ != nullptr;
    }


    TEditList GetNext() const
    {
        return TEditList(Header_->Next);
    }

    void SetNext(TEditList next)
    {
        Header_->Next = next.Header_;
    }


    int GetSize() const
    {
        return Header_->Size;
    }

    int GetCapacity() const
    {
        return Header_->Capacity;
    }


    const T* Begin() const
    {
        return reinterpret_cast<T*>(Header_ + 1);
    }

    T* Begin()
    {
        return reinterpret_cast<T*>(Header_ + 1);
    }


    const T* End() const
    {
        return reinterpret_cast<T*>(Header_ + 1) + Header_->Size;
    }

    T* End()
    {
        return reinterpret_cast<T*>(Header_ + 1) + Header_->Size;
    }


    const T& operator[] (int index) const
    {
        return Begin()[index];
    }

    T& operator[] (int index)
    {
        return Begin()[index];
    }


    const T& Front() const
    {
        YASSERT(GetSize() > 0);
        return *Begin();
    }

    T& Front()
    {
        YASSERT(GetSize() > 0);
        return *Begin();
    }
    

    const T& Back() const
    {
        YASSERT(GetSize() > 0);
        return *(End() - 1);
    }

    T& Back()
    {
        YASSERT(GetSize() > 0);
        return *(End() - 1);
    }


    void Push(T value)
    {
        YASSERT(Header_->Size < Header_->Capacity);
        *End() = value;
        ++Header_->Size;
    }

    template <class TCtor>
    void Push(TCtor valueCtor)
    {
        YASSERT(Header_->Size < Header_->Capacity);
        valueCtor(End());
        ++Header_->Size;
    }

    int Pop()
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
    TBucket()
        : Header_(nullptr)
    { }

    explicit TBucket(TBucketHeader* header)
        : Header_(header)
    { }

    static TBucket Allocate(
        TChunkedMemoryPool* pool,
        int keyCount,
        int listCount)
    {
        auto* header = reinterpret_cast<TBucketHeader*>(pool->Allocate(
            sizeof (TBucketHeader) +
            keyCount * sizeof (NVersionedTableClient::TUnversionedValue) +
            listCount * sizeof(TEditListHeader*)));
        header->Transaction = nullptr;
        header->PrepareTimestamp = NVersionedTableClient::MaxTimestamp;
        header->LastCommitTimestamp = NVersionedTableClient::NullTimestamp;
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(header + 1);
        auto** lists = reinterpret_cast<TEditListHeader**>(keys + keyCount);
        ::memset(lists, 0, sizeof (TEditListHeader*) * listCount);
        return TBucket(header);
    }


    explicit operator bool()
    {
        return Header_ != nullptr;
    }

    
    TTransaction* GetTransaction() const
    {
        return Header_->Transaction;
    }

    void SetTransaction(TTransaction* transaction)
    {
        Header_->Transaction = transaction;
    }


    NVersionedTableClient::TTimestamp GetPrepareTimestamp() const
    {
        return Header_->PrepareTimestamp;
    }

    void SetPrepareTimestamp(NVersionedTableClient::TTimestamp timestamp) const
    {
        Header_->PrepareTimestamp = timestamp;
    }


    NVersionedTableClient::TTimestamp GetLastCommitTimestamp() const
    {
        return Header_->LastCommitTimestamp;
    }

    void SetLastCommitTimestamp(NVersionedTableClient::TTimestamp timestamp) const
    {
        Header_->LastCommitTimestamp = timestamp;
    }


    const NVersionedTableClient::TUnversionedValue& GetKey(int id) const
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        return keys[id];
    }

    NVersionedTableClient::TUnversionedValue& GetKey(int id)
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        return keys[id];
    }


    TValueList GetFixedValueList(int index, int keyCount) const
    {
        return TValueList(GetLists(keyCount)[index + 2]);
    }

    void SetFixedValueList(int index, int keyCount, TValueList list)
    {
        GetLists(keyCount)[index + 2] = list.Header_;
    }


    TValueList GetVariableValueList(int keyCount) const
    {
        return TValueList(GetLists(keyCount)[0]);
    }

    void SetVariableValueList(int keyCount, TValueList list)
    {
        GetLists(keyCount)[0] = list.Header_;
    }


    TTimestampList GetTimestampList(int keyCount) const
    {
        return TTimestampList(GetLists(keyCount)[1]);
    }

    void SetTimestampList(int keyCount, TTimestampList list)
    {
        GetLists(keyCount)[1] = list.Header_;
    }


    bool operator == (TBucket other) const
    {
        return Header_ == other.Header_;
    }

    bool operator != (TBucket other) const
    {
        return Header_ != other.Header_;
    }

private:
    TBucketHeader* Header_;

    TEditListHeader** GetLists(int keyCount) const
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        return reinterpret_cast<TEditListHeader**>(keys + keyCount);
    }

};

static_assert(sizeof (TBucket) == sizeof (intptr_t), "TBucket size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

struct TBucketRef
{
    TBucketRef()
        : Tablet(nullptr)
        , Bucket()
    { }

    TBucketRef(TTablet* tablet, TBucket bucket)
        : Tablet(tablet)
        , Bucket(bucket)
    { }

    TTablet* Tablet;
    TBucket Bucket;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
