#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>

#include <ytlib/new_table_client/row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicRowHeader
{
    TTransaction* Transaction;
    NVersionedTableClient::TTimestamp PrepareTimestamp;
    NVersionedTableClient::TTimestamp LastCommitTimestamp;
    
    // Variable-size part:
    // * TUnversionedValue per each key column
    // * TEditListHeader* for timestamps
    // * TEditListHeader* per each fixed non-key column
};

struct TEditListHeader
{
    TEditListHeader* Next;
    ui16 Size;
    ui16 SuccessorsSize;
    ui16 Capacity;
    ui16 Padding;

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
        header->SuccessorsSize = 0;
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
        if (next.Header_) {
            Header_->SuccessorsSize = next.Header_->Size + next.Header_->SuccessorsSize;
        }
    }


    int GetSize() const
    {
        return Header_->Size;
    }

    int GetSuccessorsSize() const
    {
        return Header_->SuccessorsSize;
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
    friend class  TDynamicRow;

    TEditListHeader* Header_;

};

static_assert(sizeof (TValueList) == sizeof (intptr_t), "TValueList size must match that of a pointer.");
static_assert(sizeof (TTimestampList) == sizeof (intptr_t), "TTimestampList size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TDynamicRowHeader*.
class TDynamicRow
{
public:
    TDynamicRow()
        : Header_(nullptr)
    { }

    explicit TDynamicRow(TDynamicRowHeader* header)
        : Header_(header)
    { }

    static TDynamicRow Allocate(
        TChunkedMemoryPool* pool,
        int keyCount,
        int schemaColumnCount)
    {
        int listCount =
            // one list per each non-key schema column +
            // timestamps list
            schemaColumnCount -
            keyCount + 1;
        auto* header = reinterpret_cast<TDynamicRowHeader*>(pool->Allocate(
            sizeof (TDynamicRowHeader) +
            keyCount * sizeof (NVersionedTableClient::TUnversionedValue) +
            listCount * sizeof(TEditListHeader*)));
        header->Transaction = nullptr;
        header->PrepareTimestamp = NVersionedTableClient::MaxTimestamp;
        header->LastCommitTimestamp = NVersionedTableClient::NullTimestamp;
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(header + 1);
        auto** lists = reinterpret_cast<TEditListHeader**>(keys + keyCount);
        ::memset(lists, 0, sizeof (TEditListHeader*) * listCount);
        return TDynamicRow(header);
    }


    explicit operator bool() const
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


    const NVersionedTableClient::TUnversionedValue& operator [](int id) const
    {
        return GetKeys()[id];
    }

    const NVersionedTableClient::TUnversionedValue* GetKeys() const
    {
        return reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
    }

    NVersionedTableClient::TUnversionedValue* GetKeys()
    {
        return reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
    }


    TValueList GetFixedValueList(int index, int keyCount) const
    {
        return TValueList(GetLists(keyCount)[index + 1]);
    }

    void SetFixedValueList(int index, int keyCount, TValueList list)
    {
        GetLists(keyCount)[index + 1] = list.Header_;
    }


    TTimestampList GetTimestampList(int keyCount) const
    {
        return TTimestampList(GetLists(keyCount)[0]);
    }

    void SetTimestampList(int keyCount, TTimestampList list)
    {
        GetLists(keyCount)[0] = list.Header_;
    }


    bool operator == (TDynamicRow other) const
    {
        return Header_ == other.Header_;
    }

    bool operator != (TDynamicRow other) const
    {
        return Header_ != other.Header_;
    }

private:
    TDynamicRowHeader* Header_;

    TEditListHeader** GetLists(int keyCount) const
    {
        auto* keys = reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
        return reinterpret_cast<TEditListHeader**>(keys + keyCount);
    }

};

static_assert(sizeof (TDynamicRow) == sizeof (intptr_t), "TRow size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

struct TDynamicRowRef
{
    TDynamicRowRef();
    TDynamicRowRef(const TDynamicRowRef& other);
    TDynamicRowRef(TDynamicRowRef&& other);
    TDynamicRowRef(TDynamicMemoryStorePtr store, TDynamicRow row);

    ~TDynamicRowRef();

    friend void swap(TDynamicRowRef& lhs, TDynamicRowRef& rhs);
    TDynamicRowRef& operator = (TDynamicRowRef other);


    TDynamicMemoryStorePtr Store;
    TDynamicRow Row;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
