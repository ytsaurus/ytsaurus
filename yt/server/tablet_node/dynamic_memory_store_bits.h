#pragma once

#include "public.h"

#include <core/misc/enum.h>
#include <core/misc/chunked_memory_pool.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <tuple>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicRowHeader
{
    TTransaction* Transaction;
    i32 LockIndex;
    i32 LockMode;
    TTimestamp PrepareTimestamp;
    TTimestamp LastCommitTimestamp;
    
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
        auto* header = reinterpret_cast<TEditListHeader*>(pool->AllocateAligned(
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

DECLARE_ENUM(ERowLockMode,
    (None)
    (Write)
    (Delete)
);

DECLARE_ENUM(ETimestampListKind,
    (Write)
    (Delete)
);

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
        // One list per each non-key schema column
        // plus write timestamps
        // plus delete timestamp.
        int listCount =
            schemaColumnCount -
            keyCount +
            ETimestampListKind::GetDomainSize();
        auto* header = reinterpret_cast<TDynamicRowHeader*>(pool->AllocateAligned(
            sizeof (TDynamicRowHeader) +
            keyCount * sizeof (TUnversionedValue) +
            listCount * sizeof(TEditListHeader*)));
        header->Transaction = nullptr;
        header->LockIndex = InvalidLockIndex;
        header->LockMode = ERowLockMode::None;
        header->PrepareTimestamp = NVersionedTableClient::MaxTimestamp;
        header->LastCommitTimestamp = NVersionedTableClient::NullTimestamp;
        auto* keys = reinterpret_cast<TUnversionedValue*>(header + 1);
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


    static const int InvalidLockIndex;

    int GetLockIndex() const
    {
        return Header_->LockIndex;
    }

    void SetLockIndex(int index)
    {
        Header_->LockIndex = index;
    }


    ERowLockMode GetLockMode() const
    {
        return ERowLockMode(Header_->LockMode);
    }

    void SetLockMode(ERowLockMode mode)
    {
        Header_->LockMode = mode;
    }


    void Lock(TTransaction* transaction, int index, ERowLockMode mode)
    {
        Header_->Transaction = transaction;
        Header_->LockIndex = index;
        Header_->LockMode = mode;
    }

    void Unlock()
    {
        Header_->Transaction = nullptr;
        Header_->LockIndex = InvalidLockIndex;
        Header_->LockMode = ERowLockMode::None;
        Header_->PrepareTimestamp = NTransactionClient::MaxTimestamp;
    }



    TTimestamp GetPrepareTimestamp() const
    {
        return Header_->PrepareTimestamp;
    }

    void SetPrepareTimestamp(TTimestamp timestamp) const
    {
        Header_->PrepareTimestamp = timestamp;
    }


    TTimestamp GetLastCommitTimestamp() const
    {
        return Header_->LastCommitTimestamp;
    }

    void SetLastCommitTimestamp(TTimestamp timestamp) const
    {
        Header_->LastCommitTimestamp = timestamp;
    }


    const TUnversionedValue& operator [](int id) const
    {
        return GetKeys()[id];
    }

    const TUnversionedValue* GetKeys() const
    {
        return reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(Header_ + 1);
    }

    TUnversionedValue* GetKeys()
    {
        return reinterpret_cast<TUnversionedValue*>(Header_ + 1);
    }


    TValueList GetFixedValueList(int index, int keyCount) const
    {
        return TValueList(GetLists(keyCount)[index + ETimestampListKind::GetDomainSize()]);
    }

    void SetFixedValueList(int index, TValueList list, int keyCount)
    {
        GetLists(keyCount)[index + ETimestampListKind::GetDomainSize()] = list.Header_;
    }


    TTimestampList GetTimestampList(ETimestampListKind kind, int keyCount) const
    {
        return TTimestampList(GetLists(keyCount)[static_cast<int>(kind)]);
    }

    void SetTimestampList(TTimestampList list, ETimestampListKind kind, int keyCount)
    {
        GetLists(keyCount)[static_cast<int>(kind)] = list.Header_;
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
        auto* keys = reinterpret_cast<TUnversionedValue*>(Header_ + 1);
        return reinterpret_cast<TEditListHeader**>(keys + keyCount);
    }

};

static_assert(sizeof (TDynamicRow) == sizeof (intptr_t), "TDynamicRow size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

struct TDynamicRowRef
{
    TDynamicRowRef()
        : Store(nullptr)
        , Row()
    { }

    TDynamicRowRef(const TDynamicRowRef& other) = default;
    
    TDynamicRowRef(TDynamicMemoryStore* store, TDynamicRow row)
        : Store(store)
        , Row(row)
    { }


    explicit operator bool() const
    {
        return Store != nullptr;
    }


    bool operator == (const TDynamicRowRef& other) const
    {
        return Store == other.Store && Row == other.Row;
    }

    bool operator != (const TDynamicRowRef& other) const
    {
        return !(*this == other);
    }


    TDynamicMemoryStore* Store;
    TDynamicRow Row;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
