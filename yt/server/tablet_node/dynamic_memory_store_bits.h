#pragma once

#include "public.h"

#include <core/misc/enum.h>
#include <core/misc/chunked_memory_pool.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// NB: 4-aligned.
struct TDynamicString
{
    ui32 Length;
    char Data[1]; // the actual length is above
};

// NB: TDynamicValueData must be binary compatible with TUnversionedValueData for all simple types.
union TDynamicValueData
{
    //! |Int64| value.
    i64 Int64;
    //! |Uint64| value.
    ui64 Uint64;
    //! |Double| value.
    double Double;
    //! |Boolean| value.
    bool Boolean;
    //! String value for |String| type or YSON-encoded value for |Any| type.
    TDynamicString* String;
};

static_assert(
    sizeof(TDynamicValueData) == sizeof(NVersionedTableClient::TUnversionedValueData),
    "TDynamicValueData and TUnversionedValueData must be of the same size.");

struct TLockDescriptor
{
    static const int InvalidRowIndex = -1;

    TTransaction* Transaction;
    ui32 RowIndex; // index in TTransaction::LockedRows
    TTimestamp PrepareTimestamp;
    TTimestamp LastCommitTimestamp;
};

struct TDynamicRowHeader
{
    ui32 NullKeyMask;
    ui32 DeleteLockFlag : 1;
    ui32 Padding : 31;

    // Variable-size part:
    // * TDynamicValueData per each key column
    // * TLockDescriptor per each lock group
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
            sizeof(TEditListHeader) +
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

    T* BeginPush()
    {
        YASSERT(Header_->Size < Header_->Capacity);
        return End();
    }

    void EndPush()
    {
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

static_assert(
    sizeof(TValueList) == sizeof(intptr_t),
    "TValueList size must match that of a pointer.");
static_assert(
    sizeof(TTimestampList) == sizeof(intptr_t),
    "TTimestampList size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

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
        int keyColumnCount,
        int columnLockCount,
        int schemaColumnCount)
    {
        // One list per each non-key schema column
        // plus write timestamps
        // plus delete timestamp.
        int listCount =
            (schemaColumnCount - keyColumnCount) +
            ETimestampListKind::GetDomainSize();
        size_t size =
            sizeof(TDynamicRowHeader) +
            keyColumnCount * sizeof(TDynamicValueData) +
            columnLockCount * sizeof(TLockDescriptor) +
            listCount * sizeof(TEditListHeader*);

        // Allocate memory.
        auto* header = reinterpret_cast<TDynamicRowHeader*>(pool->AllocateAligned(size));
        auto row = TDynamicRow(header);

        // Generic fill.
        ::memset(header, 0, size);

        // Custom fill.
        {
            auto* lock = row.BeginLocks(keyColumnCount);
            for (int index = 0; index < columnLockCount; ++index, ++lock) {
                lock->RowIndex = TLockDescriptor::InvalidRowIndex;
                lock->PrepareTimestamp = NVersionedTableClient::MaxTimestamp;
                lock->LastCommitTimestamp = NVersionedTableClient::MinTimestamp;
            }
        }

        return row;
    }


    explicit operator bool() const
    {
        return Header_ != nullptr;
    }

    static const int PrimaryLockIndex = 0;
    static const ui32 PrimaryLockMask = (1 << PrimaryLockIndex);
    static const ui32 AllLocksMask = 0xffffffff;

    const TDynamicValueData* BeginKeys() const
    {
        return reinterpret_cast<const TDynamicValueData*>(Header_ + 1);
    }

    TDynamicValueData* BeginKeys()
    {
        return reinterpret_cast<TDynamicValueData*>(Header_ + 1);
    }


    ui32 GetNullKeyMask() const
    {
        return Header_->NullKeyMask;
    }

    void SetNullKeyMask(ui32 value)
    {
        Header_->NullKeyMask = value;
    }


    bool GetDeleteLockFlag() const
    {
        return Header_->DeleteLockFlag;
    }

    void SetDeleteLockFlag(bool value)
    {
        Header_->DeleteLockFlag = value;
    }


    const TLockDescriptor* BeginLocks(int keyColumnCount) const
    {
        return reinterpret_cast<const TLockDescriptor*>(BeginKeys() + keyColumnCount);
    }

    TLockDescriptor* BeginLocks(int keyColumnCount)
    {
        return reinterpret_cast<TLockDescriptor*>(BeginKeys() + keyColumnCount);
    }


    TValueList GetFixedValueList(int columnIndex, int keyColumnCount, int columnLockCount) const
    {
        YASSERT(columnIndex >= keyColumnCount);
        return TValueList(GetLists(keyColumnCount, columnLockCount)[columnIndex - keyColumnCount + ETimestampListKind::GetDomainSize()]);
    }

    void SetFixedValueList(int columnIndex, TValueList list, int keyColumnCount, int columnLockCount)
    {
        YASSERT(columnIndex >= keyColumnCount);
        GetLists(keyColumnCount, columnLockCount)[columnIndex  - keyColumnCount + ETimestampListKind::GetDomainSize()] = list.Header_;
    }


    TTimestampList GetTimestampList(ETimestampListKind kind, int keyColumnCount, int columnLockCount) const
    {
        return TTimestampList(GetLists(keyColumnCount, columnLockCount)[static_cast<int>(kind)]);
    }

    void SetTimestampList(TTimestampList list, ETimestampListKind kind, int keyColumnCount, int columnLockCount)
    {
        GetLists(keyColumnCount, columnLockCount)[static_cast<int>(kind)] = list.Header_;
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


    TEditListHeader** GetLists(int keyColumnCount, int columnLockCount) const
    {
        return reinterpret_cast<TEditListHeader**>(const_cast<TLockDescriptor*>(BeginLocks(keyColumnCount)) + columnLockCount);
    }

};

static_assert(
    sizeof(TDynamicRow) == sizeof(intptr_t),
    "TDynamicRow size must match that of a pointer.");

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
        return
            Store == other.Store &&
            Row == other.Row;
    }

    bool operator != (const TDynamicRowRef& other) const
    {
        return !(*this == other);
    }


    TDynamicMemoryStore* Store;
    TDynamicRow Row;
};

////////////////////////////////////////////////////////////////////////////////

/*
 * Row comparer can work with data rows and data keys.
 * Both of the latter are internally represented by TUnversionedRow.
 * However, the comparison semantics is different: data rows must contain
 * |keyColumnCount| key components at the very beginning (and the rest
 * is value components, which must be ignored) while data keys may be
 * of arbitrary size.
 *
 * To discriminate between data rows and data keys, we provide a pair of
 * wrappers on top of TUnversionedRow.
 */

struct TRowWrapper
{
    TUnversionedRow Row;
};

struct TKeyWrapper
{
    TUnversionedRow Row;
};

//! Provides a comparer functor for dynamic row keys.
class TDynamicRowKeyComparer
{
public:
    TDynamicRowKeyComparer(int keyColumnCount, const TTableSchema& schema);

    int operator()(TDynamicRow lhs, TDynamicRow rhs) const;
    int operator()(TDynamicRow lhs, TRowWrapper rhs) const;
    int operator()(TDynamicRow lhs, TKeyWrapper rhs) const;

private:
    int KeyColumnCount_;
    const TTableSchema& Schema_;

    int Compare(
        TDynamicRow lhs,
        TUnversionedValue* rhsBegin,
        int rhsLength) const;

};

////////////////////////////////////////////////////////////////////////////////

void SaveRowKeys(
    TSaveContext& context,
    TDynamicRow row,
    TTablet* tablet);

void LoadRowKeys(
    TLoadContext& context,
    TDynamicRow row,
    TTablet* tablet,
    TChunkedMemoryPool* alignedPool);

void LoadRowKeys(
    TLoadContext& context,
    NVersionedTableClient::TUnversionedRowBuilder* builder,
    TTablet* tablet,
    TChunkedMemoryPool* unalignedPool);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
