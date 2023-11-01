#pragma once

#include "private.h"

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/tablet_client/dynamic_value.h>

#include <yt/yt/core/misc/ring_queue.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <atomic>

namespace NYT::NTabletNode {

/////////////////////////////////////////////////////////////////////////////

using NTableClient::TLockMask;

using NTabletClient::TDynamicString;
using NTabletClient::TDynamicValueData;
using NTabletClient::TDynamicValue;

struct TEditListHeader;
template <class T>
class TEditList;
using TValueList = TEditList<TDynamicValue>;
using TRevisionList = TEditList<ui32>;

////////////////////////////////////////////////////////////////////////////////

struct TLockDescriptor
{
    // Each transaction can take read lock only once.
    int ReadLockCount;

    // The latest commit timestamp of a transaction that was holding this read lock.
    TTimestamp LastReadLockTimestamp;

    TTransaction* WriteTransaction;
    std::atomic<TTimestamp> PrepareTimestamp;
    std::atomic<TEditListHeader*> WriteRevisionList;

    // Edit list of revision of committed transactions that were holding this read lock.
    // Actually only maximum timestamp in this list is important for conflicts check, edit list
    // is used here only to deal with asynchronous snapshot serialization. In particular,
    // only maximum value in this list is stored into snapshot and other values are lost
    // during reserialization.
    // NB: Timestamps in this list are not monotone since transactions can be committed in
    // arbitrary order.
    std::atomic<TEditListHeader*> ReadLockRevisionList;

    std::atomic<TEditListHeader*> ExclusiveLockRevisionList;
};

struct TSortedDynamicRowHeader
{
    ui32 NullKeyMask;
    ui32 DeleteLockFlag : 1;
    ui32 Padding : 31;
    size_t DataWeight;
};

struct TEditListHeader
{
    //! Pointer to the successor list with smaller timestamps.
    std::atomic<TEditListHeader*> Successor;

    //! Number of committed slots in the list.
    //! Only updated _after_ the slot is written to.
    std::atomic<ui16> Size;

    //! Sum of sizes of all successors.
    ui16 SuccessorsSize;

    //! Number of slots in the list.
    ui16 Capacity;

    // Variable-size part:
    // * |Capacity| slots, with increasing timestamps.
};

static_assert(
    sizeof(std::atomic<TEditListHeader*>) == sizeof(intptr_t),
    "std::atomic<TEditListHeader*> does not seem to be lock-free.");

static_assert(
    sizeof(std::atomic<ui16>) == sizeof(ui16),
    "std::atomic<ui16> does not seem to be lock-free.");

////////////////////////////////////////////////////////////////////////////////

inline size_t GetDataWeight(EValueType type, bool isNull, const TDynamicValueData& data)
{
    if (isNull) {
        return GetDataWeight(EValueType::Null);
    } else if (IsStringLikeType(type)) {
        return data.String->Length;
    } else {
        return GetDataWeight(type);
    }
}

inline size_t GetDataWeight(EValueType type, const TDynamicValue& value)
{
    return GetDataWeight(type, value.Null, value.Data);
}

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
        ::memset(header, 0, sizeof(TEditListHeader));
        header->Capacity = capacity;
        return TEditList(header);
    }


    explicit operator bool() const
    {
        return Header_ != nullptr;
    }

    bool operator == (TEditList<T> other) const
    {
        return Header_ == other.Header_;
    }

    bool operator != (TEditList<T> other) const
    {
        return !(*this == other);
    }


    TEditList GetSuccessor() const
    {
        return TEditList(Header_->Successor);
    }

    void SetSuccessor(TEditList successor)
    {
        YT_ASSERT(successor);
        Header_->Successor = successor.Header_;
        Header_->SuccessorsSize = successor.GetFullSize();
    }


    int GetSize() const
    {
        return Header_->Size;
    }

    bool IsEmpty() const
    {
        return GetSize() == 0;
    }

    int GetSuccessorsSize() const
    {
        return Header_->SuccessorsSize;
    }

    int GetFullSize() const
    {
        return GetSize() + GetSuccessorsSize();
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


    const T& Front() const
    {
        return (*this)[0];
    }

    const T& Back() const
    {
        YT_ASSERT(!IsEmpty());
        return (*this)[GetSize() - 1];
    }


    const T& operator[] (int index) const
    {
        return Begin()[index];
    }

    T& operator[] (int index)
    {
        return Begin()[index];
    }

    void Push(T value)
    {
        YT_ASSERT(Header_->Size < Header_->Capacity);
        *End() = value;
        ++Header_->Size;
    }

private:
    friend class TSortedDynamicRow;

    TEditListHeader* Header_;
};

static_assert(
    sizeof(TValueList) == sizeof(intptr_t),
    "TValueList size must match that of a pointer.");
static_assert(
    sizeof(TRevisionList) == sizeof(intptr_t),
    "TRevisionList size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

//! A row within TSortedDynamicStore.
/*!
 *  A lightweight wrapper around |TSortedDynamicRowHeader*|.
 *
 *  Provides access to the following parts:
 *  1) keys
 *  2) locks
 *  3) edit lists for write and delete timestamps
 *  4) edit lists for versioned values per each fixed non-key column
 *
 *  Memory layout:
 *  1) TSortedDynamicRowHeader
 *  2) TDynamicValueData per each key column
 *  3) TLockDescriptor per each lock group
 *  4) TEditListHeader* for delete timestamps
 *  5) TEditListHeader* per each fixed non-key column
 */
class TSortedDynamicRow
{
public:
    TSortedDynamicRow()
        : Header_(nullptr)
    { }

    explicit TSortedDynamicRow(TSortedDynamicRowHeader* header)
        : Header_(header)
    { }

    static TSortedDynamicRow Allocate(
        TChunkedMemoryPool* pool,
        int keyColumnCount,
        int columnLockCount,
        int schemaColumnCount)
    {
        // One list per each non-key schema column plus delete timestamp.
        int listCount = (schemaColumnCount - keyColumnCount) + 1;
        size_t size =
            sizeof(TSortedDynamicRowHeader) +
            keyColumnCount * sizeof(TDynamicValueData) +
            columnLockCount * sizeof(TLockDescriptor) +
            listCount * sizeof(TEditListHeader*);

        // Allocate memory.
        auto* header = reinterpret_cast<TSortedDynamicRowHeader*>(pool->AllocateAligned(size));
        auto row = TSortedDynamicRow(header);

        // Generic fill.
        ::memset(header, 0, size);

        // Custom fill.
        {
            auto* lock = row.BeginLocks(keyColumnCount);
            for (int index = 0; index < columnLockCount; ++index, ++lock) {
                lock->PrepareTimestamp = NTableClient::NotPreparedTimestamp;
                lock->WriteRevisionList = nullptr;
            }
        }

        return row;
    }


    explicit operator bool() const
    {
        return Header_ != nullptr;
    }

    TSortedDynamicRowHeader* GetHeader() const
    {
        return Header_;
    }

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


    size_t GetDataWeight() const
    {
        return Header_->DataWeight;
    }

    size_t& GetDataWeight()
    {
        return Header_->DataWeight;
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
        YT_ASSERT(columnIndex >= keyColumnCount);
        return TValueList(GetLists(keyColumnCount, columnLockCount)[columnIndex - keyColumnCount + 1]);
    }

    void SetFixedValueList(int columnIndex, TValueList list, int keyColumnCount, int columnLockCount)
    {
        YT_ASSERT(columnIndex >= keyColumnCount);
        GetLists(keyColumnCount, columnLockCount)[columnIndex - keyColumnCount + 1] = list.Header_;
    }


    TRevisionList GetDeleteRevisionList(int keyColumnCount, int columnLockCount) const
    {
        return TRevisionList(GetLists(keyColumnCount, columnLockCount)[0]);
    }

    void SetDeleteRevisionList(TRevisionList list, int keyColumnCount, int columnLockCount)
    {
        GetLists(keyColumnCount, columnLockCount)[0] = list.Header_;
    }


    static TRevisionList GetWriteRevisionList(const TLockDescriptor& lock)
    {
        return TRevisionList(lock.WriteRevisionList);
    }

    static void SetWriteRevisionList(TLockDescriptor& lock, TRevisionList list)
    {
        lock.WriteRevisionList = list.Header_;
    }


    static TRevisionList GetExclusiveLockRevisionList(const TLockDescriptor& lock)
    {
        return TRevisionList(lock.ExclusiveLockRevisionList);
    }

    static void SetExclusiveLockRevisionList(TLockDescriptor& lock, TRevisionList list)
    {
        lock.ExclusiveLockRevisionList = list.Header_;
    }


    static TRevisionList GetReadLockRevisionList(const TLockDescriptor& lock)
    {
        return TRevisionList(lock.ReadLockRevisionList);
    }

    static void SetReadLockRevisionList(TLockDescriptor& lock, TRevisionList list)
    {
        lock.ReadLockRevisionList = list.Header_;
    }


    bool operator == (TSortedDynamicRow other) const
    {
        return Header_ == other.Header_;
    }

    bool operator != (TSortedDynamicRow other) const
    {
        return Header_ != other.Header_;
    }

private:
    TSortedDynamicRowHeader* Header_;


    TEditListHeader** GetLists(int keyColumnCount, int columnLockCount) const
    {
        return reinterpret_cast<TEditListHeader**>(const_cast<TLockDescriptor*>(BeginLocks(keyColumnCount)) + columnLockCount);
    }
};

static_assert(
    sizeof(TSortedDynamicRow) == sizeof(intptr_t),
    "TSortedDynamicRow size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

using TOrderedDynamicRow = NTableClient::TMutableUnversionedRow;
using TOrderedDynamicRowSegment = std::vector<std::atomic<NTableClient::TUnversionedRowHeader*>>;

////////////////////////////////////////////////////////////////////////////////

template <class TStore, class TStoreManager, class TRow>
struct TDynamicRowRef
{
    TDynamicRowRef()
        : Store(nullptr)
        , StoreManager(nullptr)
        , Row()
    { }

    TDynamicRowRef(const TDynamicRowRef& other) = default;
    TDynamicRowRef& operator=(const TDynamicRowRef& other) = default;

    TDynamicRowRef(
        TStore* store,
        TStoreManager* storeManager,
        TRow row)
        : Store(store)
        , StoreManager(storeManager)
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


    TStore* Store;
    TStoreManager* StoreManager;
    TRow Row;
};

struct TSortedDynamicRowRef
    : public TDynamicRowRef<TSortedDynamicStore, TSortedStoreManager, TSortedDynamicRow>
{
    using TBase = TDynamicRowRef<TSortedDynamicStore, TSortedStoreManager, TSortedDynamicRow>;

    TSortedDynamicRowRef() = default;
    TSortedDynamicRowRef(const TSortedDynamicRowRef& other) = default;
    TSortedDynamicRowRef(TSortedDynamicRowRef&& other) noexcept = default;
    TSortedDynamicRowRef& operator=(const TSortedDynamicRowRef& other) = default;

    TSortedDynamicRowRef(
        TSortedDynamicStore* store,
        TSortedStoreManager* storeManager,
        TSortedDynamicRow row)
        : TBase(store, storeManager, row)
    { }

    TLockMask LockMask;
};

using TOrderedDynamicRowRef = TDynamicRowRef<TOrderedDynamicStore, TOrderedStoreManager, TOrderedDynamicRow>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWritePhase,
    (Prelock)
    (Lock)
    (Commit)
);

using TTimestampToRevisionMap = THashMap<TTimestamp, ui32>;

struct TWriteContext
{
    EWritePhase Phase;

    TTransaction* Transaction = nullptr;
    TTimestamp CommitTimestamp = NullTimestamp;

    int RowCount = 0;
    size_t DataWeight = 0;

    bool Lockless = false;

    TRingQueue<TSortedDynamicRowRef>* PrelockedRows = nullptr;
    std::vector<TSortedDynamicRowRef>* LockedRows = nullptr;

    TSortedDynamicStorePtr BlockedStore;
    TSortedDynamicRow BlockedRow;
    TLockMask BlockedLockMask;
    TTimestamp BlockedTimestamp = NullTimestamp;

    TError Error;

    TTimestampToRevisionMap TimestampToRevision;

    std::optional<NTableClient::THunkChunksInfo> HunkChunksInfo;
};

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue GetUnversionedKeyValue(TSortedDynamicRow row, int index, EValueType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
