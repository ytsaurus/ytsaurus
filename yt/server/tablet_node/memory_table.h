#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/rcu_tree.h>

#include <ytlib/new_table_client/row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): extract
struct TRowGroupHeader
{
    TTransaction* Transaction;
    TRowGroupItemHeader* FirstItem;
    bool TransientLock;
    // A sequence of TRowValue instances (representing key values) follow.
};

////////////////////////////////////////////////////////////////////////////////

struct TRowGroupItemHeader
{
    TRowGroupItemHeader* NextItem;
    NVersionedTableClient::TRowHeader RowHeader;
    // A sequence of TRowValue instances (representing non-key values) follow.
};

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TRowGroupItemHeader*.
class TRowGroupItem
{
public:
    FORCED_INLINE TRowGroupItem()
        : Header_(nullptr)
    { }

    FORCED_INLINE explicit TRowGroupItem(TRowGroupItemHeader* header)
        : Header_(header)
    { }

    FORCED_INLINE TRowGroupItem(
        TChunkedMemoryPool* pool, 
        int valueCount,
        NVersionedTableClient::TTimestamp timestamp, 
        bool deleted)
        : Header_(reinterpret_cast<TRowGroupItemHeader*>(pool->Allocate(
            sizeof(TRowGroupItemHeader) +
            valueCount * sizeof(NVersionedTableClient::TRowValue))))
    {
        Header_->NextItem = nullptr;
        Header_->RowHeader.ValueCount = valueCount;
        Header_->RowHeader.Timestamp = timestamp;
        Header_->RowHeader.Deleted = deleted;
    }


    FORCED_INLINE explicit operator bool()
    {
        return Header_ != nullptr;
    }


    FORCED_INLINE NVersionedTableClient::TRow GetRow() const
    {
        return NVersionedTableClient::TRow(&Header_->RowHeader);
    }


    FORCED_INLINE TRowGroupItem GetNextItem() const
    {
        return TRowGroupItem(Header_->NextItem);
    }

    FORCED_INLINE void SetNextItem(TRowGroupItem item)
    {
        Header_->NextItem = item.Header_;
    }

private:
    friend class TRowGroup;

    TRowGroupItemHeader* Header_;

};

static_assert(sizeof (TRowGroupItem) == sizeof (intptr_t), "TRowGroupItem has to be exactly sizeof (intptr_t) bytes.");

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TRowGroupHeader*.
class TRowGroup
{
public:
    FORCED_INLINE TRowGroup()
        : Header_(nullptr)
    { }

    FORCED_INLINE explicit TRowGroup(TRowGroupHeader* header)
        : Header_(header)
    { }

    FORCED_INLINE TRowGroup(
        TChunkedMemoryPool* pool, 
        int keyColumnCount)
        : Header_(reinterpret_cast<TRowGroupHeader*>(pool->Allocate(
            sizeof(TRowGroupHeader) +
            keyColumnCount * sizeof(NVersionedTableClient::TRowValue))))
    {
        Header_->Transaction = nullptr;
        Header_->TransientLock = false;
    }


    FORCED_INLINE const NVersionedTableClient::TRowValue& operator[](int index) const
    {
        return *reinterpret_cast<NVersionedTableClient::TRowValue*>(
            reinterpret_cast<char*>(Header_) +
            sizeof(TRowGroupHeader) +
            index * sizeof(NVersionedTableClient::TRowValue));
    }

    FORCED_INLINE NVersionedTableClient::TRowValue& operator[](int index)
    {
        return *reinterpret_cast<NVersionedTableClient::TRowValue*>(
            reinterpret_cast<char*>(Header_) +
            sizeof(TRowGroupHeader) +
            index * sizeof(NVersionedTableClient::TRowValue));
    }


    FORCED_INLINE TTransaction* GetTransaction() const
    {
        return Header_->Transaction;
    }

    FORCED_INLINE void SetTransaction(TTransaction* transaction)
    {
        Header_->Transaction = transaction;
    }


    FORCED_INLINE bool GetTransientLock() const
    {
        return Header_->TransientLock;
    }

    FORCED_INLINE void SetTransientLock(bool value)
    {
        Header_->TransientLock = value;
    }


    FORCED_INLINE TRowGroupItem GetFirstItem() const
    {
        return TRowGroupItem(Header_->FirstItem);
    }

    FORCED_INLINE void SetFirstItem(TRowGroupItem item)
    {
        Header_->FirstItem = item.Header_;
    }

private:
    TRowGroupHeader* Header_;

};

static_assert(sizeof (TRowGroup) == sizeof (intptr_t), "TRowGroup has to be exactly sizeof (intptr_t) bytes.");

////////////////////////////////////////////////////////////////////////////////

class TMemoryTable
    : public TRefCounted
{
public:
    explicit TMemoryTable(
        TTabletManagerConfigPtr config,
        TTablet* tablet);

    void WriteRows(
        TTransaction* transaction,
        NVersionedTableClient::IReaderPtr reader,
        bool transient,
        std::vector<TRowGroup>* lockedGroups);

private:
    class TComparer;

    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    TChunkedMemoryPool TreePool_;
    TChunkedMemoryPool RowPool_;
    TChunkedMemoryPool StringPool_;

    std::unique_ptr<TComparer> Comparer_;
    std::unique_ptr<TRcuTree<TRowGroup, TComparer>> Tree_;
    TRcuTree<TRowGroup, TComparer>::TReader* TreeReader_;


    TRowGroup WriteRow(
        TTransaction* transaction,
        NVersionedTableClient::TRow row,
        bool transient);

    void InternValue(
        const NVersionedTableClient::TRowValue& src,
        NVersionedTableClient::TRowValue* dst);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
