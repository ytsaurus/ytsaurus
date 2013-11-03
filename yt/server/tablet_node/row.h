#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>

#include <ytlib/new_table_client/row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TRowGroupHeader
{
    TTransaction* Transaction;
    TRowGroupItemHeader* FirstItem;
    bool Prewritten;
    // A sequence of TRowValue instances (representing key values) follow.
};

////////////////////////////////////////////////////////////////////////////////

struct TRowGroupItemHeader
{
    TRowGroupItemHeader* NextItem;
    bool Orphaned;
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
        Header_->Orphaned = false;
        Header_->RowHeader.ValueCount = valueCount;
        Header_->RowHeader.Timestamp = timestamp;
        Header_->RowHeader.Deleted = deleted;
    }


    FORCED_INLINE explicit operator bool()
    {
        return Header_ != nullptr;
    }


    FORCED_INLINE bool GetOrphaned() const
    {
        return Header_->Orphaned;
    }

    FORCED_INLINE void SetOrphaned(bool value)
    {
        Header_->Orphaned = value;
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
        Header_->Prewritten = false;
    }


    FORCED_INLINE explicit operator bool()
    {
        return Header_ != nullptr;
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


    FORCED_INLINE bool GetPrewritten() const
    {
        return Header_->Prewritten;
    }

    FORCED_INLINE void SetPrewritten(bool value)
    {
        Header_->Prewritten = value;
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

} // namespace NTabletNode
} // namespace NYT
