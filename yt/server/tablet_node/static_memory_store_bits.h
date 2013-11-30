#pragma once

#include "public.h"

#include <ytlib/new_table_client/row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TStaticRowHeader
{
    NVersionedTableClient::TTimestamp LastCommitTimestamp;
    
    // Variable-size part:
    // * TUnversionedValue per each key column
    // * TTimestamp* for timestamps
    // * TVersionedValue* per each fixed non-key column
    // * ui16 for timestamp list size
    // * ui16 per each fixed non-key column for list size
    // * padding up to 8 bytes
};

////////////////////////////////////////////////////////////////////////////////

class TStaticRow
{
public:
    TStaticRow()
        : Header_(nullptr)
    { }

    explicit TStaticRow(TStaticRowHeader* header)
        : Header_(header)
    { }


    static size_t GetSize(int keyCount, int schemaColumnCount)
    {
        size_t size =
            sizeof (TStaticRowHeader) +
            sizeof (NVersionedTableClient::TUnversionedValue) * keyCount +
            sizeof (TTimestamp*) +
            sizeof (NVersionedTableClient::TVersionedValue*) * (schemaColumnCount - keyCount) +
            sizeof (ui16) +
            sizeof (ui16) * (schemaColumnCount - keyCount);
        return (size + 7) & ~7;
    }


    explicit operator bool()
    {
        return Header_ != nullptr;
    }

    
    TTimestamp GetLastCommittedTimestamp() const
    {
        return Header_->LastCommitTimestamp;
    }

    void SetLastCommittedTimestamp(TTimestamp timestamp)
    {
        Header_->LastCommitTimestamp = timestamp;
    }


    const NVersionedTableClient::TUnversionedValue& operator [](int id) const
    {
        return GetKeys()[id];
    }

    NVersionedTableClient::TUnversionedValue* GetKeys() const
    {
        return reinterpret_cast<NVersionedTableClient::TUnversionedValue*>(
            reinterpret_cast<char*>(Header_) +
            sizeof(TStaticRowHeader));
    }


    TTimestamp* GetTimestamps(int keyCount)
    {
        return *GetTimestampsPtr(keyCount);
    }

    void SetTimestamps(int keyCount, TTimestamp* timestamps)
    {
        *GetTimestampsPtr(keyCount) = timestamps;
    }


    int GetTimestampCount(int keyCount, int schemaColumnCount) const
    {
        return *GetTimestampCountPtr(keyCount, schemaColumnCount);
    }

    void SetTimestampCount(int keyCount, int schemaColumnCount, int count) const
    {
        *GetTimestampCountPtr(keyCount, schemaColumnCount) = count;
    }


    NVersionedTableClient::TVersionedValue* GetFixedValues(int keyCount, int index)
    {
        return *GetFixedValuesPtr(keyCount, index);
    }

    void SetFixedValues(int keyCount, int index, NVersionedTableClient::TVersionedValue* values)
    {
        *GetFixedValuesPtr(keyCount, index) = values;
    }


    int GetFixedValueCount(int keyCount, int schemaColumnCount, int index) const
    {
        return *GetFixedValueCountPtr(keyCount, schemaColumnCount, index);
    }

    void SetFixedValueCount(int keyCount, int schemaColumnCount, int index, int count)
    {
        *GetFixedValueCountPtr(keyCount, schemaColumnCount, index) = count;
    }

private:
    TStaticRowHeader* Header_;

    TTimestamp** GetTimestampsPtr(int keyCount) const
    {
        return reinterpret_cast<TTimestamp**>(
            reinterpret_cast<char*>(Header_) +
            sizeof(TStaticRowHeader) +
            sizeof(NVersionedTableClient::TUnversionedValue) * keyCount);
    }

    ui16* GetTimestampCountPtr(int keyCount, int schemaColumnCount) const
    {
        return reinterpret_cast<ui16*>(
            reinterpret_cast<char*>(Header_) +
            sizeof(TStaticRowHeader) +
            sizeof(NVersionedTableClient::TUnversionedValue) * keyCount +
            sizeof(TTimestamp*) +
            sizeof(NVersionedTableClient::TVersionedValue*) * (schemaColumnCount - keyCount));
    }

    NVersionedTableClient::TVersionedValue** GetFixedValuesPtr(int keyCount, int index) const
    {
        return reinterpret_cast<NVersionedTableClient::TVersionedValue**>(
            reinterpret_cast<char*>(Header_) +
            sizeof(TStaticRowHeader) +
            sizeof(NVersionedTableClient::TUnversionedValue) * keyCount +
            sizeof(TTimestamp*) +
            sizeof(NVersionedTableClient::TVersionedValue*) * index);
    }

    ui16* GetFixedValueCountPtr(int keyCount, int schemaColumnCount, int index) const
    {
        return reinterpret_cast<ui16*>(
            reinterpret_cast<char*>(Header_) +
            sizeof(TStaticRowHeader) +
            sizeof(NVersionedTableClient::TUnversionedValue) * keyCount +
            sizeof(TTimestamp*) +
            sizeof(NVersionedTableClient::TVersionedValue*) * (schemaColumnCount - keyCount) +
            sizeof(ui16) +
            sizeof(ui16) * index);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
