#pragma once

#include "public.h"

#include <ytlib/new_table_client/public.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStore
    : public TRefCounted
{
    virtual std::unique_ptr<IStoreScanner> CreateScanner() = 0;

    //! Returns a reader for the range from |lowerKey| (inclusive) to |upperKey| (exclusive).
    /*!
    *  If no matching row is found then |nullptr| might be returned.
    *
    *  The reader will be providing values filtered by |timestamp| and columns
    *  filtered by |columnFilter|.
    */
    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        NVersionedTableClient::TKey lowerKey,
        NVersionedTableClient::TKey upperKey,
        TTimestamp timestamp,
        const NApi::TColumnFilter& columnFilter) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IStoreScanner
{
    virtual ~IStoreScanner()
    { }

    //! Positions the scanner at a row with a given |key| filtering by a given |timestamp|.
    /*!
     *  If no row is found then |NullTimestamp| is returned.
     */
    virtual TTimestamp Find(NVersionedTableClient::TKey key, TTimestamp timestamp) = 0;

    //! Similar to #FindRow, but positions the scanner at the first row
    //! with key not less than |key|.
    virtual TTimestamp BeginScan(NVersionedTableClient::TKey key, TTimestamp timestamp) = 0;

    //! Advances to the next row.
    //! The return value is similar to that of #FindRow and #BeginScan.
    //! In particular, |NullTimestamp| is returned when no more matching rows are left.
    virtual TTimestamp Advance() = 0;

    //! Completes scanning, releases all resources held by the scanner.
    virtual void EndScan() = 0;


    //! Returns the array of keys.
    virtual const NVersionedTableClient::TUnversionedValue* GetKeys() const = 0;

    //! Returns the value for a fixed column with a given |index|.
    //! If no value is recorded then |nullptr| is returned.
    virtual const NVersionedTableClient::TVersionedValue* GetFixedValue(int index) const = 0;

    //! Fills |values| with up to |maxVersions| values recorded for a fixed column with a given |index|.
    //! Only values with timestamp not exceeding that passed during initialization are returned.
    //! This version scan can pass across tombstone boundaries.
    //! Values are listed in the order of decreasing timestamps.
    virtual void GetFixedValues(
        int index,
        int maxVersions,
        std::vector<NVersionedTableClient::TVersionedValue>* values) const = 0;

    //! Fills |timestamps| with all known row timestamps.
    //! Only timestamps not exceeding that passed during initialization are returned.
    //! This version scan can pass across tombstone boundaries (and will return tombstone timestamps).
    //! Timestamps are listed in decreasing order.
    virtual void GetTimestamps(std::vector<TTimestamp>* timestamps) const = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
