#pragma once

#include "public.h"

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStore
    : public TRefCounted
{
    virtual std::unique_ptr<IStoreScanner> CreateScanner() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IStoreScanner
{
    virtual ~IStoreScanner()
    { }

    //! Positions the scanner at a row with a given |key| filtering by a given |timestamp|.
    /*!
     *  If no row is found then |NullTimestamp| is returned.
     *
     *  If the row is found is is known to be deleted then the deletion
     *  timestamp combined with |TombstoneTimestampMask| is returned.
     * 
     *  If the row is found and is known to exist then the earliest modification
     *  timestamp is returned. If the store has no row deletion marker for |key|
     *  (up to |timestamp|) then the latter is combined with |IncrementalTimestampMask|.
     */
    virtual TTimestamp FindRow(NVersionedTableClient::TKey key, TTimestamp timestamp) = 0;

    //! Returns the key component with a given index.
    virtual const NVersionedTableClient::TUnversionedValue& GetKey(int index) = 0;

    //! Returns the value for a fixed column with a given index.
    //! If no value is recorded then |nullptr| is returned.
    virtual const NVersionedTableClient::TVersionedValue* GetFixedValue(int index) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
