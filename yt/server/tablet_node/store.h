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
    virtual TStoreId GetId() const = 0;
    
    virtual EStoreState GetState() const = 0;
    EStoreState GetPersistentState() const;
    virtual void SetState(EStoreState state) = 0;

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

} // namespace NTabletNode
} // namespace NYT
