#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStore
    : public virtual TRefCounted
{
    virtual TStoreId GetId() const = 0;

    virtual TTablet* GetTablet() const = 0;

    virtual i64 GetDataSize() const = 0;

    virtual EStoreType GetType() const = 0;

    virtual EStoreState GetState() const = 0;
    EStoreState GetPersistentState() const;
    virtual void SetState(EStoreState state) = 0;

    TDynamicMemoryStorePtr AsDynamicMemory();
    TChunkStorePtr AsChunk();

    virtual TPartition* GetPartition() const = 0;
    virtual void SetPartition(TPartition* partition) = 0;

    //! Returns the minimum key in the store, inclusive.
    virtual TOwningKey GetMinKey() const = 0;

    //! Returns the maximum key in the store, inclusive.
    virtual TOwningKey GetMaxKey() const = 0;

    //! Returns the minimum timestamp of changes recorded in the store.
    virtual TTimestamp GetMinTimestamp() const = 0;

    //! Returns the maximum timestamp of changes recorded in the store.
    virtual TTimestamp GetMaxTimestamp() const = 0;

    //! Creates a reader for the range from |lowerKey| (inclusive) to |upperKey| (exclusive).
    /*!
    *  If no matching row is found then |nullptr| might be returned.
    *
    *  The reader will be providing values filtered by |timestamp| and columns
    *  filtered by |columnFilter|.
    *
    *  This call is typically synchornous and fast but may occasionally yield.
    */
    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) = 0;

    //! Creates a lookuper instance.
    /*!
     *  This call is typically synchornous and fast but may occasionally yield.
     */
    virtual NVersionedTableClient::IVersionedLookuperPtr CreateLookuper(
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) = 0;

    //! Returns the latest commit timestamp for a given #key.
    virtual TTimestamp GetLatestCommitTimestamp(
        TKey key,
        ui32 lockMask) = 0;


    virtual void Save(TSaveContext& context) const = 0;
    virtual void Load(TLoadContext& context) = 0;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) = 0;

};

DEFINE_REFCOUNTED_TYPE(IStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
