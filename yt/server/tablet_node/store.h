#pragma once

#include "public.h"

#include <core/misc/range.h>

#include <core/actions/signal.h>

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

    virtual i64 GetUncompressedDataSize() const = 0;
    virtual i64 GetRowCount() const = 0;

    virtual EStoreType GetType() const = 0;

    virtual EStoreState GetStoreState() const = 0;
    virtual void SetStoreState(EStoreState state) = 0;

    virtual EStoreRemovalState GetRemovalState() const = 0;
    virtual void SetRemovalState(EStoreRemovalState state) = 0;

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
    *  This call is typically synchronous and fast but may occasionally yield.
    *
    *  Thread affinity: any
    */
    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) = 0;

    //! Creates a reader for the set of |keys|.
    /*!
    *  If no matching row is found then |nullptr| might be returned.
    *
    *  The reader will be providing values filtered by |timestamp| and columns
    *  filtered by |columnFilter|.
    *
    *  This call is typically synchronous and fast but may occasionally yield.
    *
    *  Thread affinity: any
    */
    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) = 0;

    //! Checks that #transaction attempting to take locks indicated by #lockMask
    //! has no conflicts within the store. Throws on failure.
    /*!
     *  Thread affinity: any
     */
    virtual void CheckRowLocks(
        TUnversionedRow row,
        TTransaction* transaction,
        ui32 lockMask) = 0;

    //! Returns the number of bytes currently used by the store.
    virtual i64 GetMemoryUsage() const = 0;

    //! Raised whenever the store memory usage is changed.
    DECLARE_INTERFACE_SIGNAL(void(i64 delta), MemoryUsageUpdated);


    //! Serializes the synchronous part of the state.
    virtual void Save(TSaveContext& context) const = 0;
    //! Deserializes the synchronous part of the state.
    virtual void Load(TLoadContext& context) = 0;

    //! Serializes the asynchronous part of the state.
    virtual TCallback<void(TSaveContext&)> AsyncSave() = 0;
    //! Deserializes the asynchronous part of the state.
    virtual void AsyncLoad(TLoadContext& context) = 0;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) = 0;

};

DEFINE_REFCOUNTED_TYPE(IStore)

////////////////////////////////////////////////////////////////////////////////

struct TStoreIdFormatter
{
    void operator() (TStringBuilder* builder, const IStorePtr& store) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
