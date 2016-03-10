#pragma once

#include "public.h"
#include "sorted_dynamic_store_bits.h"
#include "store.h"

#include <yt/ytlib/table_client/schema.h>

#include <yt/core/actions/signal.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreBase
    : public virtual IStore
{
public:
    TStoreBase(const TStoreId& id, TTablet* tablet);
    ~TStoreBase();

    // IStore implementation.
    virtual TStoreId GetId() const override;
    virtual TTablet* GetTablet() const override;

    virtual EStoreState GetStoreState() const override;
    virtual void SetStoreState(EStoreState state) override;

    virtual i64 GetMemoryUsage() const override;
    virtual void SubscribeMemoryUsageUpdated(const TCallback<void(i64 delta)>& callback) override;
    virtual void UnsubscribeMemoryUsageUpdated(const TCallback<void(i64 delta)>& callback) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

protected:
    const TStoreId StoreId_;
    TTablet* const Tablet_;

    const TTabletPerformanceCountersPtr PerformanceCounters_;
    const TTabletId TabletId_;
    const NTableClient::TTableSchema Schema_;
    const NTableClient::TKeyColumns KeyColumns_;
    const int KeyColumnCount_;
    const int SchemaColumnCount_;
    const int ColumnLockCount_;
    const std::vector<Stroka> LockIndexToName_;
    const std::vector<int> ColumnIndexToLockIndex_;

    EStoreState StoreState_;

    NLogging::TLogger Logger;


    void SetMemoryUsage(i64 value);

    TOwningKey RowToKey(TUnversionedRow row);
    TOwningKey RowToKey(TSortedDynamicRow row);

private:
    i64 MemoryUsage_ = 0;
    TCallbackList<void(i64 delta)> MemoryUsageUpdated_;

};

////////////////////////////////////////////////////////////////////////////////

class TDynamicStoreBase
    : public virtual TStoreBase
    , public virtual IDynamicStore
{
public:
    TDynamicStoreBase(const TStoreId& id, TTablet* tablet);

    // IDynamicStore implementation.
    virtual EStoreFlushState GetFlushState() const override;
    virtual void SetFlushState(EStoreFlushState state) override;

protected:
    EStoreFlushState FlushState_ = EStoreFlushState::None;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkStoreBase
    : public virtual TStoreBase
    , public virtual IChunkStore
{
public:
    TChunkStoreBase(const TStoreId& id, TTablet* tablet);

    // IChunkStore implementation.
    virtual EStorePreloadState GetPreloadState() const override;
    virtual void SetPreloadState(EStorePreloadState state) override;

    virtual TFuture<void> GetPreloadFuture() const override;
    virtual void SetPreloadFuture(TFuture<void> future) override;

    virtual EStoreCompactionState GetCompactionState() const override;
    virtual void SetCompactionState(EStoreCompactionState state) override;

protected:
    EStorePreloadState PreloadState_ = EStorePreloadState::Disabled;
    TFuture<void> PreloadFuture_;
    EStoreCompactionState CompactionState_ = EStoreCompactionState::None;

};

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreBase
    : public virtual TStoreBase
    , public virtual ISortedStore
{
public:
    TSortedStoreBase(const TStoreId& id, TTablet* tablet);

    virtual TPartition* GetPartition() const override;
    virtual void SetPartition(TPartition* partition) override;

protected:
    TPartition* Partition_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
