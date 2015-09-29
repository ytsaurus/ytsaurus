#pragma once

#include "public.h"
#include "store.h"
#include "dynamic_memory_store_bits.h"

#include <core/actions/signal.h>

#include <core/logging/log.h>

#include <ytlib/table_client/schema.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreBase
    : public IStore
{
public:
    TStoreBase(const TStoreId& id, TTablet* tablet);
    ~TStoreBase();

    // IStore implementation.
    virtual TStoreId GetId() const override;
    virtual TTablet* GetTablet() const override;

    virtual EStoreState GetStoreState() const override;
    virtual void SetStoreState(EStoreState state) override;

    virtual EStoreRemovalState GetRemovalState() const override;
    virtual void SetRemovalState(EStoreRemovalState state) override;

    virtual TPartition* GetPartition() const override;
    virtual void SetPartition(TPartition* partition) override;

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
    EStoreRemovalState RemovalState_;

    TPartition* Partition_ = nullptr;

    NLogging::TLogger Logger;


    void SetMemoryUsage(i64 value);

    TOwningKey RowToKey(TUnversionedRow row);
    TOwningKey RowToKey(TDynamicRow row);

private:
    i64 MemoryUsage_ = 0;
    TCallbackList<void(i64 delta)> MemoryUsageUpdated_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
