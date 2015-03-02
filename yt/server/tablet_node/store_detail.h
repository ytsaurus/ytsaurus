#pragma once

#include "public.h"
#include "store.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreBase
    : public IStore
{
public:
    TStoreBase(const TStoreId& id, TTablet* tablet);

    // IStore implementation.
    virtual TStoreId GetId() const override;
    virtual TTablet* GetTablet() const override;

    virtual EStoreState GetState() const override;
    virtual void SetState(EStoreState state) override;

    virtual TPartition* GetPartition() const override;
    virtual void SetPartition(TPartition* partition) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

protected:
    const TStoreId StoreId_;
    TTablet* const Tablet_;

    const TTabletPerformanceCountersPtr PerformanceCounters_;
    const TTabletId TabletId_;
    const NVersionedTableClient::TTableSchema Schema_;
    const NVersionedTableClient::TKeyColumns KeyColumns_;
    const int KeyColumnCount_;
    const int SchemaColumnCount_;
    const int ColumnLockCount_;
    const std::vector<Stroka> LockIndexToName_;
    const std::vector<int> ColumnIndexToLockIndex_;

    EStoreState State_;
    TPartition* Partition_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
