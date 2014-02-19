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
    TStoreBase(
        const TStoreId& id,
        TTablet* tablet);

    // IStore implementation.
    virtual TStoreId GetId() const override;

    virtual TTablet* GetTablet() const override;

    virtual EStoreState GetState() const override;
    virtual void SetState(EStoreState state) override;

    virtual TPartition* GetPartition() const override;
    virtual void SetPartition(TPartition* partition) override;

protected:
    TStoreId Id_;
    TTablet* Tablet_;

    EStoreState State_;
    TPartition* Partition_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
